package typedbclient

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync/atomic"

	pb "github.com/joycheney/go-typedb-v3-grpc/pb/proto"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// TransactionType transaction type
type TransactionType int

const (
	// Read read-only transaction
	Read TransactionType = iota
	// Write read-write transaction
	Write
	// Schema schema transaction (can modify schema)
	Schema
)

// QueryType query type
type QueryType int

const (
	// QueryTypeUnknown unknown query type
	QueryTypeUnknown QueryType = iota
	// QueryTypeRead read query
	QueryTypeRead
	// QueryTypeWrite write query
	QueryTypeWrite
	// QueryTypeSchema schema query
	QueryTypeSchema
)

// QueryResult query result
type QueryResult struct {
	// Query type
	QueryType QueryType

	// Result type flags
	IsDone           bool // Query complete, no stream results
	IsRowStream      bool // Row stream results
	IsDocumentStream bool // Document stream results

	// Row stream results
	ColumnNames []string        // Column names
	rows        [][]interface{} // Row data (private for safety, use TypedRows)

	// Strongly-typed row results
	TypedRows   []*TypedRow     // Type-safe row data

	// Document stream results
	documents       []map[string]interface{} // Document list (private for safety, use TypedDocuments)
	TypedDocuments  []*TypedDocument         // Type-safe document data
}

// GetColumnIndex returns the index of a column by name
func (qr *QueryResult) GetColumnIndex(columnName string) (int, error) {
	for i, name := range qr.ColumnNames {
		if name == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column '%s' not found in result columns: %v", columnName, qr.ColumnNames)
}

// Get returns value from the first row for a column (for convenience)
func (qr *QueryResult) Get(columnName string) (interface{}, error) {
	if !qr.IsRowStream {
		return nil, fmt.Errorf("Get() can only be used with row stream results")
	}
	if len(qr.rows) == 0 {
		return nil, fmt.Errorf("no rows in result")
	}
	return qr.GetFromRow(0, columnName)
}

// GetFromRow returns value from a specific row for a column
func (qr *QueryResult) GetFromRow(rowIndex int, columnName string) (interface{}, error) {
	if !qr.IsRowStream {
		return nil, fmt.Errorf("GetFromRow() can only be used with row stream results")
	}
	if rowIndex < 0 || rowIndex >= len(qr.rows) {
		return nil, fmt.Errorf("row index %d out of range (0-%d)", rowIndex, len(qr.rows)-1)
	}

	columnIndex, err := qr.GetColumnIndex(columnName)
	if err != nil {
		return nil, err
	}

	row := qr.rows[rowIndex]
	if columnIndex >= len(row) {
		return nil, fmt.Errorf("column index %d out of range for row with %d columns", columnIndex, len(row))
	}

	return row[columnIndex], nil
}

// GetTypedRows returns the typed rows with automatic conversion if needed
// This method detects incorrect usage and auto-converts when possible
func (qr *QueryResult) GetTypedRows() []*TypedRow {
	// Case 1: User called correctly - RowStream and accessing rows
	if qr.IsRowStream {
		return qr.TypedRows
	}

	// Case 2: User called incorrectly - DocumentStream but accessing rows
	// Detect the error and try to auto-convert
	if qr.IsDocumentStream && len(qr.TypedDocuments) > 0 {
		// Lazy conversion: only convert if not already cached
		if len(qr.TypedRows) == 0 {
			if rows, err := qr.convertDocumentsToRows(); err == nil {
				// Cache the converted result
				qr.TypedRows = rows
			}
			// If conversion fails (nested), TypedRows stays empty
		}
		return qr.TypedRows
	}

	return nil
}

// GetTypedDocuments returns the typed documents with automatic conversion if needed
// This method detects incorrect usage and auto-converts when possible
func (qr *QueryResult) GetTypedDocuments() []*TypedDocument {
	// Case 1: User called correctly - DocumentStream and accessing documents
	if qr.IsDocumentStream {
		return qr.TypedDocuments
	}

	// Case 2: User called incorrectly - RowStream but accessing documents
	// Detect the error and auto-convert (always succeeds for rows)
	if qr.IsRowStream && len(qr.TypedRows) > 0 {
		// Lazy conversion: only convert if not already cached
		if len(qr.TypedDocuments) == 0 {
			qr.TypedDocuments = qr.convertRowsToDocuments()
		}
		return qr.TypedDocuments
	}

	return nil
}

// GetRowCount returns the number of rows in the result (with automatic conversion)
// If result is DocumentStream but convertible to rows, returns converted count
func (qr *QueryResult) GetRowCount() int {
	// Primary path: direct row access
	if qr.IsRowStream {
		return len(qr.TypedRows)
	}

	// Fallback: try converting documents to rows
	if qr.IsDocumentStream && len(qr.TypedDocuments) > 0 {
		// Attempt conversion (only succeeds for flat documents)
		if rows, err := qr.convertDocumentsToRows(); err == nil {
			// Conversion succeeded - cache the result and return count
			qr.TypedRows = rows
			qr.ColumnNames = rows[0].GetColumnNames() // Already set by conversion
			return len(rows)
		}
	}

	return 0
}

// GetDocumentCount returns the number of documents in the result (with automatic conversion)
// If result is RowStream, automatically converts and returns count
func (qr *QueryResult) GetDocumentCount() int {
	// Primary path: direct document access
	if qr.IsDocumentStream {
		return len(qr.TypedDocuments)
	}

	// Fallback: auto-convert rows to documents (always succeeds)
	if qr.IsRowStream && len(qr.TypedRows) > 0 {
		// Lazy conversion: only convert if TypedDocuments is empty
		if len(qr.TypedDocuments) == 0 {
			qr.TypedDocuments = qr.convertRowsToDocuments()
		}
		return len(qr.TypedDocuments)
	}

	return 0
}

// GetDocument returns a document at the specified index (safe copy)
func (qr *QueryResult) GetDocument(index int) (map[string]interface{}, error) {
	if !qr.IsDocumentStream {
		return nil, fmt.Errorf("GetDocument() can only be used with document stream results")
	}
	if index < 0 || index >= len(qr.documents) {
		return nil, fmt.Errorf("document index %d out of range (0-%d)", index, len(qr.documents)-1)
	}

	// Return a copy to maintain immutability
	doc := make(map[string]interface{})
	for k, v := range qr.documents[index] {
		doc[k] = v
	}
	return doc, nil
}

// GetAllDocuments returns all documents (safe copies)
func (qr *QueryResult) GetAllDocuments() []map[string]interface{} {
	if !qr.IsDocumentStream || len(qr.documents) == 0 {
		return nil
	}

	// Return copies to maintain immutability
	docs := make([]map[string]interface{}, len(qr.documents))
	for i, d := range qr.documents {
		doc := make(map[string]interface{})
		for k, v := range d {
			doc[k] = v
		}
		docs[i] = doc
	}
	return docs
}

// convertMapToTypedDocument converts a map[string]interface{} to *TypedDocument
// This is used during data ingestion to create TypedDocuments from protobuf data
func convertMapToTypedDocument(data map[string]interface{}) *TypedDocument {
	fields := make(map[string]*TypedValue)

	for key, val := range data {
		fields[key] = convertInterfaceToTypedValue(val)
	}

	return &TypedDocument{fields: fields}
}

// convertInterfaceToTypedValue converts an interface{} value to *TypedValue
// This handles the conversion from generic map data to strongly-typed values
func convertInterfaceToTypedValue(val interface{}) *TypedValue {
	if val == nil {
		return &TypedValue{valueType: TypeNull, isNull: true}
	}

	switch v := val.(type) {
	case string:
		return &TypedValue{valueType: TypeString, stringVal: v}
	case int64:
		return &TypedValue{valueType: TypeInt64, int64Val: v}
	case float64:
		return &TypedValue{valueType: TypeFloat64, float64Val: v}
	case bool:
		return &TypedValue{valueType: TypeBool, boolVal: v}
	case map[string]interface{}:
		// Nested map - recursively convert
		// This might be a concept or nested structure
		return convertMapToTypedValue(v)
	case []interface{}:
		// List - convert each element
		list := make([]TypedValue, len(v))
		for i, item := range v {
			list[i] = *convertInterfaceToTypedValue(item)
		}
		return &TypedValue{valueType: TypeValueList, listVal: list}
	default:
		// Unknown type - store as raw
		return &TypedValue{valueType: TypeUnknown, rawValue: val}
	}
}

// convertMapToTypedValue converts a map to a TypedValue (for nested structures)
// This is used when a field contains a nested map (like a concept)
func convertMapToTypedValue(data map[string]interface{}) *TypedValue {
	// Check if it looks like a concept
	if typeStr, ok := data["type"].(string); ok {
		concept := &Concept{Type: typeStr}

		if iid, ok := data["iid"].(string); ok {
			concept.IID = iid
		}
		if label, ok := data["label"].(string); ok {
			concept.Label = label
		}
		if value, ok := data["value"]; ok {
			concept.Value = convertInterfaceToTypedValue(value)
		}

		// Check for links (relations)
		if links, ok := data["links"].(map[string]interface{}); ok {
			concept.Links = make(map[string][]*Concept)
			for role, players := range links {
				if playerList, ok := players.([]interface{}); ok {
					concepts := make([]*Concept, 0, len(playerList))
					for _, player := range playerList {
						if playerMap, ok := player.(map[string]interface{}); ok {
							if playerVal := convertMapToTypedValue(playerMap); playerVal.conceptVal != nil {
								concepts = append(concepts, playerVal.conceptVal)
							}
						}
					}
					concept.Links[role] = concepts
				}
			}
		}

		return &TypedValue{valueType: TypeConcept, conceptVal: concept}
	}

	// Not a concept - treat as unknown nested structure
	return &TypedValue{valueType: TypeUnknown, rawValue: data}
}

// convertRowsToDocuments converts row stream data to document format (always succeeds)
// This enables seamless conversion when user calls document methods on row results
func (qr *QueryResult) convertRowsToDocuments() []*TypedDocument {
	if len(qr.TypedRows) == 0 {
		return []*TypedDocument{}
	}

	docs := make([]*TypedDocument, len(qr.TypedRows))
	for i, row := range qr.TypedRows {
		// Create document fields from row columns
		fields := make(map[string]*TypedValue)

		// Get all column names from the row
		columnNames := row.GetColumnNames()
		for _, colName := range columnNames {
			if val, err := row.GetValue(colName); err == nil {
				fields[colName] = val
			}
		}

		docs[i] = &TypedDocument{fields: fields}
	}

	return docs
}

// convertDocumentsToRows converts document stream data to row format (conditional success)
// Only succeeds if all documents are flat (no nested structures)
func (qr *QueryResult) convertDocumentsToRows() ([]*TypedRow, error) {
	if len(qr.TypedDocuments) == 0 {
		return []*TypedRow{}, nil
	}

	// Step 1: Extract column names from the first document
	firstDoc := qr.TypedDocuments[0]
	columnNames := firstDoc.GetFieldNames() // Already sorted

	// Step 2: Validate all documents are flat and have consistent fields
	for i, doc := range qr.TypedDocuments {
		if err := validateFlatDocument(doc, columnNames); err != nil {
			return nil, fmt.Errorf("document[%d] cannot be converted to row: %w", i, err)
		}
	}

	// Step 3: Convert documents to rows
	rows := make([]*TypedRow, len(qr.TypedDocuments))
	for i, doc := range qr.TypedDocuments {
		columns := make(map[string]*TypedValue)
		for _, colName := range columnNames {
			if val, err := doc.GetValue(colName); err == nil {
				columns[colName] = val
			}
		}
		rows[i] = &TypedRow{columns: columns}
	}

	// Step 4: Update QueryResult metadata
	qr.ColumnNames = columnNames

	return rows, nil
}

// validateFlatDocument checks if a document can be flattened to a row
// Returns error if document has nested structures or field mismatch
func validateFlatDocument(doc *TypedDocument, expectedFields []string) error {
	actualFields := doc.GetFieldNames() // Already sorted

	// Check field count consistency
	if len(actualFields) != len(expectedFields) {
		return fmt.Errorf("field count mismatch: expected %d fields %v, got %d fields %v",
			len(expectedFields), expectedFields, len(actualFields), actualFields)
	}

	// Check field names consistency
	for i, expected := range expectedFields {
		if actualFields[i] != expected {
			return fmt.Errorf("field name mismatch at position %d: expected %q, got %q",
				i, expected, actualFields[i])
		}
	}

	// Check for nested structures
	for _, fieldName := range actualFields {
		val, err := doc.GetValue(fieldName)
		if err != nil {
			continue // Skip missing fields
		}

		if val.IsNested() {
			// Provide helpful error message based on nested type
			if val.IsList() {
				return fmt.Errorf("field %q contains a list (nested structure). Use GetDocuments() or modify your fetch query to return flat data", fieldName)
			}
			if val.Type() == TypeConcept {
				concept, _ := val.AsConcept()
				if concept != nil && len(concept.Links) > 0 {
					return fmt.Errorf("field %q contains a concept with links/relations (nested structure). Use GetDocuments() or modify your fetch query to return flat data", fieldName)
				}
			}
			return fmt.Errorf("field %q contains nested structure. Use GetDocuments() or modify your fetch query to return flat data", fieldName)
		}
	}

	return nil
}

// GetRowsRobust returns row data with automatic conversion from documents if needed
// This method provides seamless robustness - users don't need to know the exact result type
func (qr *QueryResult) GetRowsRobust() ([]*TypedRow, error) {
	// Case 1: Already have row data, return directly
	if qr.IsRowStream && len(qr.TypedRows) > 0 {
		return qr.TypedRows, nil
	}

	// Case 2: Have document data, attempt conversion
	if qr.IsDocumentStream && len(qr.TypedDocuments) > 0 {
		rows, err := qr.convertDocumentsToRows()
		if err != nil {
			// Conversion failed - provide helpful error message
			return nil, fmt.Errorf("cannot convert documents to rows: %w", err)
		}
		// Conversion succeeded - return converted rows seamlessly
		return rows, nil
	}

	// Case 3: Operation completed with no data
	if qr.IsDone {
		return nil, fmt.Errorf("query completed with no result data (IsDone=true)")
	}

	// Case 4: Unexpected state
	return nil, fmt.Errorf("no row or document data available")
}

// GetDocumentsRobust returns document data with automatic conversion from rows if needed
// This method provides seamless robustness - users don't need to know the exact result type
func (qr *QueryResult) GetDocumentsRobust() ([]*TypedDocument, error) {
	// Case 1: Already have document data, return directly
	if qr.IsDocumentStream && len(qr.TypedDocuments) > 0 {
		return qr.TypedDocuments, nil
	}

	// Case 2: Have row data, convert automatically (always succeeds)
	if qr.IsRowStream && len(qr.TypedRows) > 0 {
		docs := qr.convertRowsToDocuments()
		// Row→Document conversion always succeeds, return seamlessly
		return docs, nil
	}

	// Case 3: Operation completed with no data
	if qr.IsDone {
		return nil, fmt.Errorf("query completed with no result data (IsDone=true)")
	}

	// Case 4: Unexpected state
	return nil, fmt.Errorf("no row or document data available")
}

// CountRobust returns the count of results regardless of result type
// This method provides seamless counting across row and document streams
func (qr *QueryResult) CountRobust() int {
	if qr.IsRowStream {
		return len(qr.TypedRows)
	}
	if qr.IsDocumentStream {
		return len(qr.TypedDocuments)
	}
	return 0
}

// extractErrorDetails extracts detailed error information from gRPC status
func extractErrorDetails(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return err.Error()
	}

	// Start with the basic error message
	result := fmt.Sprintf("Error: %s", st.Message())

	// Try to extract error details
	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *errdetails.ErrorInfo:
			// Add error code and domain if available
			if d.Reason != "" {
				result = fmt.Sprintf("%s\nError Code: %s", result, d.Reason)
			}
			if d.Domain != "" {
				result = fmt.Sprintf("%s\nDomain: %s", result, d.Domain)
			}
			// Add metadata if available
			if len(d.Metadata) > 0 {
				result = fmt.Sprintf("%s\nMetadata:", result)
				for k, v := range d.Metadata {
					result = fmt.Sprintf("%s\n  %s: %s", result, k, v)
				}
			}
		case *errdetails.DebugInfo:
			// Add stack trace if available
			if len(d.StackEntries) > 0 {
				result = fmt.Sprintf("%s\nStack Trace:", result)
				for _, entry := range d.StackEntries {
					result = fmt.Sprintf("%s\n  %s", result, entry)
				}
			}
			if d.Detail != "" {
				result = fmt.Sprintf("%s\nDebug Detail: %s", result, d.Detail)
			}
		case *errdetails.BadRequest:
			// Add field violations if any
			if len(d.FieldViolations) > 0 {
				result = fmt.Sprintf("%s\nField Violations:", result)
				for _, violation := range d.FieldViolations {
					result = fmt.Sprintf("%s\n  %s: %s", result, violation.Field, violation.Description)
				}
			}
		}
	}

	return result
}

// StreamOperation defines operation types for the worker
type StreamOperation int

const (
	OpOpen StreamOperation = iota // Open transaction (must be first operation in bundle)
	OpExecute
	OpCommit
	OpRollback
	OpClose
)

// StreamRequest represents a request to the worker goroutine
type StreamRequest struct {
	Operation StreamOperation
	RequestID []byte
	Query     string // Only used for Execute operations
	Response  chan StreamResponse
}

// StreamResponse represents the response from worker goroutine
type StreamResponse struct {
	Result *QueryResult
	Error  error
}

// BundleOperation represents a single operation in a bundle
type BundleOperation struct {
	Type  StreamOperation
	Query string // Only for Execute operations
}

// BundleRequest represents a complete bundle of operations to execute atomically
type BundleRequest struct {
	Operations []BundleOperation
	Response   chan BundleResponse
}

// BundleResponse represents the response from bundle execution
type BundleResponse struct {
	Results []*QueryResult // Results for each Execute operation
	Error   error
}

// Transaction transaction interface (producer/consumer pattern for lock-free design)
type Transaction struct {
	client   *Client
	database string
	txType   TransactionType

	// gRPC stream (only accessed by worker goroutine)
	stream grpc.BidiStreamingClient[pb.Transaction_Client, pb.Transaction_Server]
	closed atomic.Bool
	opened atomic.Bool // Track if transaction has been opened

	// Producer/consumer channels for lock-free bundle communication
	bundleChan chan BundleRequest
	workerDone chan struct{}
	workerErr  atomic.Value // stores error

	// Request ID generator (lock-free)
	requestID atomic.Uint64
}

// BeginTransaction begin transaction (delayed initialization - stream created in worker)
func (db *Database) beginTransaction(ctx context.Context, txType TransactionType) (*Transaction, error) {
	tx := &Transaction{
		client:     db.client,
		database:   db.name,
		txType:     txType,
		bundleChan: make(chan BundleRequest, 10), // Buffered channel for bundle operations
		workerDone: make(chan struct{}),
	}

	// Start worker goroutine (no stream yet - will be created on first bundle)
	// This ensures all stream operations happen only in the worker goroutine
	go tx.worker()

	return tx, nil
}

// worker handles all gRPC stream operations in a single goroutine (lock-free design)
// Processes complete bundles atomically to ensure no interleaving
func (tx *Transaction) worker() {
	defer close(tx.workerDone)

	for {
		select {
		case bundle := <-tx.bundleChan:
			if bundle.Response == nil {
				// Channel closed, exit worker
				return
			}

			// Process entire bundle atomically
			response := tx.processBundle(bundle.Operations)

			// Send response back to caller
			bundle.Response <- response

			// If bundle ended with Close, exit worker
			if len(bundle.Operations) > 0 &&
			   bundle.Operations[len(bundle.Operations)-1].Type == OpClose {
				return
			}

		case <-tx.workerDone:
			// External shutdown signal
			return
		}
	}
}

// processBundle executes a complete bundle of operations atomically
func (tx *Transaction) processBundle(operations []BundleOperation) BundleResponse {
	var results []*QueryResult
	var hasExecutedCommit bool

	// Execute each operation in sequence
	for _, op := range operations {
		switch op.Type {
		case OpOpen:
			// Handle transaction open (creates stream and sends open request)
			if err := tx.handleOpen(); err != nil {
				return BundleResponse{Error: err}
			}
			tx.opened.Store(true)

		case OpExecute:
			reqID := tx.generateRequestID()
			resp := tx.handleExecute(reqID, op.Query)
			if resp.Error != nil {
				// On error, automatically rollback if we haven't committed yet
				// and this isn't a read-only transaction
				if !hasExecutedCommit && tx.txType != Read {
					rollbackReqID := tx.generateRequestID()
					tx.handleRollback(rollbackReqID)
				}
				// Always close the transaction on error
				tx.handleClose()
				return BundleResponse{Error: resp.Error}
			}
			results = append(results, resp.Result)

		case OpCommit:
			reqID := tx.generateRequestID()
			resp := tx.handleCommit(reqID)
			if resp.Error != nil {
				// If commit fails, try to rollback and close
				rollbackReqID := tx.generateRequestID()
				tx.handleRollback(rollbackReqID)
				tx.handleClose()
				return BundleResponse{Error: resp.Error}
			}
			hasExecutedCommit = true

		case OpRollback:
			reqID := tx.generateRequestID()
			resp := tx.handleRollback(reqID)
			if resp.Error != nil {
				// If rollback fails, still try to close
				tx.handleClose()
				return BundleResponse{Error: resp.Error}
			}
			// autoCompleteBundle has already ensured OpClose follows OpRollback
			// No need to check or add close here

		case OpClose:
			resp := tx.handleClose()
			if resp.Error != nil {
				return BundleResponse{Error: resp.Error}
			}
		}
	}

	return BundleResponse{Results: results, Error: nil}
}

// handleOpen creates stream and sends open request (must be called from worker goroutine only)
func (tx *Transaction) handleOpen() error {
	// Try to create transaction stream with retry mechanism
	var stream grpc.BidiStreamingClient[pb.Transaction_Client, pb.Transaction_Server]
	var err error

	maxRetries := 2
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get gRPC client atomically
		connRef := tx.client.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		grpcClient := wrapper.grpcClient

		// Create transaction stream with authentication
		authCtx := tx.client.withAuth(context.Background())
		stream, err = grpcClient.Transaction(authCtx)
		if err == nil {
			break // Success
		}

		// Handle authentication error
		if isAuthError(err) {
			if authErr := tx.client.authenticate(); authErr != nil {
				return fmt.Errorf("reauthentication failed: %w", authErr)
			}
			continue
		}

		// Handle connection error
		if isConnectionError(err) {
			if reconnErr := tx.client.tryReconnect(); reconnErr != nil {
				return fmt.Errorf("reconnection failed: %w", reconnErr)
			}
			continue
		}

		return fmt.Errorf("failed to create transaction stream: %w", err)
	}

	if err != nil {
		return fmt.Errorf("failed to create transaction stream after %d attempts: %w", maxRetries, err)
	}

	// Store the stream
	tx.stream = stream

	// Generate request ID
	reqID := tx.generateRequestID()

	// Send transaction open request
	openReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId:    reqID,
				Metadata: make(map[string]string),
				Req: &pb.Transaction_Req_OpenReq{
					OpenReq: &pb.Transaction_Open_Req{
						Database:             tx.database,
						Type:                 convertTxTypeToPB(tx.txType),
						Options:              &pb.Options_Transaction{},
						NetworkLatencyMillis: 0,
					},
				},
			},
		},
	}

	if err := stream.Send(&openReq); err != nil {
		return fmt.Errorf("failed to send open request: %w", err)
	}

	// Wait for open response
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive open response: %w", err)
	}

	// Validate response
	if resp.GetRes() == nil || resp.GetRes().GetOpenRes() == nil {
		return fmt.Errorf("invalid open response")
	}

	return nil
}

// handleExecute processes query execution in worker goroutine (lock-free)
func (tx *Transaction) handleExecute(requestID []byte, query string) StreamResponse {

	// Build query request with required metadata field
	queryReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId:    requestID,
				Metadata: make(map[string]string),
				Req: &pb.Transaction_Req_QueryReq{
					QueryReq: &pb.Query_Req{
						Query: query,
						Options: &pb.Options_Query{
							IncludeInstanceTypes: nil,
							PrefetchSize:        &[]uint64{32}[0], // TypeDB DEFAULT_PREFETCH_SIZE, must be >= 1
						},
					},
				},
			},
		},
	}

	if err := tx.stream.Send(&queryReq); err != nil {
		return StreamResponse{Error: fmt.Errorf("failed to send query request: %w", err)}
	}

	// Receive initial response
	resp, err := tx.stream.Recv()
	if err != nil {
		// Extract detailed error information
		detailedError := extractErrorDetails(err)
		return StreamResponse{Error: fmt.Errorf("failed to receive query response.\nQuery:\n%s\n%s", query, detailedError)}
	}



	result := &QueryResult{}

	// 详细检查错误响应 - 添加深度错误解析
	if res := resp.GetRes(); res != nil {
		// 检查是否有错误响应
		if queryInitRes := res.GetQueryInitialRes(); queryInitRes != nil {
			if errorRes := queryInitRes.GetError(); errorRes != nil {
				return StreamResponse{Error: fmt.Errorf("TypeDB Query Error - Code: %s, Domain: %s, StackTrace: %v",
					errorRes.GetErrorCode(), errorRes.GetDomain(), errorRes.GetStackTrace())}
			}
		}
	}

	// Check if it's a direct response (for schema/write queries)
	if res := resp.GetRes(); res != nil {
		// Check for query initial response
		if queryInitRes := res.GetQueryInitialRes(); queryInitRes != nil {
			// Check if response is OK
			if okRes := queryInitRes.GetOk(); okRes != nil {
				// Check if query is done (no stream)
				if done := okRes.GetDone(); done != nil {
					result.IsDone = true
					// GetQueryType() returns Query_Type directly (not a pointer)
					result.QueryType = convertQueryType(done.GetQueryType())
					return StreamResponse{Result: result}
				}

				// Check if it's a row stream response
				if rowStream := okRes.GetConceptRowStream(); rowStream != nil {
					result.IsRowStream = true
					// ConceptRowStream has ColumnVariableNames field directly
					result.ColumnNames = rowStream.GetColumnVariableNames()
					// Set query type from stream
					result.QueryType = convertQueryType(rowStream.GetQueryType())

					// Continue processing stream parts
					for {
						resp, err := tx.stream.Recv()
						if err != nil {
							if err == io.EOF {
								break
							}
							detailedError := extractErrorDetails(err)
							return StreamResponse{Error: fmt.Errorf("failed to receive stream part: %s", detailedError)}
						}

						if tx.processQueryResponse(resp, result) {
							break
						}
					}
				}

				// Check if it's a document stream response
				if docStream := okRes.GetConceptDocumentStream(); docStream != nil {
					result.IsDocumentStream = true
					// Set query type from stream
					result.QueryType = convertQueryType(docStream.GetQueryType())

					// Continue processing stream parts
					loopCount := 0
					for {
						loopCount++
						resp, err := tx.stream.Recv()
						if err != nil {
							if err == io.EOF {
								break
							}
							detailedError := extractErrorDetails(err)
							return StreamResponse{Error: fmt.Errorf("failed to receive stream part: %s", detailedError)}
						}

						shouldBreak := tx.processQueryResponse(resp, result)
						if shouldBreak {
							break
						}
					}
				}
			}

			// Check if it's an error response
			if errRes := queryInitRes.GetError(); errRes != nil {
				// Build comprehensive error message with all available details
				errMsg := fmt.Sprintf("query error: %s", errRes.GetErrorCode())
				if errRes.GetDomain() != "" {
					errMsg += fmt.Sprintf(" (domain: %s)", errRes.GetDomain())
				}
				// Include the failing query for debugging
				errMsg += fmt.Sprintf("\n\nFailing query:\n%s", query)
				// Include stack trace for debugging if available
				if stackTrace := errRes.GetStackTrace(); len(stackTrace) > 0 {
					errMsg += "\nStack trace:\n"
					for i, line := range stackTrace {
						if i < 5 { // Limit to first 5 lines to avoid overwhelming output
							errMsg += fmt.Sprintf("  %s\n", line)
						}
					}
					if len(stackTrace) > 5 {
						errMsg += fmt.Sprintf("  ... and %d more lines\n", len(stackTrace)-5)
					}
				}
				return StreamResponse{Error: fmt.Errorf("%s", errMsg)}
			}
		}
	}

	// Check if it's a ResPart response (for streaming queries)
	if resPart := resp.GetResPart(); resPart != nil {

		// CRITICAL: Check for stream error in initial ResPart before entering loop
		if streamRes := resPart.GetStreamRes(); streamRes != nil {
			if errRes := streamRes.GetError(); errRes != nil {
				// Stream error detected in initial response - return immediately
				errMsg := fmt.Sprintf("TypeDB stream error: %s", errRes.GetErrorCode())
				if errRes.GetDomain() != "" {
					errMsg += fmt.Sprintf(" (domain: %s)", errRes.GetDomain())
				}
				if stackTrace := errRes.GetStackTrace(); len(stackTrace) > 0 {
					errMsg += "\nStack trace:\n"
					for i, line := range stackTrace {
						if i < 3 {
							errMsg += fmt.Sprintf("  %s\n", line)
						}
					}
				}
				return StreamResponse{Error: fmt.Errorf("%s", errMsg)}
			}
		}

		// Process first part
		shouldStop := tx.processQueryResponse(resp, result)
		if shouldStop {
			return StreamResponse{Result: result}
		}

		// Continue receiving stream responses
		loopCount := 0
		for {
			loopCount++
			resp, err := tx.stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return StreamResponse{Error: fmt.Errorf("failed to receive stream part: %w", err)}
			}

			// CRITICAL: Check for stream error before processing (matching Rust driver behavior)
			if resPart := resp.GetResPart(); resPart != nil {
				if streamRes := resPart.GetStreamRes(); streamRes != nil {
					if errRes := streamRes.GetError(); errRes != nil {
						// Stream error detected - return immediately with detailed error message
						errMsg := fmt.Sprintf("TypeDB stream error: %s", errRes.GetErrorCode())
						if errRes.GetDomain() != "" {
							errMsg += fmt.Sprintf(" (domain: %s)", errRes.GetDomain())
						}
						if stackTrace := errRes.GetStackTrace(); len(stackTrace) > 0 {
							errMsg += "\nStack trace:\n"
							for i, line := range stackTrace {
								if i < 3 {
									errMsg += fmt.Sprintf("  %s\n", line)
								}
							}
						}
						return StreamResponse{Error: fmt.Errorf("%s", errMsg)}
					}
				}
			}

			shouldBreak := tx.processQueryResponse(resp, result)
			if shouldBreak {
				break // Query complete
			}
		}
	}

	// Pre-populate both data formats for user convenience
	// If RowStream query returns rows, also fill documents for compatibility
	if result.IsRowStream && len(result.TypedRows) > 0 && len(result.TypedDocuments) == 0 {
		result.TypedDocuments = result.convertRowsToDocuments()
	}

	// If DocumentStream query returns documents, try to fill rows (only works for flat documents)
	if result.IsDocumentStream && len(result.TypedDocuments) > 0 && len(result.TypedRows) == 0 {
		if rows, err := result.convertDocumentsToRows(); err == nil {
			result.TypedRows = rows
		}
		// Silently fail for nested documents (conversion not possible)
	}

	return StreamResponse{Result: result}
}

// handleCommit processes transaction commit in worker goroutine (lock-free)
func (tx *Transaction) handleCommit(requestID []byte) StreamResponse {
	commitReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId:    requestID,
				Metadata: make(map[string]string),
				Req: &pb.Transaction_Req_CommitReq{
					CommitReq: &pb.Transaction_Commit_Req{},
				},
			},
		},
	}

	if err := tx.stream.Send(&commitReq); err != nil {
		return StreamResponse{Error: fmt.Errorf("failed to send commit request: %w", err)}
	}

	// Wait for commit response
	resp, err := tx.stream.Recv()
	if err != nil {
		return StreamResponse{Error: fmt.Errorf("failed to receive commit response: %w", err)}
	}

	if resp.GetRes() == nil || resp.GetRes().GetCommitRes() == nil {
		return StreamResponse{Error: fmt.Errorf("invalid commit response")}
	}

	tx.closed.Store(true)
	return StreamResponse{} // Success, no result needed
}

// handleRollback processes transaction rollback in worker goroutine (lock-free)
func (tx *Transaction) handleRollback(requestID []byte) StreamResponse {
	rollbackReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId:    requestID,
				Metadata: make(map[string]string),
				Req: &pb.Transaction_Req_RollbackReq{
					RollbackReq: &pb.Transaction_Rollback_Req{},
				},
			},
		},
	}

	if err := tx.stream.Send(&rollbackReq); err != nil {
		return StreamResponse{Error: fmt.Errorf("failed to send rollback request: %w", err)}
	}

	// Wait for rollback response
	resp, err := tx.stream.Recv()
	if err != nil {
		return StreamResponse{Error: fmt.Errorf("failed to receive rollback response: %w", err)}
	}

	if resp.GetRes() == nil || resp.GetRes().GetRollbackRes() == nil {
		return StreamResponse{Error: fmt.Errorf("invalid rollback response")}
	}

	tx.closed.Store(true)
	return StreamResponse{} // Success, no result needed
}

// handleClose processes transaction close in worker goroutine (lock-free)
func (tx *Transaction) handleClose() StreamResponse {
	tx.closed.Store(true)
	if tx.stream != nil {
		if err := tx.stream.CloseSend(); err != nil {
			return StreamResponse{Error: err}
		}
	}
	return StreamResponse{} // Success
}

// ExecuteBundle executes a complete bundle of operations atomically
// This is the ONLY public method for executing transaction operations
// Ensures atomic execution with no possibility of interleaving
//
// The method automatically handles transaction lifecycle:
// - For Write/Schema transactions: automatically adds OpCommit and OpClose if not present
// - For Read transactions: automatically adds OpClose if not present
// - Ensures correct operation order (Commit before Close)
func (tx *Transaction) executeBundle(ctx context.Context, operations []BundleOperation) ([]*QueryResult, error) {
	// Check if already closed
	if tx.closed.Load() {
		return nil, fmt.Errorf("transaction is closed")
	}

	// Automatically add OpOpen if transaction hasn't been opened yet
	// This ensures Bundle is a complete atomic unit from Open to Close
	if !tx.opened.Load() {
		operations = append([]BundleOperation{{Type: OpOpen}}, operations...)
	}

	// Intelligently complete the bundle with necessary operations
	operations = tx.autoCompleteBundle(operations)

	// Create response channel
	responseChan := make(chan BundleResponse, 1)

	// Send bundle to worker
	select {
	case tx.bundleChan <- BundleRequest{
		Operations: operations,
		Response:   responseChan,
	}:
		// Successfully sent

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-tx.workerDone:
		// Worker has exited
		if err := tx.workerErr.Load(); err != nil {
			return nil, err.(error)
		}
		return nil, fmt.Errorf("worker goroutine exited unexpectedly")
	}

	// Wait for response
	select {
	case resp := <-responseChan:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Results, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-tx.workerDone:
		// Worker has exited
		if err := tx.workerErr.Load(); err != nil {
			return nil, err.(error)
		}
		return nil, fmt.Errorf("worker goroutine exited unexpectedly")
	}
}

// autoCompleteBundle intelligently adds missing commit/close operations
func (tx *Transaction) autoCompleteBundle(operations []BundleOperation) []BundleOperation {
	if len(operations) == 0 {
		return operations
	}

	lastOpIndex := len(operations) - 1
	lastOp := operations[lastOpIndex].Type

	// Case 1: Last operation is Execute - need to add commit (if needed) and close
	if lastOp == OpExecute {
		// Write/Schema transactions need commit
		if tx.txType == Write || tx.txType == Schema {
			operations = append(operations, BundleOperation{Type: OpCommit})
		}
		operations = append(operations, BundleOperation{Type: OpClose})
		return operations
	}

	// Case 2: Last operation is Commit - only need to add close
	if lastOp == OpCommit {
		operations = append(operations, BundleOperation{Type: OpClose})
		return operations
	}

	// Case 3: Last operation is Rollback - only need to add close
	if lastOp == OpRollback {
		operations = append(operations, BundleOperation{Type: OpClose})
		return operations
	}

	// Case 4: Last operation is Close - check if commit is needed before close
	if lastOp == OpClose {
		// Only Write/Schema transactions need commit
		if tx.txType == Write || tx.txType == Schema {
			// Look backwards to see if there's already a commit or rollback
			hasCommitOrRollback := false
			for i := lastOpIndex - 1; i >= 0; i-- {
				if operations[i].Type == OpCommit || operations[i].Type == OpRollback {
					hasCommitOrRollback = true
					break
				}
			}

			// If no commit/rollback found, insert commit before close
			if !hasCommitOrRollback {
				// Create new slice with commit inserted before close
				newOps := make([]BundleOperation, 0, len(operations)+1)
				newOps = append(newOps, operations[:lastOpIndex]...)
				newOps = append(newOps, BundleOperation{Type: OpCommit})
				newOps = append(newOps, operations[lastOpIndex])
				return newOps
			}
		}
	}

	// Bundle is already complete
	return operations
}


// receiveRowStream receive row stream results
func (tx *Transaction) receiveRowStream(result *QueryResult) error {
	// Continue receiving stream responses until completion
	for {
		resp, err := tx.stream.Recv()
		if err != nil {
			// io.EOF indicates normal stream termination
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to receive stream part: %w", err)
		}

		// Check if it's a ResPart response
		if resPart := resp.GetResPart(); resPart != nil {
			// Check QueryRes section
			if queryRes := resPart.GetQueryRes(); queryRes != nil {
				// Check if contains row data
				if rowsRes := queryRes.GetRowsRes(); rowsRes != nil {
					// Add row data to results
					for _, row := range rowsRes.GetRows() {
						rowData := make([]interface{}, len(row.GetRow()))
						for i, entry := range row.GetRow() {
							rowData[i] = convertRowEntry(entry)
						}
						result.rows = append(result.rows, rowData)
					}
				}
			}

			// Check StreamRes section (stream control signals)
			if streamRes := resPart.GetStreamRes(); streamRes != nil {
				// Check if stream ends
				if streamRes.GetDone() != nil {
					return nil
				}
				// If it's a Continue signal, continue receiving
				if streamRes.GetContinue() != nil {
					continue
				}
			}
		}
	}
}

// receiveDocumentStream receive document stream results
func (tx *Transaction) receiveDocumentStream(result *QueryResult) error {
	// Continue receiving stream responses until completion
	for {
		resp, err := tx.stream.Recv()
		if err != nil {
			// io.EOF indicates normal stream termination
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to receive stream part: %w", err)
		}

		// Check if it's a ResPart response
		if resPart := resp.GetResPart(); resPart != nil {
			// Check QueryRes section
			if queryRes := resPart.GetQueryRes(); queryRes != nil {
				// Check if contains document data
				if docsRes := queryRes.GetDocumentsRes(); docsRes != nil {
					// Add document data to results
					for _, doc := range docsRes.GetDocuments() {
						docMap := convertDocument(doc)
						result.documents = append(result.documents, docMap)
					}
				}
			}

			// Check StreamRes section (stream control signals)
			if streamRes := resPart.GetStreamRes(); streamRes != nil {
				// Check if stream ends
				if streamRes.GetDone() != nil {
					return nil
				}
				// If it's a Continue signal, continue receiving
				if streamRes.GetContinue() != nil {
					continue
				}
			}
		}
	}
}

// convertConcept converts protobuf Concept to strongly-typed TypedValue
// This returns a TypedValue with TypeConcept containing structured concept data
func convertConcept(concept *pb.Concept) *TypedValue {
	if concept == nil {
		return &TypedValue{valueType: TypeNull, isNull: true}
	}

	// Create Concept structure based on concept specific type
	var conceptVal *Concept

	switch c := concept.GetConcept().(type) {
	case *pb.Concept_Entity:
		conceptVal = &Concept{
			Type: "entity",
			IID:  string(c.Entity.GetIid()),
		}
	case *pb.Concept_Relation:
		conceptVal = &Concept{
			Type: "relation",
			IID:  string(c.Relation.GetIid()),
		}
	case *pb.Concept_Attribute:
		conceptVal = &Concept{
			Type: "attribute",
			IID:  string(c.Attribute.GetIid()),
		}
		// Process attribute value if present
		if value := c.Attribute.GetValue(); value != nil {
			conceptVal.Value = convertValue(value)
		}
	case *pb.Concept_EntityType:
		conceptVal = &Concept{
			Type:  "entity_type",
			Label: c.EntityType.GetLabel(),
		}
	case *pb.Concept_RelationType:
		conceptVal = &Concept{
			Type:  "relation_type",
			Label: c.RelationType.GetLabel(),
		}
	case *pb.Concept_AttributeType:
		conceptVal = &Concept{
			Type:  "attribute_type",
			Label: c.AttributeType.GetLabel(),
		}
	case *pb.Concept_RoleType:
		conceptVal = &Concept{
			Type:  "role_type",
			Label: c.RoleType.GetLabel(),
		}
	default:
		// For unknown types, return as unknown with raw value
		return &TypedValue{
			valueType: TypeUnknown,
			rawValue:  concept.String(),
		}
	}

	return &TypedValue{
		valueType:  TypeConcept,
		conceptVal: conceptVal,
	}
}

// convertDocument convert ConceptDocument to map
func convertDocument(doc *pb.ConceptDocument) map[string]interface{} {
	if doc == nil {
		return nil
	}

	if root := doc.GetRoot(); root != nil {
		if converted := convertDocumentNode(root); converted != nil {
			if m, ok := converted.(map[string]interface{}); ok {
				return m
			}
			// If not a map, wrap in one
			return map[string]interface{}{"value": converted}
		}
	}
	return make(map[string]interface{})
}

// convertDocumentNode recursively convert document nodes
func convertDocumentNode(node *pb.ConceptDocument_Node) interface{} {
	if node == nil {
		return nil
	}

	// Convert based on node type
	switch n := node.GetNode().(type) {
	case *pb.ConceptDocument_Node_Map_:
		mapData := make(map[string]interface{})
		for k, v := range n.Map.Map {
			mapData[k] = convertDocumentNode(v)
		}
		return mapData
	case *pb.ConceptDocument_Node_List_:
		items := make([]interface{}, 0, len(n.List.List))
		for _, elem := range n.List.List {
			items = append(items, convertDocumentNode(elem))
		}
		return map[string]interface{}{"list": items}
	case *pb.ConceptDocument_Node_Leaf_:
		// Leaf node contains Concept or Value
		if leaf := n.Leaf; leaf != nil {
			switch l := leaf.GetLeaf().(type) {
			case *pb.ConceptDocument_Node_Leaf_EntityType:
				return map[string]interface{}{
					"type":  "entity_type",
					"label": l.EntityType.GetLabel(),
				}
			case *pb.ConceptDocument_Node_Leaf_RelationType:
				return map[string]interface{}{
					"type":  "relation_type",
					"label": l.RelationType.GetLabel(),
				}
			case *pb.ConceptDocument_Node_Leaf_AttributeType:
				return map[string]interface{}{
					"type":  "attribute_type",
					"label": l.AttributeType.GetLabel(),
				}
			case *pb.ConceptDocument_Node_Leaf_RoleType:
				return map[string]interface{}{
					"type":  "role_type",
					"label": l.RoleType.GetLabel(),
				}
			case *pb.ConceptDocument_Node_Leaf_Attribute:
				return convertConcept(&pb.Concept{Concept: &pb.Concept_Attribute{Attribute: l.Attribute}})
			case *pb.ConceptDocument_Node_Leaf_Value:
				return convertValue(l.Value)
			}
		}
	}

	return nil
}

// processQueryResponse processes a single query response and returns true if query is complete
func (tx *Transaction) processQueryResponse(resp *pb.Transaction_Server, result *QueryResult) bool {
	// IMPORTANT: Document streams may receive Res messages (not just ResPart)
	// When receiving Res in document stream, it typically signals query completion
	if res := resp.GetRes(); res != nil {
		// Res message in stream context means query is done
		// Rust driver: when Done is received, stream is closed (no more messages)
		return true
	}

	// Check if it's a ResPart response
	if resPart := resp.GetResPart(); resPart != nil {
		// Check QueryRes section
		if queryRes := resPart.GetQueryRes(); queryRes != nil {
			// Check if contains row data
			if rowsRes := queryRes.GetRowsRes(); rowsRes != nil {
				// Add row data to results
				for _, row := range rowsRes.GetRows() {
					// Build legacy interface{} row for backward compatibility
					rowData := make([]interface{}, len(row.GetRow()))

					// Build strongly-typed row
					typedRow := &TypedRow{
						columns: make(map[string]*TypedValue),
					}

					for i, entry := range row.GetRow() {
						// Convert entry to TypedValue
						typedVal := convertRowEntry(entry)

						// For backward compatibility, extract the raw value
						if typedVal.valueType == TypeInt64 {
							rowData[i] = typedVal.int64Val
						} else if typedVal.valueType == TypeString {
							rowData[i] = typedVal.stringVal
						} else if typedVal.valueType == TypeBool {
							rowData[i] = typedVal.boolVal
						} else if typedVal.valueType == TypeFloat64 {
							rowData[i] = typedVal.float64Val
						} else if typedVal.valueType == TypeConcept {
							// For concepts, create backward-compatible map
							if typedVal.conceptVal != nil {
								conceptMap := map[string]interface{}{
									"type": typedVal.conceptVal.Type,
								}
								if typedVal.conceptVal.IID != "" {
									conceptMap["iid"] = typedVal.conceptVal.IID
								}
								if typedVal.conceptVal.Label != "" {
									conceptMap["label"] = typedVal.conceptVal.Label
								}
								if typedVal.conceptVal.Value != nil && typedVal.conceptVal.Value.valueType == TypeInt64 {
									conceptMap["value"] = typedVal.conceptVal.Value.int64Val
								} else if typedVal.conceptVal.Value != nil && typedVal.conceptVal.Value.valueType == TypeString {
									conceptMap["value"] = typedVal.conceptVal.Value.stringVal
								} else if typedVal.conceptVal.Value != nil {
									// Handle other value types
									conceptMap["value"] = typedVal.conceptVal.Value.rawValue
								}
								rowData[i] = conceptMap
							} else {
								rowData[i] = nil
							}
						} else if typedVal.isNull {
							rowData[i] = nil
						} else {
							// For complex types, use raw value
							rowData[i] = typedVal.rawValue
						}

						// Store typed value with column name if available
						if i < len(result.ColumnNames) {
							typedRow.columns[result.ColumnNames[i]] = typedVal
						} else {
							// If no column name, use index as key
							typedRow.columns[fmt.Sprintf("col_%d", i)] = typedVal
						}
					}

					// Append both for backward compatibility and new type-safe access
					result.rows = append(result.rows, rowData)
					result.TypedRows = append(result.TypedRows, typedRow)
				}
			}

			// Check if contains document data
			if documentsRes := queryRes.GetDocumentsRes(); documentsRes != nil {
				// Add document data to results
				for _, doc := range documentsRes.GetDocuments() {
					docData := convertDocument(doc)
					result.documents = append(result.documents, docData)

					// Convert to TypedDocument for type-safe access
					typedDoc := convertMapToTypedDocument(docData)
					result.TypedDocuments = append(result.TypedDocuments, typedDoc)
				}
			}
		}

		// Check StreamRes section (stream control signals)
		if streamRes := resPart.GetStreamRes(); streamRes != nil {
			// CRITICAL: Check for stream error FIRST (before Done/Continue)
			// This matches Rust driver behavior: QueryResponse::Error handling
			if errRes := streamRes.GetError(); errRes != nil {
				// Error in stream - stop processing and let caller handle error
				// The error will bubble up and be reported to user
				return true // Stop processing due to error
			}

			// Check if stream ends
			if streamRes.GetDone() != nil {
				return true // Query complete
			}
			// If it's a Continue signal, send StreamSignal.Req and continue receiving
			if streamRes.GetContinue() != nil {

				// Generate request ID for StreamSignal
				streamReqID := tx.generateRequestID()

				// Build StreamSignal request (参考Rust实现: TransactionRequest::Stream { request_id })
				streamSignalReq := pb.Transaction_Client{
					Reqs: []*pb.Transaction_Req{
						{
							ReqId:    streamReqID,
							Metadata: make(map[string]string),
							Req: &pb.Transaction_Req_StreamReq{
								StreamReq: &pb.Transaction_StreamSignal_Req{}, // Empty message as per proto definition
							},
						},
					},
				}

				// Send StreamSignal request to server
				if err := tx.stream.Send(&streamSignalReq); err != nil {
					return true // Error occurred, stop processing
				}

				return false // Continue receiving
			}
		}
	}

	return false // Continue receiving
}

// convertRowEntry converts protobuf RowEntry to strongly-typed TypedValue
// Handles single values, concepts, and lists
func convertRowEntry(entry *pb.RowEntry) *TypedValue {
	if entry == nil {
		return &TypedValue{valueType: TypeNull, isNull: true}
	}

	switch e := entry.GetEntry().(type) {
	case *pb.RowEntry_Empty_:
		return &TypedValue{valueType: TypeNull, isNull: true}
	case *pb.RowEntry_Concept:
		return convertConcept(e.Concept)
	case *pb.RowEntry_Value:
		return convertValue(e.Value)
	case *pb.RowEntry_ConceptList_:
		// Convert concept list to TypedValue list
		items := make([]TypedValue, 0, len(e.ConceptList.GetConcepts()))
		for _, c := range e.ConceptList.GetConcepts() {
			if typedVal := convertConcept(c); typedVal != nil {
				items = append(items, *typedVal)
			}
		}
		return &TypedValue{
			valueType: TypeConceptList,
			listVal:   items,
		}
	case *pb.RowEntry_ValueList_:
		// Convert value list to TypedValue list
		items := make([]TypedValue, 0, len(e.ValueList.GetValues()))
		for _, v := range e.ValueList.GetValues() {
			if typedVal := convertValue(v); typedVal != nil {
				items = append(items, *typedVal)
			}
		}
		return &TypedValue{
			valueType: TypeValueList,
			listVal:   items,
		}
	default:
		return &TypedValue{valueType: TypeNull, isNull: true}
	}
}

// convertValue converts protobuf Value to strongly-typed TypedValue
// This ensures type safety by returning TypedValue instead of interface{}
func convertValue(value *pb.Value) *TypedValue {
	if value == nil {
		return &TypedValue{valueType: TypeNull, isNull: true}
	}

	switch v := value.GetValue().(type) {
	case *pb.Value_String_:
		return &TypedValue{
			valueType: TypeString,
			stringVal: v.String_,
		}
	case *pb.Value_Boolean:
		return &TypedValue{
			valueType: TypeBool,
			boolVal:   v.Boolean,
		}
	case *pb.Value_Integer:
		// All integer types (integer, count, long) map to int64
		return &TypedValue{
			valueType: TypeInt64,
			int64Val:  v.Integer,
		}
	case *pb.Value_Double:
		return &TypedValue{
			valueType:  TypeFloat64,
			float64Val: v.Double,
		}
	case *pb.Value_Date_:
		return &TypedValue{
			valueType: TypeDate,
			dateVal: &Date{
				NumDaysSinceCE: v.Date.NumDaysSinceCe,
			},
		}
	case *pb.Value_Datetime_:
		return &TypedValue{
			valueType: TypeDateTime,
			dateTimeVal: &DateTime{
				Seconds: v.Datetime.Seconds,
				Nanos:   int32(v.Datetime.Nanos),
			},
		}
	case *pb.Value_DatetimeTz:
		// DateTimeTz is mapped to DateTime (ignoring timezone for now)
		if v.DatetimeTz.GetDatetime() != nil {
			return &TypedValue{
				valueType: TypeDateTime,
				dateTimeVal: &DateTime{
					Seconds: v.DatetimeTz.GetDatetime().Seconds,
					Nanos:   int32(v.DatetimeTz.GetDatetime().Nanos),
				},
			}
		}
		return &TypedValue{valueType: TypeNull, isNull: true}
	case *pb.Value_Duration_:
		return &TypedValue{
			valueType: TypeDuration,
			durationVal: &Duration{
				Months: int32(v.Duration.Months),
				Days:   int32(v.Duration.Days),
				Nanos:  int64(v.Duration.Nanos),
			},
		}
	case *pb.Value_Decimal_:
		return &TypedValue{
			valueType: TypeDecimal,
			decimalVal: &Decimal{
				Integer:    v.Decimal.Integer,
				Fractional: int32(v.Decimal.Fractional),
			},
		}
	default:
		// Unknown type - store raw value for debugging
		return &TypedValue{
			valueType: TypeUnknown,
			rawValue:  value.String(),
		}
	}
}

// convertQueryType convert query type
func convertQueryType(queryType pb.Query_Type) QueryType {
	switch queryType {
	case pb.Query_READ:
		return QueryTypeRead
	case pb.Query_WRITE:
		return QueryTypeWrite
	case pb.Query_SCHEMA:
		return QueryTypeSchema
	default:
		return QueryTypeUnknown
	}
}



// generateRequestID generate unique request ID (must be 16-byte UUID v4 format like Rust implementation)
func (tx *Transaction) generateRequestID() []byte {
	// Generate standard UUID v4 (16 bytes) exactly like Rust: Uuid::new_v4().as_bytes().to_vec()
	uuid := make([]byte, 16)
	rand.Read(uuid)

	// Set version (4) in bits 12-15 of the 6th byte
	uuid[6] = (uuid[6] & 0x0f) | 0x40

	// Set variant (10) in bits 6-7 of the 8th byte
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return uuid
}

// convertTxTypeToPB convert transaction type to protobuf enum
func convertTxTypeToPB(txType TransactionType) pb.Transaction_Type {
	switch txType {
	case Read:
		return pb.Transaction_READ
	case Write:
		return pb.Transaction_WRITE
	case Schema:
		return pb.Transaction_SCHEMA
	default:
		return pb.Transaction_READ
	}
}