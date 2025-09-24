package typedbclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	pb "github.com/joycheney/go-typedb-v3-grpc/pb/proto"
	"google.golang.org/grpc"
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
	Rows        [][]interface{} // Row data

	// Document stream results
	Documents []map[string]interface{} // Document list
}

// StreamOperation defines operation types for the worker
type StreamOperation int

const (
	OpExecute StreamOperation = iota
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

	// Producer/consumer channels for lock-free bundle communication
	bundleChan chan BundleRequest
	workerDone chan struct{}
	workerErr  atomic.Value // stores error

	// Request ID generator (lock-free)
	requestID atomic.Uint64
}

// BeginTransaction begin transaction
func (db *Database) BeginTransaction(ctx context.Context, txType TransactionType) (*Transaction, error) {
	tx := &Transaction{
		client:     db.client,
		database:   db.name,
		txType:     txType,
		bundleChan: make(chan BundleRequest, 10), // Buffered channel for bundle operations
		workerDone: make(chan struct{}),
	}

	// Try to create transaction stream with authentication error retry mechanism
	var stream grpc.BidiStreamingClient[pb.Transaction_Client, pb.Transaction_Server]
	var err error

	maxRetries := 2 // Maximum 2 retries
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get gRPC client atomically
		connRef := db.client.connRef.Load()
		if connRef == nil {
			return nil, fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		grpcClient := wrapper.grpcClient

		// Create transaction stream (using authentication context)
		authCtx := db.client.withAuth(ctx)
		stream, err = grpcClient.Transaction(authCtx)
		if err == nil {
			break // Successfully created stream
		}

		// Check if authentication error
		if isAuthError(err) {
			// Try to reauthenticate
			if authErr := db.client.authenticate(); authErr != nil {
				return nil, fmt.Errorf("reauthentication failed: %w", authErr)
			}
			continue // Retry
		}

		// Check if connection error
		if isConnectionError(err) {
			// Try to reconnect
			if reconnErr := db.client.tryReconnect(); reconnErr != nil {
				return nil, fmt.Errorf("reconnection failed: %w", reconnErr)
			}
			continue // Retry
		}

		// Return other errors directly
		return nil, fmt.Errorf("failed to create transaction stream: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create transaction stream after %d attempts: %w", maxRetries, err)
	}
	tx.stream = stream

	// Generate request ID
	reqID := tx.generateRequestID()

	// Send transaction open request (including required options field)
	openReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId: reqID,
				Req: &pb.Transaction_Req_OpenReq{
					OpenReq: &pb.Transaction_Open_Req{
						Database:             db.name,
						Type:                 convertTxTypeToPB(txType),
						Options:              &pb.Options_Transaction{}, // Add required options field
						NetworkLatencyMillis: 0,                          // Add required network latency field
					},
				},
			},
		},
	}

	if err := stream.Send(&openReq); err != nil {
		return nil, fmt.Errorf("failed to send open request: %w", err)
	}

	// Wait for open response
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive open response: %w", err)
	}

	// Validate response
	if resp.GetRes() == nil || resp.GetRes().GetOpenRes() == nil {
		return nil, fmt.Errorf("invalid open response")
	}

	// Store stream and start worker goroutine
	tx.stream = stream
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

	// Execute each operation in sequence
	for _, op := range operations {
		switch op.Type {
		case OpExecute:
			reqID := tx.generateRequestID()
			resp := tx.handleExecute(reqID, op.Query)
			if resp.Error != nil {
				return BundleResponse{Error: resp.Error}
			}
			results = append(results, resp.Result)

		case OpCommit:
			reqID := tx.generateRequestID()
			resp := tx.handleCommit(reqID)
			if resp.Error != nil {
				return BundleResponse{Error: resp.Error}
			}

		case OpRollback:
			reqID := tx.generateRequestID()
			resp := tx.handleRollback(reqID)
			if resp.Error != nil {
				return BundleResponse{Error: resp.Error}
			}

		case OpClose:
			resp := tx.handleClose()
			if resp.Error != nil {
				return BundleResponse{Error: resp.Error}
			}
		}
	}

	return BundleResponse{Results: results, Error: nil}
}

// handleExecute processes query execution in worker goroutine (lock-free)
func (tx *Transaction) handleExecute(requestID []byte, query string) StreamResponse {
	// Build query request
	queryReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId: requestID,
				Req: &pb.Transaction_Req_QueryReq{
					QueryReq: &pb.Query_Req{
						Query:   query,
						Options: &pb.Options_Query{}, // Use default options
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
		return StreamResponse{Error: fmt.Errorf("failed to receive query response: %w", err)}
	}

	result := &QueryResult{}

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
							return StreamResponse{Error: fmt.Errorf("failed to receive stream part: %w", err)}
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
					for {
						resp, err := tx.stream.Recv()
						if err != nil {
							if err == io.EOF {
								break
							}
							return StreamResponse{Error: fmt.Errorf("failed to receive stream part: %w", err)}
						}

						if tx.processQueryResponse(resp, result) {
							break
						}
					}
				}
			}

			// Check if it's an error response
			if errRes := queryInitRes.GetError(); errRes != nil {
				// Error struct has ErrorCode field, not Message
				return StreamResponse{Error: fmt.Errorf("query error: %s", errRes.GetErrorCode())}
			}
		}
	}

	// Check if it's a ResPart response (for streaming queries)
	if resPart := resp.GetResPart(); resPart != nil {
		// Process first part
		tx.processQueryResponse(resp, result)

		// Continue receiving stream responses
		for {
			resp, err := tx.stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return StreamResponse{Error: fmt.Errorf("failed to receive stream part: %w", err)}
			}

			if tx.processQueryResponse(resp, result) {
				break // Query complete
			}
		}
	}

	return StreamResponse{Result: result}
}

// handleCommit processes transaction commit in worker goroutine (lock-free)
func (tx *Transaction) handleCommit(requestID []byte) StreamResponse {
	commitReq := pb.Transaction_Client{
		Reqs: []*pb.Transaction_Req{
			{
				ReqId: requestID,
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
				ReqId: requestID,
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
func (tx *Transaction) ExecuteBundle(ctx context.Context, operations []BundleOperation) ([]*QueryResult, error) {
	// Check if already closed
	if tx.closed.Load() {
		return nil, fmt.Errorf("transaction is closed")
	}

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
						result.Rows = append(result.Rows, rowData)
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
						result.Documents = append(result.Documents, docMap)
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

// convertConcept convert Concept to generic interface{}
func convertConcept(concept *pb.Concept) interface{} {
	if concept == nil {
		return nil
	}

	// Return corresponding value based on concept specific type
	switch c := concept.GetConcept().(type) {
	case *pb.Concept_Entity:
		return map[string]interface{}{
			"type": "entity",
			"iid":  c.Entity.GetIid(),
		}
	case *pb.Concept_Relation:
		return map[string]interface{}{
			"type": "relation",
			"iid":  c.Relation.GetIid(),
		}
	case *pb.Concept_Attribute:
		attr := map[string]interface{}{
			"type": "attribute",
			"iid":  c.Attribute.GetIid(),
		}
		// Process attribute value
		if value := c.Attribute.GetValue(); value != nil {
			switch v := value.GetValue().(type) {
			case *pb.Value_String_:
				attr["value"] = v.String_
			case *pb.Value_Boolean:
				attr["value"] = v.Boolean
			case *pb.Value_Integer:
				attr["value"] = v.Integer
			case *pb.Value_Double:
				attr["value"] = v.Double
			case *pb.Value_Date_:
				attr["value"] = v.Date.NumDaysSinceCe
			case *pb.Value_Datetime_:
				attr["value"] = map[string]interface{}{
					"seconds": v.Datetime.Seconds,
					"nanos":   v.Datetime.Nanos,
				}
			case *pb.Value_DatetimeTz:
				attr["value"] = map[string]interface{}{
					"datetime": v.DatetimeTz.GetDatetime(),
				}
			case *pb.Value_Duration_:
				attr["value"] = map[string]interface{}{
					"months": v.Duration.Months,
					"days":   v.Duration.Days,
					"nanos":  v.Duration.Nanos,
				}
			case *pb.Value_Decimal_:
				attr["value"] = map[string]interface{}{
					"integer":    v.Decimal.Integer,
					"fractional": v.Decimal.Fractional,
				}
			}
		}
		return attr
	case *pb.Concept_EntityType:
		return map[string]interface{}{
			"type":  "entity_type",
			"label": c.EntityType.GetLabel(),
		}
	case *pb.Concept_RelationType:
		return map[string]interface{}{
			"type":  "relation_type",
			"label": c.RelationType.GetLabel(),
		}
	case *pb.Concept_AttributeType:
		return map[string]interface{}{
			"type":  "attribute_type",
			"label": c.AttributeType.GetLabel(),
		}
	case *pb.Concept_RoleType:
		return map[string]interface{}{
			"type":  "role_type",
			"label": c.RoleType.GetLabel(),
		}
	default:
		// For unknown types, return string representation
		return concept.String()
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
					result.Rows = append(result.Rows, rowData)
				}
			}

			// Check if contains document data
			if documentsRes := queryRes.GetDocumentsRes(); documentsRes != nil {
				// Add document data to results
				for _, doc := range documentsRes.GetDocuments() {
					docData := convertDocument(doc)
					result.Documents = append(result.Documents, docData)
				}
			}
		}

		// Check StreamRes section (stream control signals)
		if streamRes := resPart.GetStreamRes(); streamRes != nil {
			// Check if stream ends
			if streamRes.GetDone() != nil {
				return true // Query complete
			}
			// If it's a Continue signal, continue receiving
			if streamRes.GetContinue() != nil {
				return false // Continue receiving
			}
		}
	}

	return false // Continue receiving
}

// convertRowEntry convert row entry
func convertRowEntry(entry *pb.RowEntry) interface{} {
	if entry == nil {
		return nil
	}

	switch e := entry.GetEntry().(type) {
	case *pb.RowEntry_Empty_:
		return nil
	case *pb.RowEntry_Concept:
		return convertConcept(e.Concept)
	case *pb.RowEntry_Value:
		return convertValue(e.Value)
	case *pb.RowEntry_ConceptList_:
		items := make([]interface{}, 0, len(e.ConceptList.GetConcepts()))
		for _, c := range e.ConceptList.GetConcepts() {
			items = append(items, convertConcept(c))
		}
		return items
	case *pb.RowEntry_ValueList_:
		items := make([]interface{}, 0, len(e.ValueList.GetValues()))
		for _, v := range e.ValueList.GetValues() {
			items = append(items, convertValue(v))
		}
		return items
	default:
		return nil
	}
}

// convertValue convert Value to basic types
func convertValue(value *pb.Value) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.GetValue().(type) {
	case *pb.Value_String_:
		return v.String_
	case *pb.Value_Boolean:
		return v.Boolean
	case *pb.Value_Integer:
		return v.Integer
	case *pb.Value_Double:
		return v.Double
	case *pb.Value_Date_:
		return v.Date.NumDaysSinceCe
	case *pb.Value_Datetime_:
		return map[string]interface{}{
			"seconds": v.Datetime.Seconds,
			"nanos":   v.Datetime.Nanos,
		}
	case *pb.Value_DatetimeTz:
		return map[string]interface{}{
			"datetime": v.DatetimeTz.GetDatetime(),
		}
	case *pb.Value_Duration_:
		return map[string]interface{}{
			"months": v.Duration.Months,
			"days":   v.Duration.Days,
			"nanos":  v.Duration.Nanos,
		}
	case *pb.Value_Decimal_:
		return map[string]interface{}{
			"integer":    v.Decimal.Integer,
			"fractional": v.Decimal.Fractional,
		}
	default:
		return value.String()
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



// generateRequestID generate unique request ID (must be 16-byte UUID format)
func (tx *Transaction) generateRequestID() []byte {
	id := tx.requestID.Add(1)
	reqID := make([]byte, 16) // TypeDB requires 16 bytes

	// Generate UUID v4 format
	// First 8 bytes use timestamp and incremental ID
	binary.BigEndian.PutUint32(reqID[0:4], uint32(time.Now().Unix()))
	binary.BigEndian.PutUint32(reqID[4:8], uint32(id))

	// Last 8 bytes use random numbers
	rand.Read(reqID[8:16])

	// Set UUID v4 version bits (RFC 4122)
	reqID[6] = (reqID[6] & 0x0f) | 0x40 // Version 4
	reqID[8] = (reqID[8] & 0x3f) | 0x80 // Variant 10

	return reqID
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