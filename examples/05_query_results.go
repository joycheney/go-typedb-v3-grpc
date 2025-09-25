package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 5: Query Result Processing
// This example demonstrates the query result processing features of TypeDB v3 gRPC client, including:
// - Processing different types of query results (Done, RowStream, DocumentStream)
// - Parsing RowStream results for column names and row data
// - Parsing DocumentStream results for structured data
// - Handling complex data types (concepts, attribute values, nested structures, etc.)
// - Query result serialization and formatted output

func main() {
	fmt.Println("=== TypeDB v3 gRPC Query Result Processing Example ===")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create test database
	testDbName := "query_results_demo"
	ctx := context.Background()

	// Ensure test database exists with data
	if err := setupTestDatabaseWithData(ctx, client, testDbName); err != nil {
		log.Fatalf("Failed to prepare test data: %v", err)
	}
	defer cleanupTestDatabase(ctx, client, testDbName)

	// Get database object
	database := client.GetDatabase(testDbName)

	// Demo 1: Done type result processing
	demonstrateDoneResults(ctx, database)

	// Demo 2: RowStream result processing
	demonstrateRowStreamResults(ctx, database)

	// Demo 3: DocumentStream result processing
	demonstrateDocumentStreamResults(ctx, database)

	// Demo 4: Complex data type processing
	demonstrateComplexDataTypes(ctx, database)

	// Demo 5: Result serialization and formatting
	demonstrateResultSerialization(ctx, database)

	fmt.Println("=== Query Result Processing Example Completed ===")
}

// demonstrateDoneResults demonstrates Done type result processing
func demonstrateDoneResults(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 1: Done Type Result Processing ---\n")

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Done type results usually come from insert, delete and other operations
	doneQueries := []struct {
		name  string
		query string
	}{
		{"Insert operation", `insert $p isa person, has name "ResultTestPerson";`},
		{"Delete operation", `match $p isa person, has name "ResultTestPerson"; delete $p;`},
	}

	for _, q := range doneQueries {
		fmt.Printf("\nExecuting %s:\n", q.name)
		fmt.Printf("Query: %s\n", q.query)

		result, err := database.ExecuteWrite(queryCtx, q.query)
		if err != nil {
			fmt.Printf("  Execution failed: %v\n", err)
			continue
		}

		// Process Done type result
		if result.IsDone {
			fmt.Printf("  ✓ Operation completed\n")
			fmt.Printf("  Query type: %s\n", getQueryTypeDescription(result.QueryType))
			fmt.Printf("  Result characteristics: IsDone=true, no stream data\n")
		} else {
			fmt.Printf("  Unexpected result type: %s\n", getResultTypeDescription(result))
		}
	}
}

// demonstrateRowStreamResults demonstrates RowStream result processing
func demonstrateRowStreamResults(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 2: RowStream Result Processing ---\n")

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// RowStream results come from match, select, and reduce queries
	rowQueries := []struct {
		name  string
		query string
	}{
		{"Basic entity query", "match $p isa person; limit 3;"},
		{"SELECT specific variables", "match $p isa person, has name $name; select $p, $name; limit 3;"},
		{"Attribute query", "match $p isa person, has name $name; limit 3;"},
		{"REDUCE aggregation", "match $p isa person; reduce $count = count($p);"},
	}

	for _, q := range rowQueries {
		fmt.Printf("\nExecuting %s:\n", q.name)
		fmt.Printf("Query: %s\n", q.query)

		result, err := database.ExecuteRead(queryCtx, q.query)
		if err != nil {
			fmt.Printf("  Execution failed: %v\n", err)
			continue
		}

		// Process RowStream type result
		if result.IsRowStream {
			fmt.Printf("  ✓ Row stream result\n")
			fmt.Printf("  Column count: %d\n", len(result.ColumnNames))
			fmt.Printf("  Row count: %d\n", len(result.Rows))

			// Display column names
			if len(result.ColumnNames) > 0 {
				fmt.Printf("  Column names: %v\n", result.ColumnNames)
			}

			// Display first few rows of data
			maxRows := 3
			for i, row := range result.Rows {
				if i >= maxRows {
					fmt.Printf("    ... (%d more rows)\n", len(result.Rows)-maxRows)
					break
				}

				fmt.Printf("  Row %d: ", i+1)
				for j, cell := range row {
					if j > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("%v", formatCellValue(cell))
				}
				fmt.Println()
			}
		} else {
			fmt.Printf("  Result type: %s\n", getResultTypeDescription(result))
		}
	}
}

// demonstrateDocumentStreamResults demonstrates DocumentStream result processing
func demonstrateDocumentStreamResults(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 3: DocumentStream Result Processing ---\n")

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// DocumentStream results come from fetch queries
	docQueries := []struct {
		name  string
		query string
	}{
		{"FETCH person details", `match $p isa person, has name $n; fetch { "name": $n };`},
		{"FETCH company details", `match $c isa company, has companyname $cn; fetch { "company": $cn };`},
		{"FETCH structured data", `match $p isa person, has name $n; fetch { "person": { "name": $n } };`},
	}

	for _, q := range docQueries {
		fmt.Printf("\nExecuting %s:\n", q.name)
		fmt.Printf("Query: %s\n", q.query)

		result, err := database.ExecuteRead(queryCtx, q.query)
		if err != nil {
			fmt.Printf("  Execution failed: %v\n", err)
			continue
		}

		// Process DocumentStream type result
		if result.IsDocumentStream {
			fmt.Printf("  ✓ Document stream result\n")
			fmt.Printf("  Document count: %d\n", len(result.Documents))

			// Display first few documents
			maxDocs := 3
			for i, doc := range result.Documents {
				if i >= maxDocs {
					fmt.Printf("    ... (%d more documents)\n", len(result.Documents)-maxDocs)
					break
				}

				fmt.Printf("  Document %d:\n", i+1)
				displayDocument(doc, "    ")
			}
		} else {
			fmt.Printf("  Result type: %s\n", getResultTypeDescription(result))
		}
	}
}

// demonstrateComplexDataTypes demonstrates complex data type processing
func demonstrateComplexDataTypes(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 4: Complex Data Type Processing ---\n")

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Query results containing different data types
	query := "match $p isa person, has name $name; limit 2;"

	result, err := database.ExecuteRead(queryCtx, query)
	if err != nil {
		fmt.Printf("Failed to execute complex data type query: %v\n", err)
		return
	}

	if !result.IsRowStream {
		fmt.Printf("Expected row stream result, but got: %s\n", getResultTypeDescription(result))
		return
	}

	fmt.Printf("Complex data type analysis:\n")
	fmt.Printf("Column names: %v\n", result.ColumnNames)

	for i, row := range result.Rows {
		fmt.Printf("\nRow %d data type analysis:\n", i+1)

		for j, cell := range row {
			colName := "unknown column"
			if j < len(result.ColumnNames) {
				colName = result.ColumnNames[j]
			}

			fmt.Printf("  Column '%s': %s\n", colName, analyzeDataType(cell))
		}
	}
}

// demonstrateResultSerialization demonstrates result serialization and formatting
func demonstrateResultSerialization(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 5: Result Serialization and Formatting ---\n")

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Get a query result containing multiple data types
	query := "match $p isa person, has name $name; limit 2;"

	result, err := database.ExecuteRead(queryCtx, query)
	if err != nil {
		fmt.Printf("Failed to execute serialization test query: %v\n", err)
		return
	}

	// 1. JSON serialization
	fmt.Printf("1. JSON serialization:\n")
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Printf("JSON serialization failed: %v\n", err)
	} else {
		fmt.Printf("%s\n", string(jsonData))
	}

	// 2. Table formatting
	if result.IsRowStream {
		fmt.Printf("\n2. Table formatting:\n")
		displayTable(result)
	}

	// 3. Custom formatting
	fmt.Printf("\n3. Custom formatting:\n")
	displayCustomFormat(result)
}

// Helper functions

// setupTestDatabaseWithData sets up database with test data
func setupTestDatabaseWithData(ctx context.Context, client *typedbclient.Client, dbName string) error {
	// Ensure database exists
	exists, err := client.DatabaseExists(ctx, dbName)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %w", err)
	}

	if exists {
		// Delete existing database to get a clean state
		if err := client.DeleteDatabase(ctx, dbName); err != nil {
			return fmt.Errorf("failed to delete existing database: %w", err)
		}
	}

	// Create new database
	if err := client.CreateDatabase(ctx, dbName); err != nil {
		return fmt.Errorf("failed to create test database: %w", err)
	}

	database := client.GetDatabase(dbName)

	// Define schema - TypeDB 3.x syntax (exactly as example 03)
	schemaQuery := `
		define
		entity person, owns name;
		entity company, owns companyname;
		attribute name, value string;
		attribute companyname, value string;
	`

	if _, err := database.ExecuteSchema(ctx, schemaQuery); err != nil {
		return fmt.Errorf("failed to define schema: %w", err)
	}

	// Insert test data
	dataQuery := `
		insert
		$alice isa person, has name "Alice";
		$bob isa person, has name "Bob";
		$charlie isa person, has name "Charlie";
		$david isa person, has name "David";
		$company1 isa company, has companyname "TechCorp";
	`

	if _, err := database.ExecuteWrite(ctx, dataQuery); err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	fmt.Printf("✓ Test database '%s' created and populated with test data\n", dbName)
	return nil
}

// cleanupTestDatabase cleans up test database
func cleanupTestDatabase(ctx context.Context, client *typedbclient.Client, dbName string) {
	if err := client.DeleteDatabase(ctx, dbName); err != nil {
		fmt.Printf("Failed to clean up test database: %v\n", err)
	} else {
		fmt.Printf("✓ Cleaned up test database: %s\n", dbName)
	}
}

// formatCellValue formats cell value
func formatCellValue(value interface{}) string {
	if value == nil {
		return "null"
	}

	// Handle map type (concept objects)
	if m, ok := value.(map[string]interface{}); ok {
		if conceptType, exists := m["type"]; exists {
			if conceptType == "entity" || conceptType == "relation" || conceptType == "attribute" {
				if iid, hasIid := m["iid"]; hasIid {
					return fmt.Sprintf("%s[%v]", conceptType, iid)
				}
			}
			if conceptType == "entity_type" || conceptType == "relation_type" || conceptType == "attribute_type" {
				if label, hasLabel := m["label"]; hasLabel {
					return fmt.Sprintf("%s:%s", conceptType, label)
				}
			}
			if conceptType == "attribute" {
				if attrValue, hasValue := m["value"]; hasValue {
					return fmt.Sprintf("attr[%v]", attrValue)
				}
			}
		}
		// Other map types, display simplified format
		return fmt.Sprintf("map[%d keys]", len(m))
	}

	// Handle basic types
	return fmt.Sprintf("%v", value)
}

// analyzeDataType analyzes data type
func analyzeDataType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case string:
		return fmt.Sprintf("string: \"%s\"", v)
	case int, int64:
		return fmt.Sprintf("integer: %v", v)
	case float64:
		return fmt.Sprintf("float: %v", v)
	case bool:
		return fmt.Sprintf("boolean: %v", v)
	case map[string]interface{}:
		if conceptType, exists := v["type"]; exists {
			return fmt.Sprintf("concept object (%s): %v", conceptType, v)
		}
		return fmt.Sprintf("map object (keys: %v)", getMapKeys(v))
	case []interface{}:
		return fmt.Sprintf("array: [%d elements]", len(v))
	default:
		return fmt.Sprintf("other type (%T): %v", value, value)
	}
}

// displayDocument displays document structure
func displayDocument(doc map[string]interface{}, indent string) {
	for key, value := range doc {
		fmt.Printf("%s%s: ", indent, key)
		if subDoc, ok := value.(map[string]interface{}); ok {
			fmt.Println("{")
			displayDocument(subDoc, indent+"  ")
			fmt.Printf("%s}\n", indent)
		} else if list, ok := value.([]interface{}); ok {
			fmt.Printf("[\n")
			for i, item := range list {
				fmt.Printf("%s  [%d]: %v\n", indent, i, item)
			}
			fmt.Printf("%s]\n", indent)
		} else {
			fmt.Printf("%v\n", value)
		}
	}
}

// displayTable displays row stream results in table format
func displayTable(result *typedbclient.QueryResult) {
	if !result.IsRowStream {
		return
	}

	// Calculate column widths
	colWidths := make([]int, len(result.ColumnNames))
	for i, colName := range result.ColumnNames {
		colWidths[i] = len(colName)
	}

	for _, row := range result.Rows {
		for i, cell := range row {
			if i < len(colWidths) {
				cellStr := formatCellValue(cell)
				if len(cellStr) > colWidths[i] {
					colWidths[i] = len(cellStr)
				}
			}
		}
	}

	// Print table header
	fmt.Print("|")
	for i, colName := range result.ColumnNames {
		fmt.Printf(" %-*s |", colWidths[i], colName)
	}
	fmt.Println()

	// Print separator line
	fmt.Print("|")
	for _, width := range colWidths {
		fmt.Print(string(make([]byte, width+2)))
		for j := 0; j < width+2; j++ {
			fmt.Print("-")
		}
		fmt.Print("|")
	}
	fmt.Println()

	// Print data rows
	maxRows := 5
	for i, row := range result.Rows {
		if i >= maxRows {
			fmt.Printf("... (%d more rows)\n", len(result.Rows)-maxRows)
			break
		}

		fmt.Print("|")
		for j, cell := range row {
			if j < len(colWidths) {
				cellStr := formatCellValue(cell)
				fmt.Printf(" %-*s |", colWidths[j], cellStr)
			}
		}
		fmt.Println()
	}
}

// displayCustomFormat displays custom format
func displayCustomFormat(result *typedbclient.QueryResult) {
	fmt.Printf("Query result summary:\n")
	fmt.Printf("  - Query type: %s\n", getQueryTypeDescription(result.QueryType))
	fmt.Printf("  - Result type: %s\n", getResultTypeDescription(result))

	if result.IsRowStream {
		fmt.Printf("  - Row stream details: %d rows × %d columns\n", len(result.Rows), len(result.ColumnNames))
		if len(result.ColumnNames) > 0 {
			fmt.Printf("  - Column names: %v\n", result.ColumnNames)
		}
	}

	if result.IsDocumentStream {
		fmt.Printf("  - Document stream details: %d documents\n", len(result.Documents))
	}

	if result.IsDone {
		fmt.Printf("  - Operation completed, no return data\n")
	}
}

// getMapKeys gets the key list of a map
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getResultTypeDescription gets the description of result type
func getResultTypeDescription(result *typedbclient.QueryResult) string {
	if result.IsDone {
		return "Done (operation completed)"
	}
	if result.IsRowStream {
		return fmt.Sprintf("RowStream (row stream, %d rows)", len(result.Rows))
	}
	if result.IsDocumentStream {
		return fmt.Sprintf("DocumentStream (document stream, %d documents)", len(result.Documents))
	}
	return "Unknown (unknown type)"
}

// getQueryTypeDescription gets the description of query type
func getQueryTypeDescription(queryType typedbclient.QueryType) string {
	switch queryType {
	case typedbclient.QueryTypeRead:
		return "READ"
	case typedbclient.QueryTypeWrite:
		return "WRITE"
	case typedbclient.QueryTypeSchema:
		return "SCHEMA"
	default:
		return "UNKNOWN"
	}
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running
// 2. Run this example: go run examples/05_query_results.go
// 3. Observe processing and formatting of different types of query results
//
// QueryResult structure covered in this example:
// - IsDone: Operation completed flag
// - IsRowStream: Row stream result flag
// - IsDocumentStream: Document stream result flag
// - ColumnNames: Column names (row stream results)
// - Rows: Row data (row stream results)
// - Documents: Document data (document stream results)
// - QueryType: Query type identifier
//
// Data type processing:
// - Basic types: string, int, float, boolean
// - Concept objects: entity, relation, attribute, types
// - Composite types: map, array
// - Null value handling
//
// Formatting options:
// - JSON serialization
// - Table display
// - Custom format
// - Debug output

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
