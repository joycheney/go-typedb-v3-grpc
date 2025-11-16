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

	// Demo 5: Comprehensive Type-Safe Value Access
	demonstrateComprehensiveValueAccess(ctx, database)

	// Demo 6: Result serialization and formatting
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
			fmt.Printf("  Row count: %d\n", len(result.TypedRows))

			// Display column names
			if len(result.ColumnNames) > 0 {
				fmt.Printf("  Column names: %v\n", result.ColumnNames)
			}

			// Display first few rows using type-safe API
			maxRows := 3
			for i, typedRow := range result.TypedRows {
				if i >= maxRows {
					fmt.Printf("    ... (%d more rows)\n", len(result.TypedRows)-maxRows)
					break
				}

				fmt.Printf("  Row %d: ", i+1)
				for j, colName := range result.ColumnNames {
					if j > 0 {
						fmt.Printf(", ")
					}
					// Use type-safe GetValue method
					if val, err := typedRow.GetValue(colName); err == nil {
						fmt.Printf("%v", val)
					} else {
						fmt.Printf("<error: %v>", err)
					}
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
			fmt.Printf("  Document stream data is available (access through safe API methods)\n")
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

	fmt.Printf("Complex data type analysis (using type-safe API):\n")
	fmt.Printf("Column names: %v\n", result.ColumnNames)

	for i, typedRow := range result.TypedRows {
		fmt.Printf("\nRow %d data type analysis:\n", i+1)

		for _, colName := range result.ColumnNames {
			// Use type-safe GetValue to get typed value
			if val, err := typedRow.GetValue(colName); err == nil {
				fmt.Printf("  Column '%s': Type=%s, Value=%v\n", colName, val.Type(), val)
			} else {
				fmt.Printf("  Column '%s': <error: %v>\n", colName, err)
			}
		}
	}
}

// demonstrateComprehensiveValueAccess demonstrates how to safely access all types of values
func demonstrateComprehensiveValueAccess(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 5: Comprehensive Type-Safe Value Access ---\n")
	fmt.Printf("This demo shows how to use the type-safe API to access all TypeDB value types\n\n")

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 1. Demonstrate getting string values
	fmt.Println("1. Getting String Values (GetString):")
	query1 := "match $p isa person, has name $n; limit 2;"
	if result, err := database.ExecuteRead(queryCtx, query1); err == nil && result.IsRowStream {
		fmt.Printf("   Query returned %d rows (using GetRowCount())\n", result.GetRowCount())
		for i, row := range result.TypedRows {
			// Safe string retrieval
			if name, err := row.GetString("n"); err == nil {
				fmt.Printf("   Row %d: name = \"%s\" (using GetString())\n", i+1, name)
			} else {
				fmt.Printf("   Row %d: Failed to get name: %v\n", i+1, err)
			}
		}
	}

	// 2. Demonstrate getting integer values
	fmt.Println("\n2. Getting Integer Values (GetInt64):")
	query2 := "match $p isa person, has age $a; limit 2;"
	if result, err := database.ExecuteRead(queryCtx, query2); err == nil && result.IsRowStream {
		for i, row := range result.TypedRows {
			// Safe integer retrieval
			if age, err := row.GetInt64("a"); err == nil {
				fmt.Printf("   Row %d: age = %d (using GetInt64())\n", i+1, age)
			} else {
				// If age attribute doesn't exist
				fmt.Printf("   Row %d: No age attribute or type mismatch\n", i+1)
			}
		}
	}

	// 3. Demonstrate getting Concept objects
	fmt.Println("\n3. Getting Concept Objects (GetConcept):")
	query3 := "match $p isa person, has name $n; $p has age $a; limit 2;"
	if result, err := database.ExecuteRead(queryCtx, query3); err == nil && result.IsRowStream {
		for i, row := range result.TypedRows {
			// Safe concept retrieval
			if person, err := row.GetConcept("p"); err == nil {
				fmt.Printf("   Row %d: Person Concept\n", i+1)
				fmt.Printf("     - Type: %s\n", person.Type)
				fmt.Printf("     - IID: %s\n", person.IID)
				fmt.Printf("     - Is Entity: %v\n", person.Type == "entity")
			}
		}
	}

	// 4. Demonstrate getting count values
	fmt.Println("\n4. Getting Count Values (GetCount):")
	query4 := "match $p isa person; reduce $count = count($p);"
	if result, err := database.ExecuteRead(queryCtx, query4); err == nil && result.IsRowStream {
		if len(result.TypedRows) > 0 {
			// Safe count retrieval
			if count, err := result.TypedRows[0].GetCount(); err == nil {
				fmt.Printf("   Total persons: %d (using GetCount())\n", count)
			}
		}
	}

	// 5. Demonstrate generic value access
	fmt.Println("\n5. Generic Value Access (GetValue) - Returns TypedValue:")
	query5 := "match $p isa person, has name $n, has age $a; limit 1;"
	if result, err := database.ExecuteRead(queryCtx, query5); err == nil && result.IsRowStream {
		if len(result.TypedRows) > 0 {
			row := result.TypedRows[0]
			for _, colName := range result.ColumnNames {
				if val, err := row.GetValue(colName); err == nil {
					fmt.Printf("   Column '%s':\n", colName)
					fmt.Printf("     - Type(): %v\n", val.Type())
					fmt.Printf("     - IsNull(): %v\n", val.IsNull())
					fmt.Printf("     - String(): %s\n", val.String())

					// Demonstrate type checking and safe conversion
					switch val.Type() {
					case typedbclient.TypeString:
						if s, err := val.AsString(); err == nil {
							fmt.Printf("     - AsString(): \"%s\"\n", s)
						}
					case typedbclient.TypeInt64:
						if i, err := val.AsInt64(); err == nil {
							fmt.Printf("     - AsInt64(): %d\n", i)
						}
					case typedbclient.TypeConcept:
						if c, err := val.AsConcept(); err == nil {
							fmt.Printf("     - AsConcept(): IID=%s, Type=%s\n", c.IID, c.Type)
						}
					}
				}
			}
		}
	}

	// 6. Demonstrate handling NULL values
	fmt.Println("\n6. Handling NULL Values:")
	// Some persons might not have an age attribute
	query6 := "match $p isa person; limit 3;"
	if result, err := database.ExecuteRead(queryCtx, query6); err == nil && result.IsRowStream {
		for i, row := range result.TypedRows {
			// Try to get a value that might not exist
			val, err := row.GetValue("age")
			if err != nil {
				fmt.Printf("   Row %d: Column 'age' doesn't exist\n", i+1)
			} else if val.IsNull() {
				fmt.Printf("   Row %d: age is NULL\n", i+1)
			} else {
				fmt.Printf("   Row %d: age = %v\n", i+1, val)
			}
		}
	}

	// 7. Demonstrate document count (if DocumentStream)
	fmt.Println("\n7. Getting Document Count (GetDocumentCount):")
	fetchQuery := `match $p isa person, has name $n; fetch { "name": $n };`
	if result, err := database.ExecuteRead(queryCtx, fetchQuery); err == nil && result.IsDocumentStream {
		docCount := result.GetDocumentCount()
		fmt.Printf("   Document stream contains %d documents (using GetDocumentCount())\n", docCount)

		// Demonstrate getting a document
		if docCount > 0 {
			if doc, err := result.GetDocument(0); err == nil {
				fmt.Printf("   First document: %v (using GetDocument(0))\n", doc)
			}
		}

		// Get all documents
		allDocs := result.GetAllDocuments()
		fmt.Printf("   Got all documents: %d items (using GetAllDocuments())\n", len(allDocs))
	}

	fmt.Println("\n✅ Comprehensive demo completed - demonstrated all type-safe value access methods")
}

// demonstrateResultSerialization demonstrates result serialization and formatting
func demonstrateResultSerialization(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 6: Result Serialization and Formatting ---\n")

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
		attribute name value string;
		attribute companyname value string;
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

// formatTypedValue formats a TypedValue for display
func formatTypedValue(val *typedbclient.TypedValue) string {
	if val.IsNull() {
		return "null"
	}

	// Handle different value types
	switch val.Type() {
	case typedbclient.TypeString:
		if s, err := val.AsString(); err == nil {
			return fmt.Sprintf("\"%s\"", s)
		}
	case typedbclient.TypeBool:
		if b, err := val.AsBool(); err == nil {
			return fmt.Sprintf("%t", b)
		}
	case typedbclient.TypeInt64:
		if i, err := val.AsInt64(); err == nil {
			return fmt.Sprintf("%d", i)
		}
	case typedbclient.TypeFloat64:
		if f, err := val.AsFloat64(); err == nil {
			return fmt.Sprintf("%g", f)
		}
	case typedbclient.TypeDate:
		if d, err := val.AsDate(); err == nil {
			return fmt.Sprintf("Date(%d)", d.NumDaysSinceCE)
		}
	case typedbclient.TypeDateTime:
		if dt, err := val.AsDateTime(); err == nil {
			return dt.ToTime().Format(time.RFC3339)
		}
	case typedbclient.TypeDuration:
		if d, err := val.AsDuration(); err == nil {
			return fmt.Sprintf("%dM%dD%dns", d.Months, d.Days, d.Nanos)
		}
	case typedbclient.TypeDecimal:
		if d, err := val.AsDecimal(); err == nil {
			return fmt.Sprintf("%f", d.ToFloat64())
		}
	case typedbclient.TypeConcept:
		if c, err := val.AsConcept(); err == nil {
			if c.IID != "" {
				return fmt.Sprintf("%s[%s]", c.Type, c.IID)
			}
			return fmt.Sprintf("%s:%s", c.Type, c.Label)
		}
	case typedbclient.TypeConceptList, typedbclient.TypeValueList:
		if list, err := val.AsList(); err == nil {
			return fmt.Sprintf("[%d items]", len(list))
		}
	}

	return "<unknown>"
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

	// Use type-safe TypedRows API
	for _, typedRow := range result.TypedRows {
		for i, colName := range result.ColumnNames {
			if i < len(colWidths) {
				// Get value safely
				val, err := typedRow.GetValue(colName)
				var cellStr string
				if err != nil {
					cellStr = "<error>"
				} else {
					cellStr = formatTypedValue(val)
				}
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

	// Print data rows using type-safe API
	maxRows := 5
	for i, typedRow := range result.TypedRows {
		if i >= maxRows {
			fmt.Printf("... (%d more rows)\n", len(result.TypedRows)-maxRows)
			break
		}

		fmt.Print("|")
		for j, colName := range result.ColumnNames {
			if j < len(colWidths) {
				// Get value safely
				val, err := typedRow.GetValue(colName)
				var cellStr string
				if err != nil {
					cellStr = "<error>"
				} else {
					cellStr = formatTypedValue(val)
				}
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
		fmt.Printf("  - Row stream details: %d rows × %d columns\n", len(result.TypedRows), len(result.ColumnNames))
		if len(result.ColumnNames) > 0 {
			fmt.Printf("  - Column names: %v\n", result.ColumnNames)
		}
	}

	if result.IsDocumentStream {
		fmt.Printf("  - Document stream: results available\n")
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
		return fmt.Sprintf("RowStream (row stream, %d rows)", len(result.TypedRows))
	}
	if result.IsDocumentStream {
		return "DocumentStream (document stream results)"
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
