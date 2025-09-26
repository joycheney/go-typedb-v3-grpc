package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 4: Full Transaction API with Automatic Management
// This example demonstrates the full transaction management API with automatic lifecycle:
// - Single queries: ExecuteRead, ExecuteWrite, ExecuteSchema
// - Bundle operations: Database.ExecuteBundle for atomic multi-operation transactions
// - All transaction lifecycle (open, commit, close, rollback) is handled automatically

func main() {
	fmt.Println("=== TypeDB v3 gRPC Full Transaction API Example ===")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create test database
	testDbName := "full_transaction_demo"
	ctx := context.Background()

	// Ensure test database exists
	if err := ensureTestDatabase(ctx, client, testDbName); err != nil {
		log.Fatalf("Failed to prepare test database: %v", err)
	}
	defer cleanupTestDatabase(ctx, client, testDbName)

	// Get database object
	database := client.GetDatabase(testDbName)

	// Demo 1: Read transaction
	demonstrateReadTransaction(ctx, database)

	// Demo 2: Schema transaction
	demonstrateSchemaTransaction(ctx, database)

	// Demo 3: Write transaction
	demonstrateWriteTransaction(ctx, database)

	// Demo 4: Transaction rollback
	demonstrateTransactionRollback(ctx, database)

	// Demo 5: Batch operation transaction
	demonstrateBatchTransaction(ctx, database)

	fmt.Println("=== Full Transaction API Example Completed ===")
}

// demonstrateReadTransaction demonstrates read transaction with automatic lifecycle
func demonstrateReadTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 1: Read Transaction (READ) ---\n")

	// Create context with timeout
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	fmt.Println("✓ Using automatic read transaction management")

	// Execute multiple read queries using bundle - automatic transaction lifecycle
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: "match $p isa person; limit 3;"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; limit 3;"},
		// No need to add OpClose - it's automatic
	}

	fmt.Println("\nExecuting bundle with 2 queries...")
	results, err := database.ExecuteBundle(readCtx, typedbclient.Read, bundle)
	if err != nil {
		fmt.Printf("  Bundle execution failed: %v\n", err)
		return
	}

	fmt.Printf("  ✓ Bundle successful, executed %d operations\n", len(results))
	for i, result := range results {
		if result != nil {
			fmt.Printf("  Query %d result type: %s\n", i+1, getResultTypeDescription(result))
			if result.IsRowStream && len(result.TypedRows) > 0 {
				fmt.Printf("    Found %d row results\n", len(result.TypedRows))
			}
		}
	}

	// Read transactions don't need to commit, only close (automatic)
	fmt.Println("\nRead transaction characteristics:")
	fmt.Printf("  - Does not modify data, only reads data\n")
	fmt.Printf("  - Automatically handles transaction lifecycle\n")
	fmt.Printf("  - No manual Close() needed - handled automatically\n")
	fmt.Printf("  - Can execute multiple queries maintaining consistent read view\n")
}

// demonstrateSchemaTransaction demonstrates schema transaction with automatic lifecycle
func demonstrateSchemaTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 2: Schema Transaction (SCHEMA) ---\n")

	// Create context with timeout
	schemaCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	fmt.Println("✓ Using automatic schema transaction management")

	// Define complete schema
	schemaQuery := `define
		entity person, owns name;
		entity company, owns companyname;
		attribute name value string;
		attribute companyname value string;`

	// Method 1: Single schema query (simplest)
	fmt.Println("\nExecuting single schema query...")
	_, err := database.ExecuteSchema(schemaCtx, schemaQuery)
	if err != nil {
		fmt.Printf("  Schema definition failed: %v\n", err)
		return
	}
	fmt.Println("  ✓ Schema definition successful (automatic commit & close)")

	// Method 2: Multiple schema operations in bundle
	extendSchema := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `define
			entity department, owns deptname;
			attribute deptname value string;`},
		{Type: typedbclient.OpExecute, Query: `define
			relation employment, relates employee, relates employer;
			person plays employment:employee;
			company plays employment:employer;`},
		// No need to add OpCommit or OpClose - they're automatic
	}

	fmt.Println("\nExecuting schema bundle for extensions...")
	results, err := database.ExecuteBundle(schemaCtx, typedbclient.Schema, extendSchema)
	if err != nil {
		fmt.Printf("  Schema bundle failed: %v\n", err)
		return
	}

	fmt.Printf("  ✓ Schema extension successful, executed %d operations\n", len(results))

	fmt.Println("\n✓ Schema transaction successfully committed")
	fmt.Println("Schema transaction features:")
	fmt.Printf("  - Used for defining or modifying database schema\n")
	fmt.Printf("  - Automatic commit and close on success\n")
	fmt.Printf("  - Automatic rollback and close on error\n")
	fmt.Printf("  - Can perform multiple schema operations in one transaction\n")
}

// demonstrateWriteTransaction demonstrates write transaction with automatic lifecycle
func demonstrateWriteTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 3: Write Transaction (WRITE) ---\n")

	// Create context with timeout
	writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	fmt.Println("✓ Using automatic write transaction management")

	// Create bundle with insert and verification operations
	bundle := []typedbclient.BundleOperation{
		// Insert operations
		{Type: typedbclient.OpExecute, Query: `insert
		$p1 isa person, has name "Alice";
		$p2 isa person, has name "Bob";
		$c1 isa company, has companyname "TechCorp";`},
		// Verification query before automatic commit
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
		// No need to add OpCommit or OpClose - they're automatic
	}

	fmt.Println("\nExecuting write bundle with insert and verification...")
	results, err := database.ExecuteBundle(writeCtx, typedbclient.Write, bundle)
	if err != nil {
		fmt.Printf("  Write bundle failed: %v\n", err)
		// Transaction automatically rolled back and closed on error
		return
	}

	fmt.Printf("  ✓ Write bundle successful, executed %d operations\n", len(results))

	// Display results for each operation
	if len(results) > 0 && results[0] != nil {
		fmt.Printf("  Insert operation result: %s\n", getQueryTypeDescription(results[0].QueryType))
	}
	if len(results) > 1 && results[1] != nil {
		fmt.Printf("  Verification query result: %s\n", getResultTypeDescription(results[1]))
		if results[1].IsRowStream && len(results[1].TypedRows) > 0 {
			count, _ := results[1].TypedRows[0].GetCount()
			fmt.Printf("    Person count: %v\n", count)
		}
	}

	fmt.Println("\n✓ Write transaction automatically committed")
	fmt.Println("Write transaction features:")
	fmt.Printf("  - Used for inserting, updating, or deleting data\n")
	fmt.Printf("  - Automatic commit on success\n")
	fmt.Printf("  - Automatic rollback on error\n")
	fmt.Printf("  - Atomic execution ensures all-or-nothing semantics\n")
}

// demonstrateTransactionRollback demonstrates automatic rollback on error
func demonstrateTransactionRollback(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 4: Automatic Transaction Rollback ---\n")

	// Create context with timeout
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	fmt.Println("✓ Demonstrating automatic rollback on error")

	// Insert some test data first
	_, err := database.ExecuteWrite(writeCtx, `insert $p isa person, has name "TestPerson";`)
	if err != nil {
		fmt.Printf("Failed to insert test data: %v\n", err)
	} else {
		fmt.Println("✓ Inserted test person")
	}

	// Now try an invalid operation that will trigger automatic rollback
	fmt.Println("\nExecuting invalid operation to trigger rollback...")

	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `insert $p isa person, has name "Person2";`},
		// This will fail because 'invalid_type' doesn't exist
		{Type: typedbclient.OpExecute, Query: `insert $x isa invalid_type;`},
	}

	_, err = database.ExecuteBundle(writeCtx, typedbclient.Write, bundle)
	if err != nil {
		fmt.Printf("✓ Bundle failed as expected: %v\n", err)
		fmt.Println("✓ Transaction automatically rolled back")
		fmt.Println("✓ No partial data persisted")
	} else {
		fmt.Println("✗ Unexpected: invalid operation succeeded")
	}

	// Verify that Person2 was not persisted due to rollback
	fmt.Println("\nVerifying rollback effect...")
	result, err := database.ExecuteRead(writeCtx, `match $p isa person, has name "Person2";`)
	if err != nil {
		fmt.Printf("Verification query failed: %v\n", err)
	} else if result.IsRowStream && len(result.TypedRows) == 0 {
		fmt.Println("✓ Verification successful: Person2 was not persisted (rolled back)")
	} else {
		fmt.Printf("✗ Verification failed: found %d rows (should be 0)\n", len(result.TypedRows))
	}

	fmt.Println("\nAutomatic rollback features:")
	fmt.Printf("  - Errors trigger automatic rollback\n")
	fmt.Printf("  - Database state is restored automatically\n")
	fmt.Printf("  - No manual rollback needed\n")
	fmt.Printf("  - Ensures data consistency on errors\n")
}

// demonstrateBatchTransaction demonstrates batch operations with automatic transaction
func demonstrateBatchTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 5: Batch Operation Transaction ---\n")

	// Create context with timeout
	batchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	fmt.Println("✓ Using automatic batch transaction management")

	// Batch insert multiple persons
	fmt.Printf("\nExecuting batch person insert...\n")
	batchInsert := `
		insert
		$p1 isa person, has name "John";
		$p2 isa person, has name "Jane";
		$p3 isa person, has name "Mike";
		$p4 isa person, has name "Sarah";
	`

	// Create bundle with multiple batch operations
	bundle := []typedbclient.BundleOperation{
		// Batch insert multiple persons
		{Type: typedbclient.OpExecute, Query: batchInsert},
		// Batch insert company data
		{Type: typedbclient.OpExecute, Query: `
		insert
		$c1 isa company, has companyname "InnovateTech";
		$c2 isa company, has companyname "DataCorp";
		`},
		// Perform data statistics before automatic commit
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; reduce $count = count($c);"},
		// No need to add OpCommit or OpClose - they're automatic
	}

	fmt.Println("\nExecuting batch bundle with multiple operations...")
	results, err := database.ExecuteBundle(batchCtx, typedbclient.Write, bundle)
	if err != nil {
		fmt.Printf("  Batch bundle failed: %v\n", err)
		// Transaction automatically rolled back and closed on error
		return
	}

	fmt.Printf("  ✓ Batch bundle successful, executed %d operations\n", len(results))

	// Display results for each operation
	if len(results) > 0 && results[0] != nil {
		fmt.Printf("  Batch person insert: %s\n", getQueryTypeDescription(results[0].QueryType))
	}
	if len(results) > 1 && results[1] != nil {
		fmt.Printf("  Batch company insert: %s\n", getQueryTypeDescription(results[1].QueryType))
	}
	if len(results) > 2 && results[2] != nil {
		fmt.Printf("  Person count statistics: %s\n", getResultTypeDescription(results[2]))
		if results[2].IsRowStream && len(results[2].TypedRows) > 0 {
			count, _ := results[2].TypedRows[0].GetCount()
			fmt.Printf("    Total persons: %v\n", count)
		}
	}
	if len(results) > 3 && results[3] != nil {
		fmt.Printf("  Company count statistics: %s\n", getResultTypeDescription(results[3]))
		if results[3].IsRowStream && len(results[3].TypedRows) > 0 {
			count, _ := results[3].TypedRows[0].GetCount()
			fmt.Printf("    Total companies: %v\n", count)
		}
	}

	fmt.Println("\n✓ Batch operation transaction automatically committed")
	fmt.Println("Batch operation features:")
	fmt.Printf("  - Execute multiple related operations atomically\n")
	fmt.Printf("  - Automatic transaction management\n")
	fmt.Printf("  - All operations succeed or all fail together\n")
	fmt.Printf("  - Suitable for complex data import scenarios\n")
}

// Helper functions

// ensureTestDatabase ensures test database exists
func ensureTestDatabase(ctx context.Context, client *typedbclient.Client, dbName string) error {
	exists, err := client.DatabaseExists(ctx, dbName)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %w", err)
	}

	if !exists {
		if err := client.CreateDatabase(ctx, dbName); err != nil {
			return fmt.Errorf("failed to create test database: %w", err)
		}
		fmt.Printf("✓ Created test database: %s\n", dbName)
	}

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

// getResultTypeDescription gets the description of result type
func getResultTypeDescription(result *typedbclient.QueryResult) string {
	if result.IsDone {
		return "Done (operation completed)"
	}
	if result.IsRowStream {
		return fmt.Sprintf("RowStream (%d rows, %d columns)", len(result.TypedRows), len(result.ColumnNames))
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
// 2. Run this example: go run examples/04_full_transactions.go
// 3. Observe automatic lifecycle management of different transaction types
//
// APIs covered in this example:
// - database.ExecuteRead(ctx, query)       // Single read query with automatic transaction
// - database.ExecuteWrite(ctx, query)      // Single write query with automatic transaction
// - database.ExecuteSchema(ctx, query)     // Single schema query with automatic transaction
// - database.ExecuteBundle(ctx, txType, bundle) // Multiple operations with automatic transaction
//
// Transaction types:
// - typedbclient.Read: Read-only transaction for querying data
// - typedbclient.Write: Write transaction for modifying data
// - typedbclient.Schema: Schema transaction for modifying schema
//
// Transaction management features:
// 1. Automatic transaction lifecycle - no manual open/close needed
// 2. Automatic commit for Write/Schema transactions
// 3. Automatic rollback on errors
// 4. Thread-safe execution through single worker pattern

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}