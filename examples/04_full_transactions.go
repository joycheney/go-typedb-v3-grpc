package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 4: Full Transaction API with Bundles
// This example demonstrates the full transaction management API using atomic bundles:
// - BeginTransaction: Start different types of transactions (Read/Write/Schema)
// - Transaction.ExecuteBundle: Execute atomic bundles of operations
// - Bundle operations: OpExecute, OpCommit, OpRollback, OpClose
// - Multi-query transaction management with atomic bundles
// - Bundle-based error handling and resource management

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

// demonstrateReadTransaction demonstrates read transaction
func demonstrateReadTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 1: Read Transaction (READ) ---\n")

	// Create context with timeout
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Begin read transaction
	tx, err := database.BeginTransaction(readCtx, typedbclient.Read)
	if err != nil {
		fmt.Printf("Failed to begin read transaction: %v\n", err)
		return
	}
	fmt.Println("✓ Read transaction started")

	// Create a bundle with multiple read queries
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: "match $p isa person; limit 3;"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; limit 3;"},
		{Type: typedbclient.OpClose}, // Read transactions just need close
	}

	fmt.Println("\nExecuting bundle with 2 queries...")
	results, err := tx.ExecuteBundle(readCtx, bundle)
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

	// Read transactions don't need to commit, only close
	fmt.Println("\nRead transaction characteristics:")
	fmt.Printf("  - Does not modify data, only reads data\n")
	fmt.Printf("  - Does not need to call Commit(), only Close()\n")
	fmt.Printf("  - Can execute multiple queries maintaining consistent read view\n")
}

// demonstrateSchemaTransaction demonstrates schema transaction
func demonstrateSchemaTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 2: Schema Transaction (SCHEMA) ---\n")

	// Create context with timeout
	schemaCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Begin schema transaction
	tx, err := database.BeginTransaction(schemaCtx, typedbclient.Schema)
	if err != nil {
		fmt.Printf("Failed to begin schema transaction: %v\n", err)
		return
	}

	fmt.Println("✓ Schema transaction started")

	// Define complete schema
	schemaQuery := `define
		entity person, owns name;
		entity company, owns companyname;
		attribute name value string;
		attribute companyname value string;`

	// Create bundle with schema definition and commit
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: schemaQuery},
		{Type: typedbclient.OpCommit}, // Schema transactions need commit
		{Type: typedbclient.OpClose},
	}

	fmt.Println("\nExecuting schema bundle...")
	results, err := tx.ExecuteBundle(schemaCtx, bundle)
	if err != nil {
		fmt.Printf("  Schema bundle failed: %v\n", err)
		// Bundle automatically handles rollback on error
		return
	}

	if len(results) > 0 && results[0] != nil {
		fmt.Printf("  ✓ Schema definition successful, query type: %s\n", getQueryTypeDescription(results[0].QueryType))
	}

	fmt.Println("\n✓ Schema transaction successfully committed")
	fmt.Println("Schema transaction features:")
	fmt.Printf("  - Used for defining or modifying database schema\n")
	fmt.Printf("  - Must call Commit() to persist changes\n")
	fmt.Printf("  - Should call Rollback() on failure to avoid partial changes\n")
	fmt.Printf("  - Can perform multiple schema operations in one transaction\n")
}

// demonstrateWriteTransaction demonstrates write transaction
func demonstrateWriteTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 3: Write Transaction (WRITE) ---\n")

	// Create context with timeout
	writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Begin write transaction
	tx, err := database.BeginTransaction(writeCtx, typedbclient.Write)
	if err != nil {
		fmt.Printf("Failed to begin write transaction: %v\n", err)
		return
	}

	fmt.Println("✓ Write transaction started")

	// Create bundle with insert and verification operations
	bundle := []typedbclient.BundleOperation{
		// Insert operations
		{Type: typedbclient.OpExecute, Query: `insert
		$p1 isa person, has name "Alice";
		$p2 isa person, has name "Bob";
		$c1 isa company, has companyname "TechCorp";`},
		// Verification query before commit
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
		// Commit the transaction
		{Type: typedbclient.OpCommit},
		// Close the transaction
		{Type: typedbclient.OpClose},
	}

	fmt.Println("\nExecuting write bundle with insert and verification...")
	results, err := tx.ExecuteBundle(writeCtx, bundle)
	if err != nil {
		fmt.Printf("  Write bundle failed: %v\n", err)
		// Bundle automatically handles rollback on error
		return
	}

	fmt.Printf("  ✓ Write bundle successful, executed %d operations\n", len(results))

	// Display results for each operation
	if len(results) > 0 && results[0] != nil {
		fmt.Printf("  Insert operation result: %s\n", getQueryTypeDescription(results[0].QueryType))
	}
	if len(results) > 1 && results[1] != nil {
		fmt.Printf("  Verification query result: %s\n", getResultTypeDescription(results[1]))
	}

	fmt.Println("\n✓ Write transaction successfully committed")
	fmt.Println("Write transaction features:")
	fmt.Printf("  - Used for inserting, updating, or deleting data\n")
	fmt.Printf("  - Bundle includes commit operation for persistence\n")
	fmt.Printf("  - Can include verification queries in same bundle\n")
	fmt.Printf("  - Atomic execution ensures all-or-nothing semantics\n")
}

// demonstrateTransactionRollback demonstrates transaction rollback
func demonstrateTransactionRollback(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 4: Transaction Rollback (ROLLBACK) ---\n")

	// Create context with timeout
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Begin write transaction
	tx, err := database.BeginTransaction(writeCtx, typedbclient.Write)
	if err != nil {
		fmt.Printf("Failed to begin write transaction: %v\n", err)
		return
	}

	fmt.Println("✓ Write transaction started (for rollback demonstration)")

	// Create bundle with insert followed by rollback (no commit)
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `insert $p isa person, has name "TestPerson";`},
		// Simulate business logic failure - rollback instead of commit
		{Type: typedbclient.OpRollback},
		{Type: typedbclient.OpClose},
	}

	fmt.Println("\nExecuting bundle with insert followed by rollback...")
	fmt.Println("Simulating business logic check failure...")

	results, err := tx.ExecuteBundle(writeCtx, bundle)
	if err != nil {
		fmt.Printf("  Bundle execution failed: %v\n", err)
		return
	}

	if len(results) > 0 && results[0] != nil {
		fmt.Printf("✓ Insert operation executed, query type: %s\n", getQueryTypeDescription(results[0].QueryType))
	}
	fmt.Println("✓ Transaction successfully rolled back")

	// Verify that data was not persisted
	fmt.Println("\nVerifying rollback effect:")
	verifyBundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `match $p isa person, has name "TestPerson";`},
		{Type: typedbclient.OpClose},
	}

	readTx, err := database.BeginTransaction(writeCtx, typedbclient.Read)
	if err != nil {
		fmt.Printf("Failed to begin verification transaction: %v\n", err)
		return
	}

	verifyResults, err := readTx.ExecuteBundle(writeCtx, verifyBundle)
	if err != nil {
		fmt.Printf("Verification bundle failed: %v\n", err)
	} else if len(verifyResults) > 0 && verifyResults[0] != nil {
		result := verifyResults[0]
		if result.IsRowStream && len(result.TypedRows) == 0 {
			fmt.Println("✓ Verification successful: data was not persisted after rollback")
		} else {
			fmt.Printf("✗ Verification failed: found %d rows of data\n", len(result.TypedRows))
		}
	}

	fmt.Println("\nTransaction rollback features:")
	fmt.Printf("  - OpRollback in bundle undoes all changes in the transaction\n")
	fmt.Printf("  - Database state is restored to before transaction began\n")
	fmt.Printf("  - Suitable for error handling and business logic validation\n")
	fmt.Printf("  - Bundle ensures atomic rollback operation\n")
}

// demonstrateBatchTransaction demonstrates batch operation transaction
func demonstrateBatchTransaction(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 5: Batch Operation Transaction ---\n")

	// Create context with timeout
	batchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// Begin write transaction for batch operations
	tx, err := database.BeginTransaction(batchCtx, typedbclient.Write)
	if err != nil {
		fmt.Printf("Failed to begin batch operation transaction: %v\n", err)
		return
	}

	fmt.Println("✓ Batch operation transaction started")

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

		// Perform data statistics before commit
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; reduce $count = count($c);"},
		// Commit all batch operations
		{Type: typedbclient.OpCommit},
		{Type: typedbclient.OpClose},
	}

	fmt.Println("\nExecuting batch bundle with multiple operations...")
	results, err := tx.ExecuteBundle(batchCtx, bundle)
	if err != nil {
		fmt.Printf("  Batch bundle failed: %v\n", err)
		// Bundle automatically handles rollback on error
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
	}
	if len(results) > 3 && results[3] != nil {
		fmt.Printf("  Company count statistics: %s\n", getResultTypeDescription(results[3]))
	}


	fmt.Println("\n✓ Batch operation transaction successfully committed")
	fmt.Println("Batch operation features:")
	fmt.Printf("  - Execute multiple related operations in a single atomic bundle\n")
	fmt.Printf("  - Guarantee atomicity of all operations (all succeed or all fail)\n")
	fmt.Printf("  - Suitable for complex data import and update scenarios\n")
	fmt.Printf("  - Can include validation and statistics in same bundle\n")
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
// 3. Observe complete lifecycle management of different transaction types
//
// APIs covered in this example:
// - database.BeginTransaction(ctx, txType)  // Begin transaction
// - transaction.Execute(ctx, query)        // Execute query
// - transaction.Commit(ctx)                // Commit transaction
// - transaction.Rollback(ctx)              // Rollback transaction
// - transaction.Close(ctx)                 // Close transaction
//
// Transaction types:
// - typedbclient.Read: Read-only transaction for querying data
// - typedbclient.Write: Write transaction for modifying data
// - typedbclient.Schema: Schema transaction for modifying schema
//
// Transaction management best practices:
// 1. Use defer to ensure transactions are closed
// 2. Write/Schema transactions must be explicitly committed to persist
// 3. Should call Rollback() on errors
// 4. Read transactions don't need commit, just Close()

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}