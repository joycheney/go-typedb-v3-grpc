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
// - Transaction.ExecuteBundle: Execute atomic bundles with automatic lifecycle management
//
// Automatic Lifecycle Management:
// 1. Normal flow: ExecuteBundle automatically adds required operations
//    - Write/Schema transactions: adds OpCommit and OpClose
//    - Read transactions: adds OpClose only
// 2. Error flow: ExecuteBundle automatically handles cleanup
//    - Write/Schema transactions: executes rollback and close on failure
//    - Read transactions: executes close on failure

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
	// ExecuteBundle will automatically add OpClose for read transactions
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: "match $p isa person; limit 3;"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; limit 3;"},
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

	// Read transactions don't need to commit
	fmt.Println("\nRead transaction characteristics:")
	fmt.Printf("  - Does not modify data, only reads data\n")
	fmt.Printf("  - Automatic lifecycle: ExecuteBundle adds OpClose\n")
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

	// Create bundle with schema definition
	// ExecuteBundle will automatically add OpCommit and OpClose for schema transactions
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: schemaQuery},
	}

	fmt.Println("\nExecuting schema bundle...")
	results, err := tx.ExecuteBundle(schemaCtx, bundle)
	if err != nil {
		fmt.Printf("  Schema bundle failed: %v\n", err)
		// On error: ExecuteBundle has already executed rollback and close
		return
	}

	if len(results) > 0 && results[0] != nil {
		fmt.Printf("  ✓ Schema definition successful, query type: %s\n", getQueryTypeDescription(results[0].QueryType))
	}

	fmt.Println("\n✓ Schema transaction successfully committed")
	fmt.Println("Schema transaction features:")
	fmt.Printf("  - Used for defining or modifying database schema\n")
	fmt.Printf("  - Automatic lifecycle: ExecuteBundle adds OpCommit and OpClose\n")
	fmt.Printf("  - Error handling: automatic rollback and close on failure\n")
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
	// ExecuteBundle will automatically add OpCommit and OpClose for write transactions
	bundle := []typedbclient.BundleOperation{
		// Insert operations
		{Type: typedbclient.OpExecute, Query: `insert
		$p1 isa person, has name "Alice";
		$p2 isa person, has name "Bob";
		$c1 isa company, has companyname "TechCorp";`},
		// Verification query (will run before auto-added commit)
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
	}

	fmt.Println("\nExecuting write bundle with insert and verification...")
	results, err := tx.ExecuteBundle(writeCtx, bundle)
	if err != nil {
		fmt.Printf("  Write bundle failed: %v\n", err)
		// On error: ExecuteBundle has already executed rollback and close
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
	fmt.Printf("  - Automatic lifecycle: ExecuteBundle adds OpCommit and OpClose\n")
	fmt.Printf("  - Verification queries run before auto-added commit\n")
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

	// Create bundle with insert followed by explicit rollback
	// This demonstrates business logic validation failure scenario
	// ExecuteBundle will automatically add OpClose after OpRollback
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `insert $p isa person, has name "TestPerson";`},
		{Type: typedbclient.OpRollback}, // Explicit rollback for business logic failure
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
	// Verification query in read transaction
	// ExecuteBundle will automatically add OpClose
	verifyBundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `match $p isa person, has name "TestPerson";`},
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
	fmt.Printf("  - Automatic lifecycle: ExecuteBundle adds OpClose after OpRollback\n")
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
	// ExecuteBundle will automatically add OpCommit and OpClose for atomicity
	bundle := []typedbclient.BundleOperation{
		// Batch insert multiple persons
		{Type: typedbclient.OpExecute, Query: batchInsert},
		// Batch insert company data
		{Type: typedbclient.OpExecute, Query: `
		insert
		$c1 isa company, has companyname "InnovateTech";
		$c2 isa company, has companyname "DataCorp";
		`},
		// Statistics queries (will run before auto-added commit)
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; reduce $count = count($c);"},
	}

	fmt.Println("\nExecuting batch bundle with multiple operations...")
	results, err := tx.ExecuteBundle(batchCtx, bundle)
	if err != nil {
		fmt.Printf("  Batch bundle failed: %v\n", err)
		// On error: ExecuteBundle has already executed rollback and close
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
	fmt.Printf("  - Automatic lifecycle: ExecuteBundle adds OpCommit and OpClose\n")
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
// - database.BeginTransaction(ctx, txType)    // Begin transaction
// - transaction.ExecuteBundle(ctx, operations) // Execute bundle with automatic lifecycle
//
// Automatic lifecycle management:
// - Normal flow: adds OpCommit (Write/Schema) and OpClose as needed
// - Error flow: executes rollback (Write/Schema) and close automatically
//
// Transaction types:
// - typedbclient.Read: Read-only transaction for querying data
// - typedbclient.Write: Write transaction for modifying data
// - typedbclient.Schema: Schema transaction for modifying schema
//
// Best practices:
// 1. Let ExecuteBundle handle transaction lifecycle - just provide OpExecute operations
// 2. Use explicit OpRollback only for business logic validation failures
// 3. Error handling is automatic - no need to manually rollback or close on errors
// 4. Bundle operations execute atomically - all succeed or all fail

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}