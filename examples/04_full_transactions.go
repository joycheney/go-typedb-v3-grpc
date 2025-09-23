package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 4: Full Transaction API
// This example demonstrates the full transaction management API of TypeDB v3 gRPC client, including:
// - BeginTransaction: Start different types of transactions (Read/Write/Schema)
// - Transaction.Execute: Execute queries within transactions
// - Transaction.Commit: Commit transactions
// - Transaction.Rollback: Rollback transactions
// - Transaction.Close: Close transactions
// - Multi-query transaction management and batch operations
// - Transaction error handling and resource management

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
	defer tx.Close(readCtx) // Ensure transaction is closed

	fmt.Println("✓ Read transaction started")

	// TypeDB 3.x compatible queries - removed get clauses and use business data queries
	queries := []string{
		"match $p isa person; limit 3;",
		"match $c isa company; limit 3;",
	}

	for i, query := range queries {
		fmt.Printf("\nExecuting query %d: %s\n", i+1, query)

		result, err := tx.Execute(readCtx, query)
		if err != nil {
			fmt.Printf("  Query failed: %v\n", err)
			continue
		}

		fmt.Printf("  ✓ Query successful, result type: %s\n", getResultTypeDescription(result))
		if result.IsRowStream && len(result.Rows) > 0 {
			fmt.Printf("  Found %d row results\n", len(result.Rows))
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
	// TypeDB 3.x schema definitions - using simplified syntax that works
	schemaQueries := []string{
		`define
		entity person, owns name;
		entity company, owns companyname;
		attribute name, value string;
		attribute companyname, value string;`,
	}

	// Execute all schema definitions in transaction
	for i, query := range schemaQueries {
		fmt.Printf("\nExecuting schema definition %d...\n", i+1)

		result, err := tx.Execute(schemaCtx, query)
		if err != nil {
			fmt.Printf("  Schema definition failed: %v\n", err)
			fmt.Println("  Rolling back...")
			tx.Rollback(schemaCtx)
			return
		}

		fmt.Printf("  ✓ Schema definition successful, query type: %s\n", getQueryTypeDescription(result.QueryType))
	}

	// Commit schema changes
	if err := tx.Commit(schemaCtx); err != nil {
		fmt.Printf("Failed to commit schema transaction: %v\n", err)
		return
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

	// Insert data
	insertQueries := []string{
		`insert
		$p1 isa person, has name "Alice";
		$p2 isa person, has name "Bob";
		$c1 isa company, has companyname "TechCorp";`,
	}

	// Execute all insert operations in transaction
	for i, query := range insertQueries {
		fmt.Printf("\nExecuting insert operation %d...\n", i+1)

		result, err := tx.Execute(writeCtx, query)
		if err != nil {
			fmt.Printf("  Insert operation failed: %v\n", err)
			fmt.Println("  Rolling back...")
			tx.Rollback(writeCtx)
			return
		}

		fmt.Printf("  ✓ Insert operation successful, query type: %s\n", getQueryTypeDescription(result.QueryType))
	}

	// Perform verification query before committing
	fmt.Printf("\nVerifying data before commit...\n")
	verifyQuery := "match $p isa person; count;"
	result, err := tx.Execute(writeCtx, verifyQuery)
	if err != nil {
		fmt.Printf("  Verification query failed: %v\n", err)
	} else {
		fmt.Printf("  ✓ Verification query successful, result type: %s\n", getResultTypeDescription(result))
	}

	// Commit write transaction
	if err := tx.Commit(writeCtx); err != nil {
		fmt.Printf("Failed to commit write transaction: %v\n", err)
		return
	}

	fmt.Println("\n✓ Write transaction successfully committed")
	fmt.Println("Write transaction features:")
	fmt.Printf("  - Used for inserting, updating, or deleting data\n")
	fmt.Printf("  - Must call Commit() to persist changes\n")
	fmt.Printf("  - Can perform verification queries before commit\n")
	fmt.Printf("  - Supports complex batch data operations\n")
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

	// Execute an insert operation
	insertQuery := `insert $p isa person, has name "TestPerson";`

	result, err := tx.Execute(writeCtx, insertQuery)
	if err != nil {
		fmt.Printf("Insert operation failed: %v\n", err)
		tx.Close(writeCtx)
		return
	}

	fmt.Printf("✓ Insert operation successful, query type: %s\n", getQueryTypeDescription(result.QueryType))

	// Simulate business logic decision to rollback
	fmt.Println("\nSimulating business logic check...")
	fmt.Println("Assuming business logic check failed, need to rollback transaction")

	// Execute rollback
	if err := tx.Rollback(writeCtx); err != nil {
		fmt.Printf("Failed to rollback transaction: %v\n", err)
		return
	}

	fmt.Println("✓ Transaction successfully rolled back")

	// Verify that data was not persisted
	fmt.Println("\nVerifying rollback effect:")
	readTx, err := database.BeginTransaction(writeCtx, typedbclient.Read)
	if err != nil {
		fmt.Printf("Failed to begin verification transaction: %v\n", err)
		return
	}
	defer readTx.Close(writeCtx)

	verifyQuery := `match $p isa person, has name "TestPerson";`
	result, err = readTx.Execute(writeCtx, verifyQuery)
	if err != nil {
		fmt.Printf("Verification query failed: %v\n", err)
	} else if result.IsRowStream && len(result.Rows) == 0 {
		fmt.Println("✓ Verification successful: data was not persisted after rollback")
	} else {
		fmt.Printf("✗ Verification failed: found %d rows of data\n", len(result.Rows))
	}

	fmt.Println("\nTransaction rollback features:")
	fmt.Printf("  - Rollback() undoes all changes in the transaction\n")
	fmt.Printf("  - Database state is restored to before transaction began\n")
	fmt.Printf("  - Suitable for error handling and business logic validation\n")
	fmt.Printf("  - Close() on Write/Schema transactions automatically rolls back\n")
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

	result, err := tx.Execute(batchCtx, batchInsert)
	if err != nil {
		fmt.Printf("Batch insert failed: %v\n", err)
		tx.Rollback(batchCtx)
		return
	}
	fmt.Printf("✓ Batch insert successful, query type: %s\n", getQueryTypeDescription(result.QueryType))

	// Batch insert company data
	fmt.Printf("\nExecuting batch company insert...\n")
	companyInsert := `
		insert
		$c1 isa company, has companyname "InnovateTech";
		$c2 isa company, has companyname "DataCorp";
	`
	result2, err := tx.Execute(batchCtx, companyInsert)
	if err != nil {
		fmt.Printf("Company insert failed: %v\n", err)
		tx.Rollback(batchCtx)
		return
	}
	fmt.Printf("✓ Company insert successful, query type: %s\n", getQueryTypeDescription(result2.QueryType))

	// Perform data statistics before batch operations
	fmt.Printf("\nStatistics before commit...\n")
	countQueries := map[string]string{
		"Total persons": "match $p isa person; count;",
		"Total companies": "match $c isa company; count;"
	}

	for desc, query := range countQueries {
		result, err := tx.Execute(batchCtx, query)
		if err != nil {
			fmt.Printf("Statistics %s failed: %v\n", desc, err)
		} else {
			fmt.Printf("✓ %s statistics successful, result type: %s\n", desc, getResultTypeDescription(result))
		}
	}

	// Commit batch operations
	if err := tx.Commit(batchCtx); err != nil {
		fmt.Printf("Failed to commit batch operations: %v\n", err)
		return
	}

	fmt.Println("\n✓ Batch operation transaction successfully committed")
	fmt.Println("Batch operation features:")
	fmt.Printf("  - Execute multiple related operations in a single transaction\n")
	fmt.Printf("  - Guarantee atomicity of all operations (all succeed or all fail)\n")
	fmt.Printf("  - Suitable for complex data import and update scenarios\n")
	fmt.Printf("  - Can perform validation and statistics before commit\n")
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
		return fmt.Sprintf("RowStream (%d rows, %d columns)", len(result.Rows), len(result.ColumnNames))
	}
	if result.IsDocumentStream {
		return fmt.Sprintf("DocumentStream (%d documents)", len(result.Documents))
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