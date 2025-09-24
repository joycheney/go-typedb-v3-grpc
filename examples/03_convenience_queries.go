package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 3: Convenience Query Interfaces
// This example demonstrates the convenience query interfaces of TypeDB v3 gRPC client, including:
// - ExecuteRead: Convenience interface for read queries (automatically manages read transactions)
// - ExecuteWrite: Convenience interface for write queries (automatically manages write transactions and commits)
// - ExecuteSchema: Convenience interface for schema queries (automatically manages schema transactions and commits)
// - Error handling and result parsing
// - Timeout management and resource cleanup

func main() {
	fmt.Println("=== TypeDB v3 gRPC Convenience Query Interfaces Example ===")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create test database
	testDbName := "convenience_queries_demo"
	ctx := context.Background()

	// Ensure test database exists
	if err := ensureTestDatabase(ctx, client, testDbName); err != nil {
		log.Fatalf("Failed to prepare test database: %v", err)
	}
	defer cleanupTestDatabase(ctx, client, testDbName)

	// Get database object
	database := client.GetDatabase(testDbName)

	// Demo 1: Convenience read query interface
	demonstrateConvenienceRead(ctx, database)

	// Demo 2: Convenience write query interface
	demonstrateConvenienceWrite(ctx, database)

	// Demo 3: Convenience schema query interface
	demonstrateConvenienceSchema(ctx, database)

	// Demo 4: Error handling and timeout management
	demonstrateErrorHandling(database)

	fmt.Println("=== Convenience Query Interfaces Example Completed ===")
}

// demonstrateConvenienceRead demonstrates convenience read query interface
func demonstrateConvenienceRead(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 1: Convenience Read Query Interface (ExecuteRead) ---\n")

	// Create context with timeout
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Example query 1: Define and query person entity
	fmt.Println("1. First define schema, then query person instances:")
	// First define schema
	schemaQuery := `
		define
		entity person, owns name;
		attribute name, value string;
	`
	_, err := database.ExecuteSchema(readCtx, schemaQuery)
	if err != nil {
		fmt.Printf("   Failed to define schema: %v\n", err)
	} else {
		fmt.Printf("   ✓ Schema definition successful\n")
	}

	// then query person instances (should be empty)
	query1 := "match $x isa person;"
	result1, err := database.ExecuteRead(readCtx, query1)
	if err != nil {
		fmt.Printf("   Query failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ Query successful, result type: %v\n", getResultTypeDescription(result1))
		fmt.Printf("   query type: %v\n", getQueryTypeDescription(result1.QueryType))
		if result1.IsRowStream && len(result1.Rows) > 0 {
			fmt.Printf("   Found %d results\n", len(result1.Rows))
			// Show first few results
			maxShow := 3
			for i := 0; i < len(result1.Rows) && i < maxShow; i++ {
				fmt.Printf("     Result %d: %v\n", i+1, result1.Rows[i])
			}
			if len(result1.Rows) > maxShow {
				fmt.Printf("     ... (%d more results)\n", len(result1.Rows)-maxShow)
			}
		} else {
			fmt.Printf("   No matching results found\n")
		}
	}

	// Example query2: Insert data and query
	fmt.Println("\n2. Insert person data and query:")
	// Insert a person instance
	insertQuery := `
		insert
		$p isa person, has name "Alice";
	`
	_, err = database.ExecuteWrite(readCtx, insertQuery)
	if err != nil {
		fmt.Printf("   Insert failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ Insert successful\n")
	}

	// Query all persons
	query2 := "match $x isa person;"
	result2, err := database.ExecuteRead(readCtx, query2)
	if err != nil {
		fmt.Printf("   Query failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ Query successful, result type: %v\n", getResultTypeDescription(result2))
		if result2.IsRowStream && len(result2.Rows) > 0 {
			fmt.Printf("   Found %d person instances\n", len(result2.Rows))
		} else {
			fmt.Printf("   No person instances found\n")
		}
	}

	// Example query3: Query person with attributes
	fmt.Println("\n3. Query person's name attributes:")
	query3 := "match $p isa person, has name $n;"
	result3, err := database.ExecuteRead(readCtx, query3)
	if err != nil {
		fmt.Printf("   Query failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ Query successful, result type: %v\n", getResultTypeDescription(result3))
		if result3.IsRowStream && len(result3.Rows) > 0 {
			fmt.Printf("   Found %d attribute instances\n", len(result3.Rows))
		} else {
			fmt.Printf("   No attribute instances found\n")
		}
	}
}

// demonstrateConvenienceWrite demonstrates convenience write query interface
func demonstrateConvenienceWrite(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 2: Convenience Write Query Interface (ExecuteWrite) ---\n")

	// Create context with timeout
	writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// First try to define some basic schema (if not already defined)
	fmt.Println("1. Attempt to insert test data:")

	// Example: Insert a simple entity instance (requires schema to exist first)
	insertQuery := `
		insert $p isa person;
	`

	result, err := database.ExecuteWrite(writeCtx, insertQuery)
	if err != nil {
		fmt.Printf("   Insert failed (may need to define schema first): %v\n", err)

		// If Insert failed, it may need to define schema first
		fmt.Println("   Note: Write operations require corresponding schema to exist")
	} else {
		fmt.Printf("   ✓ Insert successful, result type: %v\n", getResultTypeDescription(result))
		if result.IsDone {
			fmt.Printf("   Operation completed, query type: %v\n", getQueryTypeDescription(result.QueryType))
		}
	}

	// Example 2: Attempt update operations
	fmt.Println("\n2. Demonstrate write query features:")
	fmt.Printf("   - ExecuteWrite automatically creates write transactions\n")
	fmt.Printf("   - Automatically commits transactions after successful execution\n")
	fmt.Printf("   - Automatically rolls back transactions on errors\n")
	fmt.Printf("   - No need for manual transaction lifecycle management\n")
}

// demonstrateConvenienceSchema demonstrates convenience schema query interface
func demonstrateConvenienceSchema(ctx context.Context, database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 3: Convenience Schema Query Interface (ExecuteSchema) ---\n")

	// Create context with timeout
	schemaCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Example 1: Define entity types
	fmt.Println("1. Define basic entity types:")
	defineEntityQuery := `
		define
		entity employee, owns emp_name, owns emp_age;
		attribute emp_name, value string;
		attribute emp_age, value long;
	`

	result1, err := database.ExecuteSchema(schemaCtx, defineEntityQuery)
	if err != nil {
		fmt.Printf("   Failed to define entity types: %v\n", err)
	} else {
		fmt.Printf("   ✓ Successfully defined entity types\n")
		fmt.Printf("   result type: %v\n", getResultTypeDescription(result1))
		fmt.Printf("   query type: %v\n", getQueryTypeDescription(result1.QueryType))
	}

	// Example 2: Define relation types
	fmt.Println("\n2. Define relation types:")
	defineRelationQuery := `
		define
		relation friendship, relates friend;
		person plays friendship:friend;
	`

	result2, err := database.ExecuteSchema(schemaCtx, defineRelationQuery)
	if err != nil {
		fmt.Printf("   Failed to define relation types: %v\n", err)
	} else {
		fmt.Printf("   ✓ Successfully defined relation types\n")
		fmt.Printf("   result type: %v\n", getResultTypeDescription(result2))
	}

	// Example 3: Demonstrate schema query features
	fmt.Println("\n3. Schema query features description:")
	fmt.Printf("   - ExecuteSchema automatically creates schema transactions\n")
	fmt.Printf("   - Supports defining and deleting schema elements\n")
	fmt.Printf("   - Automatically commits changes after successful execution\n")
	fmt.Printf("   - Schema changes affect the entire database structure\n")
}

// demonstrateErrorHandling demonstrates error handling and timeout management
func demonstrateErrorHandling(database *typedbclient.Database) {
	fmt.Printf("\n--- Demo 4: Error Handling and Timeout Management ---\n")

	// Example 1: Timeout handling
	fmt.Println("1. Timeout handling demonstration:")
	shortCtx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, err := database.ExecuteRead(shortCtx, "match $x sub entity; get;")
	if err != nil {
		fmt.Printf("   ✓ Timeout error correctly handled: %v\n", err)
	}

	// Example 2: Syntax error handling
	fmt.Println("\n2. Syntax error handling demonstration:")
	normalCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	_, err = database.ExecuteRead(normalCtx, "invalid query syntax")
	if err != nil {
		fmt.Printf("   ✓ Syntax error correctly handled: %v\n", err)
	}

	// Example 3: Normal query comparison
	fmt.Println("\n3. Normal query comparison:")
	_, err = database.ExecuteRead(normalCtx, "match $x isa person; limit 1;")
	if err != nil {
		fmt.Printf("   Query execution failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ Normal query executed successfully\n")
	}

	fmt.Println("\n4. Advantages of convenience interfaces:")
	fmt.Printf("   - Automatic transaction management, no need for manual open/close\n")
	fmt.Printf("   - Automatic error handling and resource cleanup\n")
	fmt.Printf("   - Unified retry mechanism and authentication handling\n")
	fmt.Printf("   - Simplified API, reduces boilerplate code\n")
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
		return "READ (read query)"
	case typedbclient.QueryTypeWrite:
		return "WRITE (write query)"
	case typedbclient.QueryTypeSchema:
		return "SCHEMA (schema query)"
	default:
		return "UNKNOWN (unknown query type)"
	}
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running
// 2. Run this example: go run examples/03_convenience_queries.go
// 3. Observe automatic transaction management for different query types
//
// APIs covered in this example:
// - database.ExecuteRead(ctx, query)     // Convenience read query
// - database.ExecuteWrite(ctx, query)    // Convenience write query
// - database.ExecuteSchema(ctx, query)   // Convenience schema query
//
// Advantages of convenience interfaces:
// 1. Automatic transaction management: No need to manually create, commit or close transactions
// 2. Error handling: Automatic handling of authentication and connection errors
// 3. Resource cleanup: Ensures transaction resources are properly released
// 4. Retry mechanism: Built-in retry and reconnection logic
// 5. Simplified API: Reduces boilerplate code and improves development efficiency

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
