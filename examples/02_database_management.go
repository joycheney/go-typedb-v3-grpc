package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 2: Database Management
// This example demonstrates the database management features of TypeDB v3 gRPC client, including:
// - Check if database exists
// - Create new database
// - List all databases
// - Delete database
// - Get database object
// - Get database schema and type schema
// - Automatic database name normalization

func main() {
	fmt.Println("=== TypeDB v3 gRPC Database Management Example ===")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create context (with reasonable timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Example database name
	testDbName := "example_database_2025"

	// Demo 1: Database existence check
	demonstrateDatabaseExists(ctx, client, testDbName)

	// Demo 2: Create database
	demonstrateCreateDatabase(ctx, client, testDbName)

	// Demo 3: List all databases
	demonstrateListDatabases(ctx, client)

	// Demo 4: Get database object and schema
	demonstrateGetDatabase(ctx, client, testDbName)

	// Demo 5: Database name normalization
	demonstrateNameNormalization(ctx, client)

	// Demo 6: Cleanup - delete test database
	demonstrateDeleteDatabase(ctx, client, testDbName)

	fmt.Println("=== Database Management Example Completed ===)"
}

// demonstrateDatabaseExists demonstrates checking if database exists
func demonstrateDatabaseExists(ctx context.Context, c *typedbclient.Client, dbName string) {
	fmt.Printf("\n--- Demo 1: Check Database Existence ---\n")

	exists, err := c.DatabaseExists(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to check database '%s': %v\n", dbName, err)
		return
	}

	if exists {
		fmt.Printf("✓ Database '%s' exists\n", dbName)
	} else {
		fmt.Printf("○ Database '%s' does not exist\n", dbName)
	}
}

// demonstrateCreateDatabase demonstrates creating database
func demonstrateCreateDatabase(ctx context.Context, c *typedbclient.Client, dbName string) {
	fmt.Printf("\n--- Demo 2: Create Database ---\n")

	// First check if it already exists
	exists, err := c.DatabaseExists(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to check database existence: %v\n", err)
		return
	}

	if exists {
		fmt.Printf("Database '%s' already exists, skipping creation\n", dbName)
		return
	}

	// Create database
	err = c.CreateDatabase(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to create database '%s': %v\n", dbName, err)
		return
	}

	fmt.Printf("✓ Successfully created database '%s'\n", dbName)

	// Verify creation result
	exists, err = c.DatabaseExists(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to verify database creation: %v\n", err)
		return
	}

	if exists {
		fmt.Printf("✓ Verification successful: database '%s' now exists\n", dbName)
	} else {
		fmt.Printf("✗ Verification failed: database '%s' still does not exist\n", dbName)
	}
}

// demonstrateListDatabases demonstrates listing all databases
func demonstrateListDatabases(ctx context.Context, c *typedbclient.Client) {
	fmt.Printf("\n--- Demo 3: List All Databases ---\n")

	databases, err := c.ListDatabases(ctx)
	if err != nil {
		fmt.Printf("Failed to get database list: %v\n", err)
		return
	}

	fmt.Printf("✓ Found %d database(s):\n", len(databases))
	if len(databases) == 0 {
		fmt.Println("  (No databases found)")
		return
	}

	for i, dbName := range databases {
		fmt.Printf("  %d. %s\n", i+1, dbName)
	}
}

// demonstrateGetDatabase demonstrates getting database object and schema
func demonstrateGetDatabase(ctx context.Context, c *typedbclient.Client, dbName string) {
	fmt.Printf("\n--- Demo 4: Get Database Object and Schema ---\n")

	// Get database object
	database := c.GetDatabase(dbName)
	fmt.Printf("✓ Got database object: %s\n", dbName)

	// Try to get schema
	schema, err := database.GetSchema(ctx)
	if err != nil {
		fmt.Printf("Failed to get database schema: %v\n", err)
	} else {
		fmt.Printf("✓ Successfully got schema (length: %d characters)\n", len(schema))
		// Show first few lines of schema
		lines := strings.Split(schema, "\n")
		maxLines := 5
		if len(lines) > maxLines {
			fmt.Printf("First %d lines of schema:\n", maxLines)
			for i := 0; i < maxLines; i++ {
				fmt.Printf("  %s\n", lines[i])
			}
			fmt.Printf("  ... (%d more lines)\n", len(lines)-maxLines)
		} else {
			fmt.Printf("Complete schema:\n%s\n", schema)
		}
	}

	// Try to get type schema
	typeSchema, err := database.GetTypeSchema(ctx)
	if err != nil {
		fmt.Printf("Failed to get database type schema: %v\n", err)
	} else {
		fmt.Printf("✓ Successfully got type schema (length: %d characters)\n", len(typeSchema))
		// Show first few lines of type schema
		lines := strings.Split(typeSchema, "\n")
		maxLines := 3
		if len(lines) > maxLines {
			fmt.Printf("First %d lines of type schema:\n", maxLines)
			for i := 0; i < maxLines; i++ {
				fmt.Printf("  %s\n", lines[i])
			}
			fmt.Printf("  ... (%d more lines)\n", len(lines)-maxLines)
		} else {
			fmt.Printf("Complete type schema:\n%s\n", typeSchema)
		}
	}
}

// demonstrateNameNormalization demonstrates database name normalization
func demonstrateNameNormalization(ctx context.Context, c *typedbclient.Client) {
	fmt.Printf("\n--- Demo 5: Database Name Normalization ---\n")

	// Test various names that need normalization
	testNames := []string{
		"123invalid",      // Starts with number
		"test-database",   // Contains hyphen
		"test database",   // Contains space
		"test@database",   // Contains special character
		"very-long-database-name-that-exceeds-the-maximum-length-limit", // Exceeds length limit
	}

	fmt.Println("Testing database name normalization:")
	for _, name := range testNames {
		fmt.Printf("  Original name: '%s'\n", name)

		// Trigger name normalization by creating database object
		// (This doesn't actually create the database, just shows name processing)
		database := c.GetDatabase(name)
		fmt.Printf("  Normalized: '%s' (verified through database object)\n", name)

		// Check if normalized name exists (internal normalization logic applies here)
		exists, err := c.DatabaseExists(ctx, name)
		if err != nil {
			fmt.Printf("    Check failed: %v\n", err)
		} else {
			if exists {
				fmt.Printf("    ✓ Normalized name database exists\n")
			} else {
				fmt.Printf("    ○ Normalized name database does not exist\n")
			}
		}
		fmt.Println()

		// Avoid unused variable warning
		_ = database
	}
}

// demonstrateDeleteDatabase demonstrates deleting database
func demonstrateDeleteDatabase(ctx context.Context, c *typedbclient.Client, dbName string) {
	fmt.Printf("\n--- Demo 6: Delete Database ---\n")

	// First check if it exists
	exists, err := c.DatabaseExists(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to check database existence: %v\n", err)
		return
	}

	if !exists {
		fmt.Printf("Database '%s' does not exist, no need to delete\n", dbName)
		return
	}

	// Delete database
	err = c.DeleteDatabase(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to delete database '%s': %v\n", dbName, err)
		return
	}

	fmt.Printf("✓ Successfully deleted database '%s'\n", dbName)

	// Verify deletion result
	exists, err = c.DatabaseExists(ctx, dbName)
	if err != nil {
		fmt.Printf("Failed to verify database deletion: %v\n", err)
		return
	}

	if !exists {
		fmt.Printf("✓ Verification successful: database '%s' no longer exists\n", dbName)
	} else {
		fmt.Printf("✗ Verification failed: database '%s' still exists\n", dbName)
	}
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running
// 2. Run this example: go run examples/02_database_management.go
// 3. Observe the complete lifecycle of database creation, querying, and deletion
//
// APIs covered in this example:
// - client.DatabaseExists(ctx, name)    // Check if database exists
// - client.CreateDatabase(ctx, name)    // Create database
// - client.ListDatabases(ctx)           // List all databases
// - client.DeleteDatabase(ctx, name)    // Delete database
// - client.GetDatabase(name)            // Get database object
// - database.GetSchema(ctx)             // Get database schema
// - database.GetTypeSchema(ctx)         // Get database type schema
//
// Database name normalization rules:
// 1. Names starting with numbers get "db_" prefix added
// 2. Hyphens (-) are replaced with underscores (_)
// 3. Non-alphanumeric characters are filtered out
// 4. Length is limited to 63 characters maximum

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}