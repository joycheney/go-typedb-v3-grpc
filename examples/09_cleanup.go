package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 8: Database Cleanup Utility
// This example provides a utility to clean up test databases created by the other examples.
// It demonstrates:
// - Listing all databases
// - Identifying test databases by naming patterns
// - Safely deleting test databases
// - Preserving production databases

func main() {
	fmt.Println("=== TypeDB v3 gRPC Database Cleanup Utility ===")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// List all databases
	fmt.Println("\n🔍 Scanning for databases...")
	databases, err := client.ListDatabases(ctx)
	if err != nil {
		log.Fatalf("Failed to list databases: %v", err)
	}

	fmt.Printf("Found %d database(s):\n", len(databases))
	for _, db := range databases {
		fmt.Printf("  - %s\n", db)
	}

	// Identify test databases (based on naming patterns from examples)
	testDatabasePatterns := []string{
		"example_",          // From example 01, 02
		"simple_query_demo", // From example 03
		"full_transaction_", // From example 04
		"query_results_",    // From example 05
		"error_handling_",   // From example 06
		"comprehensive_",    // From example 07
		"test_",            // General test databases
		"_demo",            // Demo databases
		"_test",            // Test suffix
		"temp_",            // Temporary databases
		"tmp_",             // Temporary databases
	}

	// Find test databases to clean up
	testDatabases := []string{}
	for _, db := range databases {
		isTestDb := false
		dbLower := strings.ToLower(db)

		// Check if database matches any test pattern
		for _, pattern := range testDatabasePatterns {
			if strings.Contains(dbLower, pattern) {
				isTestDb = true
				break
			}
		}

		if isTestDb {
			testDatabases = append(testDatabases, db)
		}
	}

	if len(testDatabases) == 0 {
		fmt.Println("\n✅ No test databases found. Nothing to clean up.")
		return
	}

	// Display test databases found
	fmt.Printf("\n🗑️  Found %d test database(s) to clean up:\n", len(testDatabases))
	for _, db := range testDatabases {
		fmt.Printf("  - %s\n", db)
	}

	// Safety check - list protected databases that should never be deleted
	protectedDatabases := []string{
		"production",
		"prod",
		"main",
		"master",
		"staging",
		"dev",
		"development",
	}

	// Double-check none of the test databases are protected
	for _, testDb := range testDatabases {
		for _, protected := range protectedDatabases {
			if strings.EqualFold(testDb, protected) {
				fmt.Printf("\n⚠️  WARNING: Skipping protected database: %s\n", testDb)
				// Remove from cleanup list
				newTestDbs := []string{}
				for _, db := range testDatabases {
					if db != testDb {
						newTestDbs = append(newTestDbs, db)
					}
				}
				testDatabases = newTestDbs
				break
			}
		}
	}

	// Perform cleanup
	fmt.Printf("\n🧹 Starting cleanup of %d test database(s)...\n", len(testDatabases))

	successCount := 0
	failCount := 0

	for _, db := range testDatabases {
		fmt.Printf("  Deleting %s... ", db)

		// Create a new context for each deletion with timeout
		deleteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.DeleteDatabase(deleteCtx, db)
		cancel()

		if err != nil {
			fmt.Printf("❌ Failed: %v\n", err)
			failCount++
		} else {
			fmt.Printf("✅ Success\n")
			successCount++
		}
	}

	// Display summary
	fmt.Printf("\n📊 Cleanup Summary:\n")
	fmt.Printf("  ✅ Successfully deleted: %d database(s)\n", successCount)
	if failCount > 0 {
		fmt.Printf("  ❌ Failed to delete: %d database(s)\n", failCount)
	}

	// List remaining databases
	fmt.Println("\n📋 Remaining databases after cleanup:")
	remainingDbs, err := client.ListDatabases(ctx)
	if err != nil {
		fmt.Printf("  Failed to list databases: %v\n", err)
	} else {
		if len(remainingDbs) == 0 {
			fmt.Println("  (No databases remaining)")
		} else {
			for _, db := range remainingDbs {
				fmt.Printf("  - %s\n", db)
			}
		}
	}

	fmt.Println("\n=== Cleanup Completed ===")
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running
// 2. Run this cleanup utility: go run examples/08_cleanup.go
// 3. The utility will automatically identify and delete test databases
//
// Safety features:
// - Only deletes databases matching test patterns
// - Protects production and important databases
// - Shows what will be deleted before proceeding
// - Reports success/failure for each deletion
//
// Customization:
// - Add your own patterns to testDatabasePatterns
// - Add protected database names to protectedDatabases
// - Adjust timeout values as needed

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}