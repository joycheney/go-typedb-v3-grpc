package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 8: Database Persistence Test
// This example verifies that databases persist across client sessions.
// It demonstrates:
// - Creating a database in one session
// - Closing the client completely
// - Opening a new client and verifying the database still exists
// - Basic database lifecycle management

func main() {
	fmt.Println("=== TypeDB v3 Database Persistence Test ===")
	fmt.Println()

	testDbName := "database_persistence_test"
	ctx := context.Background()

	// Phase 1: Create database
	fmt.Println("📝 Phase 1: Create Database")
	fmt.Println("----------------------------------------")

	// Create first client
	client1, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client 1: %v", err)
	}

	// Clean up any existing test database
	client1.DeleteDatabase(ctx, testDbName)

	// Create fresh database
	if err := client1.CreateDatabase(ctx, testDbName); err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	fmt.Printf("✓ Database '%s' created\n", testDbName)

	// List databases to confirm
	databases1, err := client1.ListDatabases(ctx)
	if err != nil {
		fmt.Printf("⚠️  Failed to list databases: %v\n", err)
	} else {
		fmt.Printf("✓ Databases in session 1: %v\n", databases1)
	}

	// Close first client
	client1.Close()
	fmt.Println("\n✓ First client closed")

	// Wait to ensure complete disconnection
	fmt.Println("\n⏳ Waiting 3 seconds to ensure complete disconnection...")
	time.Sleep(3 * time.Second)

	// Phase 2: Verify database persists
	fmt.Println("\n📖 Phase 2: Verify Database Persistence")
	fmt.Println("----------------------------------------")

	// Create second client
	client2, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client 2: %v", err)
	}
	defer client2.Close()

	// List databases in new session
	databases2, err := client2.ListDatabases(ctx)
	if err != nil {
		fmt.Printf("⚠️  Failed to list databases: %v\n", err)
	} else {
		fmt.Printf("✓ Databases in session 2: %v\n", databases2)
	}

	// Check if our test database exists
	exists, err := client2.DatabaseExists(ctx, testDbName)
	if err != nil {
		log.Fatalf("Failed to check database existence: %v", err)
	}

	// Phase 3: Test Results
	fmt.Println("\n📊 Test Results")
	fmt.Println("=====================================")

	if exists {
		fmt.Println("✅ SUCCESS: Database persists across sessions!")
		fmt.Printf("   Database '%s' exists after client restart\n", testDbName)

		// Try to get database object
		database2 := client2.GetDatabase(testDbName)
		if database2 != nil {
			fmt.Println("   ✓ Can get database object in new session")
		}

		// Test creating another database to verify server is working
		testDb2 := "second_test_db"
		if err := client2.CreateDatabase(ctx, testDb2); err != nil {
			fmt.Printf("   ⚠️  Failed to create second database: %v\n", err)
		} else {
			fmt.Printf("   ✓ Can create new databases in session 2\n")
			client2.DeleteDatabase(ctx, testDb2)
		}
	} else {
		fmt.Println("❌ FAILURE: Database does not persist!")
		fmt.Printf("   Database '%s' not found after client restart\n", testDbName)
	}

	// Phase 4: Cleanup
	fmt.Println("\n🧹 Cleaning up...")
	if err := client2.DeleteDatabase(ctx, testDbName); err != nil {
		fmt.Printf("⚠️  Failed to delete test database: %v\n", err)
	} else {
		fmt.Printf("✓ Test database '%s' deleted\n", testDbName)
	}

	fmt.Println("\n=== Database Persistence Test Completed ===")
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running
// 2. Run: go run examples/08_database_persistence.go
// 3. This tests that databases persist across client sessions
//
// Expected behavior:
// - Database created in session 1 should exist in session 2
// - This verifies the TypeDB server is properly persisting data
//
// If this test fails, it indicates:
// - TypeDB server may not be persisting data to disk
// - Server may be running in memory-only mode
// - Server configuration issues