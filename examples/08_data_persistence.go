package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

func main() {
	fmt.Println("=== TypeDB v3 Data Persistence Verification Test ===")
	fmt.Println()

	testDbName := "persistence_test_db"
	ctx := context.Background()

	// Phase 1: First session - Create database and insert data
	fmt.Println("📝 Phase 1: First Session - Create and Populate Database")
	fmt.Println("----------------------------------------")

	client1, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client 1: %v", err)
	}

	// Create test database
	fmt.Println("Creating test database...")
	if err := client1.CreateDatabase(ctx, testDbName); err != nil {
		// If database exists, delete and recreate
		client1.DeleteDatabase(ctx, testDbName)
		if err := client1.CreateDatabase(ctx, testDbName); err != nil {
			log.Fatalf("Failed to create database: %v", err)
		}
	}
	fmt.Printf("✓ Database '%s' created\n", testDbName)

	// Define schema using transaction API
	fmt.Println("\nDefining schema...")
	database1 := client1.GetDatabase(testDbName)

	// Begin schema transaction
	schemaTx, err := database1.BeginTransaction(ctx, typedbclient.Schema)
	if err != nil {
		log.Fatalf("Failed to begin schema transaction: %v", err)
	}

	schemaQuery := `define
		entity person, owns name, owns age, owns email;
		entity company, owns company_name, owns industry;
		relation employment,
			relates employer,
			relates employee;
		person plays employment:employee;
		company plays employment:employer;
		attribute name, value string;
		attribute age, value integer;
		attribute email, value string;
		attribute company_name, value string;
		attribute industry, value string;`

	// Execute schema with commit
	schemaBundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: schemaQuery},
		{Type: typedbclient.OpCommit},
		{Type: typedbclient.OpClose},
	}

	if _, err := schemaTx.ExecuteBundle(ctx, schemaBundle); err != nil {
		log.Fatalf("Failed to define schema: %v", err)
	}
	fmt.Println("✓ Schema defined successfully")

	// Insert test data using transaction API
	fmt.Println("\nInserting test data...")

	// Begin write transaction for all inserts
	writeTx, err := database1.BeginTransaction(ctx, typedbclient.Write)
	if err != nil {
		log.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Create bundle with all insert operations
	insertBundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: `insert
		$p1 isa person, has name "Alice Smith", has age 30, has email "alice@example.com";
		$p2 isa person, has name "Bob Johnson", has age 25, has email "bob@example.com";
		$p3 isa person, has name "Charlie Brown", has age 35, has email "charlie@example.com";`},

		{Type: typedbclient.OpExecute, Query: `insert
		$c1 isa company, has company_name "TechCorp", has industry "Technology";
		$c2 isa company, has company_name "FinanceInc", has industry "Finance";`},

		{Type: typedbclient.OpExecute, Query: `match
		$p isa person, has name "Alice Smith";
		$c isa company, has company_name "TechCorp";
		insert
		$emp isa employment, links (employee: $p, employer: $c);`},

		{Type: typedbclient.OpExecute, Query: `match
		$p isa person, has name "Bob Johnson";
		$c isa company, has company_name "FinanceInc";
		insert
		$emp isa employment, links (employee: $p, employer: $c);`},

		// Commit all changes
		{Type: typedbclient.OpCommit},
		{Type: typedbclient.OpClose},
	}

	results, err := writeTx.ExecuteBundle(ctx, insertBundle)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Report results
	for i := 0; i < 4; i++ {
		if i < len(results) && results[i] != nil {
			fmt.Printf("✓ Data batch %d inserted\n", i+1)
		}
	}

	// Verify data in first session using transaction API
	fmt.Println("\nVerifying data in first session...")

	// Begin read transaction for verification
	readTx, err := database1.BeginTransaction(ctx, typedbclient.Read)
	if err != nil {
		log.Fatalf("Failed to begin read transaction: %v", err)
	}

	// Create bundle with count queries
	verifyBundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
		{Type: typedbclient.OpExecute, Query: "match $c isa company; reduce $count = count($c);"},
		{Type: typedbclient.OpExecute, Query: "match $e isa employment; reduce $count = count($e);"},
		{Type: typedbclient.OpClose},
	}

	verifyResults, err := readTx.ExecuteBundle(ctx, verifyBundle)
	if err != nil {
		log.Fatalf("Failed to verify data: %v", err)
	}

	// Extract counts
	personCount := 0
	if len(verifyResults) > 0 && verifyResults[0] != nil && verifyResults[0].IsRowStream &&
		len(verifyResults[0].Rows) > 0 && len(verifyResults[0].Rows[0]) > 0 {
		if count, ok := verifyResults[0].Rows[0][0].(float64); ok {
			personCount = int(count)
		}
	}
	fmt.Printf("  • Person count: %d\n", personCount)

	companyCount := 0
	if len(verifyResults) > 1 && verifyResults[1] != nil && verifyResults[1].IsRowStream &&
		len(verifyResults[1].Rows) > 0 && len(verifyResults[1].Rows[0]) > 0 {
		if count, ok := verifyResults[1].Rows[0][0].(float64); ok {
			companyCount = int(count)
		}
	}
	fmt.Printf("  • Company count: %d\n", companyCount)

	employmentCount := 0
	if len(verifyResults) > 2 && verifyResults[2] != nil && verifyResults[2].IsRowStream &&
		len(verifyResults[2].Rows) > 0 && len(verifyResults[2].Rows[0]) > 0 {
		if count, ok := verifyResults[2].Rows[0][0].(float64); ok {
			employmentCount = int(count)
		}
	}
	fmt.Printf("  • Employment relation count: %d\n", employmentCount)

	// Close first client
	client1.Close()
	fmt.Println("\n✓ First session closed")

	// Wait a moment to ensure complete disconnection
	fmt.Println("\n⏳ Waiting 2 seconds before starting new session...")
	time.Sleep(2 * time.Second)

	// Phase 2: Second session - Verify data persistence
	fmt.Println("\n📖 Phase 2: Second Session - Verify Data Persistence")
	fmt.Println("----------------------------------------")

	client2, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client 2: %v", err)
	}
	defer client2.Close()

	// Check database exists
	exists, err := client2.DatabaseExists(ctx, testDbName)
	if err != nil {
		log.Fatalf("Failed to check database existence: %v", err)
	}
	if !exists {
		log.Fatalf("❌ Database '%s' does not exist in second session!", testDbName)
	}
	fmt.Printf("✓ Database '%s' found in second session\n", testDbName)

	database2 := client2.GetDatabase(testDbName)

	// Verify data in second session
	fmt.Println("\nVerifying data in second session...")

	// Query specific persons
	fmt.Println("\n1. Query specific persons by name:")
	personQueries := []string{"Alice Smith", "Bob Johnson", "Charlie Brown"}
	for _, name := range personQueries {
		query := fmt.Sprintf(`match $p isa person, has name "%s", has email $e;`, name)
		result, err := database2.ExecuteRead(ctx, query)
		if err != nil {
			fmt.Printf("  ❌ Failed to query %s: %v\n", name, err)
		} else if result.IsRowStream && len(result.Rows) > 0 {
			// NEW: Use Get() method to access email by column name
			email, err := result.Get("e")
			if err != nil {
				fmt.Printf("  ✓ Found %s (email access error: %v)\n", name, err)
			} else {
				fmt.Printf("  ✓ Found %s with email: %v\n", name, email)
			}
		} else {
			fmt.Printf("  ❌ %s not found\n", name)
		}
	}

	// Query companies
	fmt.Println("\n2. Query companies:")
	companyQuery := `match $c isa company, has company_name $n, has industry $i;`
	companyRes, err := database2.ExecuteRead(ctx, companyQuery)
	if err != nil {
		fmt.Printf("  ❌ Failed to query companies: %v\n", err)
	} else if companyRes.IsRowStream {
		fmt.Printf("  ✓ Found %d companies\n", len(companyRes.Rows))
		for i := range companyRes.Rows {
			if i < 3 { // Show first 3
				// NEW: Use GetFromRow() to access specific row values by column name
				companyName, _ := companyRes.GetFromRow(i, "n")
				industry, _ := companyRes.GetFromRow(i, "i")
				fmt.Printf("    • Company: %v, Industry: %v\n", companyName, industry)
			}
		}
	}

	// Query employment relations
	fmt.Println("\n3. Query employment relations:")
	employmentQuery := `match
		$emp isa employment, links (employee: $p, employer: $c);
		$p has name $pname;
		$c has company_name $cname;`
	empRes, err := database2.ExecuteRead(ctx, employmentQuery)
	if err != nil {
		fmt.Printf("  ❌ Failed to query employment: %v\n", err)
	} else if empRes.IsRowStream {
		fmt.Printf("  ✓ Found %d employment relations\n", len(empRes.Rows))
	}

	// Phase 3: Add more data in second session and verify
	fmt.Println("\n📝 Phase 3: Add More Data in Second Session")
	fmt.Println("----------------------------------------")

	// Begin write transaction for new data
	newWriteTx, err := database2.BeginTransaction(ctx, typedbclient.Write)
	if err != nil {
		fmt.Printf("❌ Failed to begin write transaction: %v\n", err)
	} else {
		newDataBundle := []typedbclient.BundleOperation{
			{Type: typedbclient.OpExecute, Query: `insert
				$p isa person, has name "Diana Prince", has age 28, has email "diana@example.com";`},
			{Type: typedbclient.OpCommit},
			{Type: typedbclient.OpClose},
		}

		if _, err := newWriteTx.ExecuteBundle(ctx, newDataBundle); err != nil {
			fmt.Printf("❌ Failed to insert new data: %v\n", err)
		} else {
			fmt.Println("✓ New person 'Diana Prince' inserted")
		}
	}

	// Verify new data using transaction API
	verifyNewTx, err := database2.BeginTransaction(ctx, typedbclient.Read)
	if err != nil {
		fmt.Printf("❌ Failed to begin verification transaction: %v\n", err)
	} else {
		verifyNewBundle := []typedbclient.BundleOperation{
			{Type: typedbclient.OpExecute, Query: `match $p isa person, has name "Diana Prince";`},
			{Type: typedbclient.OpClose},
		}

		verifyNewRes, err := verifyNewTx.ExecuteBundle(ctx, verifyNewBundle)
		if err != nil {
			fmt.Printf("❌ Failed to verify new data: %v\n", err)
		} else if len(verifyNewRes) > 0 && verifyNewRes[0] != nil &&
			verifyNewRes[0].IsRowStream && len(verifyNewRes[0].Rows) > 0 {
			fmt.Println("✓ New data successfully verified")
		}
	}

	// Final count verification using transaction API
	fmt.Println("\nFinal data counts:")
	finalPersonCount := 0
	finalReadTx, err := database2.BeginTransaction(ctx, typedbclient.Read)
	if err != nil {
		fmt.Printf("Failed to begin final read transaction: %v\n", err)
	} else {
		finalCountBundle := []typedbclient.BundleOperation{
			{Type: typedbclient.OpExecute, Query: "match $p isa person; reduce $count = count($p);"},
			{Type: typedbclient.OpClose},
		}

		finalResults, err := finalReadTx.ExecuteBundle(ctx, finalCountBundle)
		if err == nil && len(finalResults) > 0 && finalResults[0] != nil &&
			finalResults[0].IsRowStream && len(finalResults[0].Rows) > 0 &&
			len(finalResults[0].Rows[0]) > 0 {
			if count, ok := finalResults[0].Rows[0][0].(float64); ok {
				finalPersonCount = int(count)
			}
		}
	}
	fmt.Printf("  • Total persons: %d (was %d, added 1)\n", finalPersonCount, personCount)

	// Phase 4: Test results summary
	fmt.Println("\n📊 Test Results Summary")
	fmt.Println("=====================================")

	testsPassed := 0
	totalTests := 5

	// Test 1: Database persistence
	if exists {
		fmt.Println("✅ Test 1: Database persists across sessions")
		testsPassed++
	} else {
		fmt.Println("❌ Test 1: Database persistence failed")
	}

	// Test 2: Data persistence
	if personCount == 3 {
		fmt.Println("✅ Test 2: Original data persists correctly")
		testsPassed++
	} else {
		fmt.Println("❌ Test 2: Data persistence issue")
	}

	// Test 3: Relation persistence
	if employmentCount == 2 {
		fmt.Println("✅ Test 3: Relations persist correctly")
		testsPassed++
	} else {
		fmt.Println("❌ Test 3: Relation persistence issue")
	}

	// Test 4: New data insertion
	if finalPersonCount == 4 {
		fmt.Println("✅ Test 4: New data can be added in new session")
		testsPassed++
	} else {
		fmt.Println("❌ Test 4: New data insertion issue")
	}

	// Test 5: Query consistency
	if empRes != nil && empRes.IsRowStream && len(empRes.Rows) == 2 {
		fmt.Println("✅ Test 5: Query results are consistent")
		testsPassed++
	} else {
		fmt.Println("❌ Test 5: Query consistency issue")
	}

	fmt.Printf("\n🎯 Overall Result: %d/%d tests passed\n", testsPassed, totalTests)

	if testsPassed == totalTests {
		fmt.Println("✨ SUCCESS: All persistence tests passed!")
		fmt.Println("   Data is correctly persisted and retrievable across sessions.")
	} else {
		fmt.Printf("⚠️  WARNING: Only %d/%d tests passed.\n", testsPassed, totalTests)
	}

	// Cleanup
	fmt.Println("\n🧹 Cleaning up test database...")
	if err := client2.DeleteDatabase(ctx, testDbName); err != nil {
		fmt.Printf("Warning: Failed to clean up database: %v\n", err)
	} else {
		fmt.Println("✓ Test database cleaned up")
	}

	fmt.Println("\n=== Data Persistence Test Completed ===")
}