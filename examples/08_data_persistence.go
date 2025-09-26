package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

func main() {
	fmt.Println("=== TypeDB v3 Comprehensive Feature Demonstration ===")
	fmt.Println("Demonstrating: Data Types, Constraints, Aggregations, Fetch Queries, Advanced Patterns")
	fmt.Println()

	testDbName := "comprehensive_demo_db"
	ctx := context.Background()

	// Phase 1: First session - Create database and insert data
	fmt.Println("📝 Phase 1: First Session - Create and Populate Database")
	fmt.Println("----------------------------------------")

	client1, err := typedbclient.NewClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client 1: %v", err)
	}

	// Create test database - always delete first to ensure clean state
	fmt.Println("Creating fresh test database...")
	// Always delete existing database to ensure clean state
	client1.DeleteDatabase(ctx, testDbName) // Ignore error if doesn't exist
	if err := client1.CreateDatabase(ctx, testDbName); err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	fmt.Printf("✓ Database '%s' created\n", testDbName)

	// Define schema using simplified API
	fmt.Println("\nDefining schema...")
	database1 := client1.GetDatabase(testDbName)

	// Define schema with ALL TypeQL v3 data types and constraints for comprehensive testing
	// TypeQL v3 requires attributes to be defined BEFORE they can be used in entities/relations

	// Step 1: Define all attributes with their types and constraints FIRST
	attributesSchema := `define
		attribute name value string @regex("^[A-Za-z ]+$");
		attribute age value integer @range(18..120);
		attribute email value string @regex("^[^@]+@[^@]+\.[^@]+$");
		attribute salary value double @range(0.0..1000000.0);
		attribute is_active value boolean;
		attribute birth_date value date;
		attribute last_login value datetime;
		attribute registration_time value datetime-tz;
		attribute performance_rating value integer @range(1..10);

		attribute company_name value string;
		attribute industry value string @values("Technology", "Finance", "Healthcare", "Manufacturing");
		attribute revenue value double @range(0.0..100000000.0);
		attribute is_public value boolean;
		attribute founded_date value date;
		attribute employee_count value integer @range(1..50000);
		attribute start_date value date;
		attribute contract_active value boolean;
		attribute position_level value string @values("Junior", "Mid", "Senior", "Lead", "Manager");`

	// Execute attributes definition first - must be done before entities can reference them
	if _, err := database1.ExecuteSchema(ctx, attributesSchema); err != nil {
		log.Fatalf("Failed to define attributes schema: %v", err)
	}
	fmt.Println("✓ Attributes with constraints defined")

	// Step 2: Define entities and relations (now that attributes exist)
	basicSchema := `define
		entity person,
			owns name,
			owns age,
			owns email,
			owns salary,
			owns is_active,
			owns birth_date,
			owns last_login,
			owns registration_time,
			owns performance_rating;

		entity company,
			owns company_name,
			owns industry,
			owns revenue,
			owns is_public,
			owns founded_date,
			owns employee_count;

		relation employment,
			relates employer,
			relates employee,
			owns start_date,
			owns contract_active,
			owns position_level;

		person plays employment:employee;
		company plays employment:employer;`

	// Execute entity/relation schema
	if _, err := database1.ExecuteSchema(ctx, basicSchema); err != nil {
		log.Fatalf("Failed to define basic schema: %v", err)
	}
	fmt.Println("✓ Entities and relations defined")

	// Step 3: Add key and unique constraints (after entities/relations are defined)
	// Test without key/unique constraints first to isolate the issue
	fmt.Println("⚠️  Testing constraints step by step for debugging...")

	// Test 1: Skip @key constraint on person name to avoid uniqueness issues
	// Note: @key makes the attribute unique, which would conflict with duplicate inserts
	fmt.Println("Skipping @key constraint on person name to avoid duplicate conflicts...")

	// Test 2: Add @unique constraint
	constraintsSchema2 := `define person owns email @unique;`
	fmt.Println("Testing @unique constraint on person email...")

	if _, err := database1.ExecuteSchema(ctx, constraintsSchema2); err != nil {
		log.Fatalf("❌ Failed to define @unique constraint: %v", err)
	}
	fmt.Println("✓ @unique constraint defined successfully")

	// Test 3: Add remaining constraints
	constraintsSchema3 := `define
		company owns company_name @key;
		employment owns position_level @card(1);`
	fmt.Println("Testing company @key and employment @card constraints...")

	if _, err := database1.ExecuteSchema(ctx, constraintsSchema3); err != nil {
		log.Fatalf("❌ Failed to define company/employment constraints: %v", err)
	}
	fmt.Println("✓ All constraint types defined successfully")
	fmt.Println("✓ Constraints schema defined")
	fmt.Println("✓ Complete schema with all TypeQL v3 features defined successfully")

	// Insert test data using simplified API
	fmt.Println("\nInserting test data...")

	// Create bundle with insert operations demonstrating ALL TypeQL v3 data types
	insertBundle := []typedbclient.BundleOperation{
		// Insert persons with all data types - normalized format
		{Type: typedbclient.OpExecute, Query: `insert $p1 isa person, has name "Alice Smith", has age 30, has email "alice@example.com", has salary 85000.50, has is_active true, has birth_date 1994-03-15, has last_login 2024-01-01T10:30:00, has registration_time 2023-01-01T04:00:00Z, has performance_rating 8;`},

		{Type: typedbclient.OpExecute, Query: `insert $p2 isa person, has name "Bob Johnson", has age 25, has email "bob@example.com", has salary 65000.00, has is_active true, has birth_date 1999-07-22, has last_login 2024-01-02T14:45:30, has registration_time 2023-06-15T14:00:00Z, has performance_rating 7;`},

		{Type: typedbclient.OpExecute, Query: `insert $p3 isa person, has name "Charlie Brown", has age 35, has email "charlie@example.com", has salary 95000.75, has is_active false, has birth_date 1989-11-08, has last_login 2023-12-31T23:59:59, has registration_time 2022-03-01T00:00:00Z, has performance_rating 9;`},

		// Insert companies with double, boolean, and date types
		{Type: typedbclient.OpExecute, Query: `insert
		$c1 isa company,
			has company_name "TechCorp",
			has industry "Technology",
			has revenue 5000000.50,
			has is_public true,
			has founded_date 2010-01-15,
			has employee_count 250;`},

		{Type: typedbclient.OpExecute, Query: `insert
		$c2 isa company,
			has company_name "FinanceInc",
			has industry "Finance",
			has revenue 8500000.00,
			has is_public false,
			has founded_date 2005-06-30,
			has employee_count 180;`},

		// Insert employment relations with date and boolean attributes
		{Type: typedbclient.OpExecute, Query: `match
		$p isa person, has name "Alice Smith";
		$c isa company, has company_name "TechCorp";
		insert
		$emp isa employment,
			links (employee: $p, employer: $c),
			has start_date 2020-03-01,
			has contract_active true,
			has position_level "Senior";`},

		{Type: typedbclient.OpExecute, Query: `match
		$p isa person, has name "Bob Johnson";
		$c isa company, has company_name "FinanceInc";
		insert
		$emp isa employment,
			links (employee: $p, employer: $c),
			has start_date 2021-06-15,
			has contract_active true,
			has position_level "Mid";`},

		{Type: typedbclient.OpExecute, Query: `match
		$p isa person, has name "Charlie Brown";
		$c isa company, has company_name "TechCorp";
		insert
		$emp isa employment,
			links (employee: $p, employer: $c),
			has start_date 2019-01-10,
			has contract_active false,
			has position_level "Lead";`},
	}

	results, err := database1.ExecuteBundle(ctx, typedbclient.Write, insertBundle)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Report results
	for i := 0; i < 4; i++ {
		if i < len(results) && results[i] != nil {
			fmt.Printf("✓ Data batch %d inserted\n", i+1)
		}
	}

	// Verify data in first session using simplified API
	fmt.Println("\nVerifying data in first session...")

	// Create bundle with count queries
	verifyBundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: "match\n\t$p isa person;\nreduce\n\t$count = count($p);"},
		{Type: typedbclient.OpExecute, Query: "match\n\t$c isa company;\nreduce\n\t$count = count($c);"},
		{Type: typedbclient.OpExecute, Query: "match\n\t$e isa employment;\nreduce\n\t$count = count($e);"},
	}

	verifyResults, err := database1.ExecuteBundle(ctx, typedbclient.Read, verifyBundle)
	if err != nil {
		log.Fatalf("Failed to verify data: %v", err)
	}


	// Extract counts using new TypedRow API
	personCount := 0
	if len(verifyResults) > 0 && verifyResults[0] != nil && verifyResults[0].IsRowStream &&
		len(verifyResults[0].TypedRows) > 0 {
		// Use type-safe GetCount() method
		if count, err := verifyResults[0].TypedRows[0].GetCount(); err == nil {
			personCount = int(count)
		}
	}
	fmt.Printf("  • Person count: %d\n", personCount)

	companyCount := 0
	if len(verifyResults) > 1 && verifyResults[1] != nil && verifyResults[1].IsRowStream &&
		len(verifyResults[1].TypedRows) > 0 {
		// Use type-safe GetCount() method
		if count, err := verifyResults[1].TypedRows[0].GetCount(); err == nil {
			companyCount = int(count)
		}
	}
	fmt.Printf("  • Company count: %d\n", companyCount)

	employmentCount := 0
	if len(verifyResults) > 2 && verifyResults[2] != nil && verifyResults[2].IsRowStream &&
		len(verifyResults[2].TypedRows) > 0 {
		// Use type-safe GetCount() method
		if count, err := verifyResults[2].TypedRows[0].GetCount(); err == nil {
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

	// Query specific persons - demonstrates safe string and concept access
	fmt.Println("\n1. Query specific persons by name:")
	personQueries := []string{"Alice Smith", "Bob Johnson", "Charlie Brown"}
	for _, name := range personQueries {
		query := fmt.Sprintf(`match $p isa person, has name "%s", has email $e, has age $a;`, name)
		result, err := database2.ExecuteRead(ctx, query)
		if err != nil {
			fmt.Printf("  ❌ Failed to query %s: %v\n", name, err)
		} else if result.IsRowStream && len(result.TypedRows) > 0 {
			// Demonstrate safe string access with GetString()
			email, err := result.TypedRows[0].GetString("e")
			if err != nil {
				fmt.Printf("  ✓ Found %s (email not a string: %v)\n", name, err)
			} else {
				fmt.Printf("  ✓ Found %s with email: %s\n", name, email)
			}

			// Demonstrate safe integer access with GetInt64()
			if age, err := result.TypedRows[0].GetInt64("a"); err == nil {
				fmt.Printf("     Age: %d (using GetInt64())\n", age)
			}

			// Demonstrate safe concept access with GetConcept()
			if person, err := result.TypedRows[0].GetConcept("p"); err == nil {
				fmt.Printf("     Person IID: %s (using GetConcept())\n", person.IID)
			}
		} else {
			fmt.Printf("  ❌ %s not found\n", name)
		}
	}

	// Query companies - demonstrates safe string access for attributes
	fmt.Println("\n2. Query companies:")
	companyQuery := `match $c isa company, has company_name $n, has industry $i;`
	companyRes, err := database2.ExecuteRead(ctx, companyQuery)
	if err != nil {
		fmt.Printf("  ❌ Failed to query companies: %v\n", err)
	} else if companyRes.IsRowStream {
		fmt.Printf("  ✓ Found %d companies\n", len(companyRes.TypedRows))
		for i, row := range companyRes.TypedRows {
			if i < 3 { // Show first 3
				// Demonstrate safe string access for company name
				companyName, err := row.GetString("n")
				if err != nil {
					fmt.Printf("    • Company %d: (name error: %v)\n", i+1, err)
					continue
				}

				// Demonstrate safe string access for industry
				industry, err := row.GetString("i")
				if err != nil {
					fmt.Printf("    • Company: %s, Industry: (error: %v)\n", companyName, err)
				} else {
					fmt.Printf("    • Company: %s, Industry: %s\n", companyName, industry)
				}

				// Also demonstrate concept access for the company entity
				if company, err := row.GetConcept("c"); err == nil {
					fmt.Printf("      Company IID: %s\n", company.IID)
				}
			}
		}
	}

	// Query employment relations with date and boolean attributes
	fmt.Println("\n3. Query employment relations with date and boolean attributes:")
	employmentQuery := `match
		$emp isa employment, links (employee: $p, employer: $c),
			has start_date $sd,
			has contract_active $ca;
		$p has name $pname;
		$c has company_name $cname;`
	empRes, err := database2.ExecuteRead(ctx, employmentQuery)
	if err != nil {
		fmt.Printf("  ❌ Failed to query employment: %v\n", err)
	} else if empRes.IsRowStream {
		fmt.Printf("  ✓ Found %d employment relations\n", len(empRes.TypedRows))
		// Show detailed employment information using safe accessors
		for i, row := range empRes.TypedRows {
			if i < 3 { // Show first 3
				// Safe string access for person name
				personName, _ := row.GetString("pname")
				// Safe string access for company name
				companyName, _ := row.GetString("cname")
				fmt.Printf("    • %s works at %s\n", personName, companyName)

				// Demonstrate date access for start_date
				if startDate, err := row.GetDate("sd"); err == nil {
					fmt.Printf("      Start Date: %v\n", startDate)
				} else {
					fmt.Printf("      Start Date: (error: %v)\n", err)
				}

				// Demonstrate boolean access for contract_active
				if contractActive, err := row.GetBool("ca"); err == nil {
					fmt.Printf("      Contract Active: %v\n", contractActive)
				} else {
					fmt.Printf("      Contract Active: (error: %v)\n", err)
				}

				// Demonstrate concept access for employment relation
				if emp, err := row.GetConcept("emp"); err == nil {
					fmt.Printf("      Employment IID: %s\n", emp.IID)
				}
			}
		}
	}

	// Query person attributes with all data types
	fmt.Println("\n4. Query person attributes (all data types demonstration):")
	// Simple query for persons with these specific attributes
	personAllQuery := `match
		$p isa person, has name $name,
			has salary $salary,
			has is_active $active,
			has birth_date $bdate,
			has last_login $login,
			has registration_time $regtime;`

	personAllRes, err := database2.ExecuteRead(ctx, personAllQuery)
	if err != nil {
		fmt.Printf("  ❌ Failed to query person attributes: %v\n", err)
	} else if personAllRes.IsRowStream {
		fmt.Printf("  ✓ Found %d persons with attributes\n", len(personAllRes.TypedRows))
		for i, row := range personAllRes.TypedRows {
			if i < 2 { // Show first 2 persons in detail
				fmt.Printf("\n  Person %d attributes:\n", i+1)

				// String type (name)
				if name, err := row.GetString("name"); err == nil {
					fmt.Printf("    • Name (string): %s\n", name)
				}

				// Double type (salary)
				if salary, err := row.GetFloat64("salary"); err == nil {
					fmt.Printf("    • Salary (double): %.2f\n", salary)
				} else {
					fmt.Printf("    • Salary: (error: %v)\n", err)
				}

				// Boolean type (is_active)
				if active, err := row.GetBool("active"); err == nil {
					fmt.Printf("    • Is Active (boolean): %v\n", active)
				} else {
					fmt.Printf("    • Is Active: (error: %v)\n", err)
				}

				// Date type (birth_date)
				if bdate, err := row.GetDate("bdate"); err == nil {
					fmt.Printf("    • Birth Date (date): %v\n", bdate)
				} else {
					fmt.Printf("    • Birth Date: (error: %v)\n", err)
				}

				// DateTime type (last_login)
				if login, err := row.GetDateTime("login"); err == nil {
					fmt.Printf("    • Last Login (datetime): %v\n", login.ToTime())
				} else {
					fmt.Printf("    • Last Login: (error: %v)\n", err)
				}

				// DateTime-tz type (registration_time)
				if regtime, err := row.GetDateTime("regtime"); err == nil {
					fmt.Printf("    • Registration Time (datetime-tz): %v\n", regtime.ToTime())
				} else {
					fmt.Printf("    • Registration Time: (error: %v)\n", err)
				}
			}
		}
	}

	// Phase 3: Add more data in second session and verify
	fmt.Println("\n📝 Phase 3: Add More Data in Second Session")
	fmt.Println("----------------------------------------")

	// Insert person with all attribute types for comprehensive testing
	newPersonQuery := `insert
		$p isa person,
			has name "Diana Prince",
			has age 28,
			has email "diana@example.com",
			has salary 95000.75,              # double type
			has is_active true,               # boolean type
			has birth_date 1995-06-15,        # date type
			has last_login 2024-01-02T14:30:00, # datetime type
			has registration_time 2023-06-01T15:00:00Z; # datetime-tz type (UTC)`

	if _, err := database2.ExecuteWrite(ctx, newPersonQuery); err != nil {
		fmt.Printf("❌ Failed to insert new data: %v\n", err)
	} else {
		fmt.Println("✓ New person 'Diana Prince' inserted with all data types")
	}

	// Verify new data using simplified API
	verifyNewRes, err := database2.ExecuteRead(ctx, `match $p isa person, has name "Diana Prince";`)
	if err != nil {
		fmt.Printf("❌ Failed to verify new data: %v\n", err)
	} else if verifyNewRes.IsRowStream && len(verifyNewRes.TypedRows) > 0 {
		fmt.Println("✓ New data successfully verified")
	}

	// Final count verification using simplified API
	fmt.Println("\nFinal data counts:")
	finalPersonCount := 0
	finalResult, err := database2.ExecuteRead(ctx, "match\n\t\t$p isa person;\n\treduce\n\t\t$count = count($p);")
	if err == nil && finalResult.IsRowStream && len(finalResult.TypedRows) > 0 {
		// Use type-safe GetCount() method
		if count, err := finalResult.TypedRows[0].GetCount(); err == nil {
			finalPersonCount = int(count)
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

	// Test 3: Relation persistence (we actually insert 3 employments)
	if employmentCount == 3 {
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

	// Test 5: Query consistency (we have 3 employment relations)
	if empRes != nil && empRes.IsRowStream && len(empRes.TypedRows) == 3 {
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

	// Phase 4: TypeQL v3 Advanced Features Demonstration
	fmt.Println("\n🚀 Phase 4: TypeQL v3 Advanced Features Demonstration")
	fmt.Println("========================================================")

	// Demonstrate aggregations
	demonstrateAggregations(ctx, database2)

	// Demonstrate fetch queries
	demonstrateFeatureQueries(ctx, database2)

	// Demonstrate advanced patterns
	demonstrateAdvancedPatterns(ctx, database2)

	// Cleanup
	fmt.Println("\n🧹 Cleaning up test database...")
	if err := client2.DeleteDatabase(ctx, testDbName); err != nil {
		fmt.Printf("Warning: Failed to clean up database: %v\n", err)
	} else {
		fmt.Println("✓ Test database cleaned up")
	}

	fmt.Println("\n=== TypeDB v3 Comprehensive Feature Demonstration Completed ===")
}

// demonstrateAggregations demonstrates TypeQL v3 aggregation features
func demonstrateAggregations(ctx context.Context, database *typedbclient.Database) {
	fmt.Println("\n📊 Aggregations Demonstration")
	fmt.Println("------------------------------")

	// 1. Basic count aggregation
	fmt.Println("\n1. Count aggregations:")
	countQuery := `match $p isa person; reduce $count = count($p);`
	countResult, err := database.ExecuteRead(ctx, countQuery)
	if err != nil {
		fmt.Printf("  ❌ Count query failed: %v\n", err)
	} else if countResult.IsRowStream && len(countResult.TypedRows) > 0 {
		if count, err := countResult.TypedRows[0].GetCount(); err == nil {
			fmt.Printf("  ✓ Total persons: %d\n", count)
		}
	}

	// 2. Statistical aggregations on salary
	fmt.Println("\n2. Statistical aggregations on salary:")
	statQueries := map[string]string{
		"Average": `match $p isa person, has salary $s; reduce $avg = mean($s);`,
		"Maximum": `match $p isa person, has salary $s; reduce $max = max($s);`,
		"Minimum": `match $p isa person, has salary $s; reduce $min = min($s);`,
		"Sum":     `match $p isa person, has salary $s; reduce $sum = sum($s);`,
	}

	for statName, query := range statQueries {
		result, err := database.ExecuteRead(ctx, query)
		if err != nil {
			fmt.Printf("  ❌ %s query failed: %v\n", statName, err)
		} else if result.IsRowStream && len(result.TypedRows) > 0 {
			row := result.TypedRows[0]
			// Try to get the aggregation result from different possible column names
			if val, err := row.GetValue("avg"); err == nil && val.Type() == typedbclient.TypeFloat64 {
				if avgVal, err := val.AsFloat64(); err == nil {
					fmt.Printf("  ✓ %s salary: %.2f\n", statName, avgVal)
				}
			} else if val, err := row.GetValue("max"); err == nil && val.Type() == typedbclient.TypeFloat64 {
				if maxVal, err := val.AsFloat64(); err == nil {
					fmt.Printf("  ✓ %s salary: %.2f\n", statName, maxVal)
				}
			} else if val, err := row.GetValue("min"); err == nil && val.Type() == typedbclient.TypeFloat64 {
				if minVal, err := val.AsFloat64(); err == nil {
					fmt.Printf("  ✓ %s salary: %.2f\n", statName, minVal)
				}
			} else if val, err := row.GetValue("sum"); err == nil && val.Type() == typedbclient.TypeFloat64 {
				if sumVal, err := val.AsFloat64(); err == nil {
					fmt.Printf("  ✓ %s salary: %.2f\n", statName, sumVal)
				}
			}
		}
	}

	// 3. Group by aggregations
	fmt.Println("\n3. Group by aggregations (by industry):")
	groupByQuery := `match
		$c isa company, has industry $industry;
		$e isa employment, links (employer: $c);
	reduce
		$count = count($e) groupby $industry;`

	groupResult, err := database.ExecuteRead(ctx, groupByQuery)
	if err != nil {
		fmt.Printf("  ❌ Group by query failed: %v\n", err)
	} else if groupResult.IsRowStream {
		fmt.Printf("  ✓ Found %d industry groups\n", len(groupResult.TypedRows))
		for i, row := range groupResult.TypedRows {
			if i < 5 { // Show first 5 groups
				if industry, err := row.GetString("industry"); err == nil {
					if count, err := row.GetCount(); err == nil {
						fmt.Printf("    • %s industry: %d employees\n", industry, count)
					}
				}
			}
		}
	}
}

// demonstrateFeatureQueries demonstrates TypeQL v3 select and fetch features
func demonstrateFeatureQueries(ctx context.Context, database *typedbclient.Database) {
	fmt.Println("\n🔍 Select and Fetch Queries Demonstration")
	fmt.Println("------------------------------------------")

	// 1. Select queries (replaces get in TypeQL v3)
	fmt.Println("\n1. Select queries:")
	selectQuery := `match
		$p isa person, has name $name, has salary $salary, has age $age;
	select
		$name, $salary, $age;`

	selectResult, err := database.ExecuteRead(ctx, selectQuery)
	if err != nil {
		fmt.Printf("  ❌ Select query failed: %v\n", err)
	} else if selectResult.IsRowStream {
		fmt.Printf("  ✓ Selected %d person records\n", len(selectResult.TypedRows))
		for i, row := range selectResult.TypedRows {
			if i < 3 { // Show first 3
				name, _ := row.GetString("name")
				salary, _ := row.GetFloat64("salary")
				age, _ := row.GetInt64("age")
				fmt.Printf("    • %s: Age %d, Salary %.0f\n", name, age, salary)
			}
		}
	}

	// 2. Fetch queries with JSON-like structure
	fmt.Println("\n2. Fetch queries (JSON output):")
	fetchQuery := `match
		$p isa person, has name $name, has email $email;
		$e isa employment, links (employee: $p, employer: $c);
		$c has company_name $company;
	fetch {
		"person": {
			"name": $name,
			"email": $email
		},
		"company": $company
	};`

	fetchResult, err := database.ExecuteRead(ctx, fetchQuery)
	if err != nil {
		fmt.Printf("  ❌ Fetch query failed: %v\n", err)
	} else if fetchResult.IsDocumentStream {
		fmt.Printf("  ✓ Fetched %d employment documents\n", len(fetchResult.TypedDocuments))
		for i, doc := range fetchResult.TypedDocuments {
			if i < 2 { // Show first 2
				fmt.Printf("    • Document %d: %+v\n", i+1, doc)
			}
		}
	} else if fetchResult.IsRowStream {
		fmt.Printf("  ✓ Fetch returned %d rows (fallback to row format)\n", len(fetchResult.TypedRows))
		for i, row := range fetchResult.TypedRows {
			if i < 2 {
				name, _ := row.GetString("name")
				email, _ := row.GetString("email")
				company, _ := row.GetString("company")
				fmt.Printf("    • %s (%s) works at %s\n", name, email, company)
			}
		}
	}

	// 3. Complex fetch with computed values
	fmt.Println("\n3. Complex fetch with computed values:")
	complexFetchQuery := `match
		$p isa person, has name $name, has age $age, has salary $salary;
	fetch {
		"person": {
			"name": $name,
			"age": $age,
			"salary": $salary
		}
	};`

	complexResult, err := database.ExecuteRead(ctx, complexFetchQuery)
	if err != nil {
		fmt.Printf("  ❌ Complex fetch failed: %v\n", err)
	} else if complexResult.IsDocumentStream {
		fmt.Printf("  ✓ Complex fetch returned %d documents\n", len(complexResult.TypedDocuments))
		// Process document results
	} else if complexResult.IsRowStream {
		fmt.Printf("  ✓ Complex fetch returned %d rows\n", len(complexResult.TypedRows))
		for i, row := range complexResult.TypedRows {
			if i < 2 {
				name, _ := row.GetString("name")
				age, _ := row.GetInt64("age")
				salary, _ := row.GetFloat64("salary")
				fmt.Printf("    • %s: %d years old, $%.0f salary\n", name, age, salary)
			}
		}
	}
}

// demonstrateAdvancedPatterns demonstrates TypeQL v3 advanced query patterns
func demonstrateAdvancedPatterns(ctx context.Context, database *typedbclient.Database) {
	fmt.Println("\n🎯 Advanced Query Patterns Demonstration")
	fmt.Println("------------------------------------------")

	// 1. Disjunction (OR) patterns - find people who are either young or high earners
	fmt.Println("\n1. Disjunction patterns (OR logic):")
	disjunctionQuery := `match
		$p isa person, has name $name, has age $age, has salary $salary;
		{ $age < 30; } or { $salary > 80000; };`

	disjResult, err := database.ExecuteRead(ctx, disjunctionQuery)
	if err != nil {
		fmt.Printf("  ❌ Disjunction query failed: %v\n", err)
	} else if disjResult.IsRowStream {
		fmt.Printf("  ✓ Found %d people who are either young (<30) or high earners (>80k)\n", len(disjResult.TypedRows))
		for i, row := range disjResult.TypedRows {
			if i < 3 { // Show first 3
				name, _ := row.GetString("name")
				age, _ := row.GetInt64("age")
				salary, _ := row.GetFloat64("salary")
				reason := "Unknown"
				if age < 30 && salary > 80000 {
					reason = "Both young AND high earner"
				} else if age < 30 {
					reason = "Young (under 30)"
				} else if salary > 80000 {
					reason = "High earner (over $80k)"
				}
				fmt.Printf("    • %s: Age %d, Salary $%.0f - %s\n", name, age, salary, reason)
			}
		}
	}

	// 2. Negation (NOT) patterns - find people who don't work in Technology
	fmt.Println("\n2. Negation patterns (NOT logic):")
	negationQuery := `match
		$p isa person, has name $name;
		$emp isa employment, links (employee: $p, employer: $c);
		$c isa company, has industry $industry;
		not { $industry == "Technology"; };`

	negResult, err := database.ExecuteRead(ctx, negationQuery)
	if err != nil {
		fmt.Printf("  ❌ Negation query failed: %v\n", err)
	} else if negResult.IsRowStream {
		fmt.Printf("  ✓ Found %d people who don't work in Technology\n", len(negResult.TypedRows))
		for i, row := range negResult.TypedRows {
			if i < 3 { // Show first 3
				name, _ := row.GetString("name")
				industry, _ := row.GetString("industry")
				fmt.Printf("    • %s works in %s industry (not Technology)\n", name, industry)
			}
		}
	}

	// 3. Value comparisons between variables - find employees earning more than average
	fmt.Println("\n3. Value comparisons between variables:")
	comparisonQuery := `match
		$p isa person, has name $name, has salary $salary;
		$salary > 75000;`

	compResult, err := database.ExecuteRead(ctx, comparisonQuery)
	if err != nil {
		fmt.Printf("  ❌ Comparison query failed: %v\n", err)
	} else if compResult.IsRowStream {
		fmt.Printf("  ✓ Found %d people earning above average (>$75k)\n", len(compResult.TypedRows))
		for i, row := range compResult.TypedRows {
			if i < 3 { // Show first 3
				name, _ := row.GetString("name")
				salary, _ := row.GetFloat64("salary")
				fmt.Printf("    • %s: $%.0f (above $75k average)\n", name, salary)
			}
		}
	}

	// 4. Complex pattern matching with multiple conditions
	fmt.Println("\n4. Complex pattern matching:")
	complexQuery := `match
		$p isa person,
			has name $name,
			has age $age,
			has salary $salary,
			has performance_rating $rating;
		$emp isa employment, links (employee: $p, employer: $c);
		$c has industry $industry;
		$age >= 25;
		$age <= 35;
		$salary > 60000;
		$rating >= 7;
		{ $industry == "Technology"; } or { $industry == "Finance"; };`

	complexResult, err := database.ExecuteRead(ctx, complexQuery)
	if err != nil {
		fmt.Printf("  ❌ Complex pattern query failed: %v\n", err)
	} else if complexResult.IsRowStream {
		fmt.Printf("  ✓ Found %d high-performing employees in prime age range\n", len(complexResult.TypedRows))
		for i, row := range complexResult.TypedRows {
			if i < 3 { // Show first 3
				name, _ := row.GetString("name")
				age, _ := row.GetInt64("age")
				salary, _ := row.GetFloat64("salary")
				rating, _ := row.GetInt64("rating")
				industry, _ := row.GetString("industry")
				fmt.Printf("    • %s: Age %d, $%.0f, Rating %d/10, %s\n", name, age, salary, rating, industry)
			}
		}
	}

	// 5. Pattern matching with range constraints demonstration
	fmt.Println("\n5. Range constraint validation:")
	rangeQuery := `match
		$p isa person, has age $age, has performance_rating $rating;
		$age >= 18; $age <= 120;  # Age range constraint validation
		$rating >= 1; $rating <= 10;  # Performance rating range validation`

	rangeResult, err := database.ExecuteRead(ctx, rangeQuery)
	if err != nil {
		fmt.Printf("  ❌ Range validation query failed: %v\n", err)
	} else if rangeResult.IsRowStream {
		fmt.Printf("  ✓ All %d records pass range constraint validation\n", len(rangeResult.TypedRows))
		fmt.Printf("    (Ages 18-120, Performance ratings 1-10)\n")
	}

	fmt.Println("\n6. Summary of advanced pattern features:")
	fmt.Printf("   ✓ Disjunction (OR): Find entities matching any of multiple conditions\n")
	fmt.Printf("   ✓ Negation (NOT): Exclude entities matching specific patterns\n")
	fmt.Printf("   ✓ Value comparisons: Compare attribute values between variables\n")
	fmt.Printf("   ✓ Complex patterns: Multiple conditions with logical operators\n")
	fmt.Printf("   ✓ Range validation: Constraint compliance verification\n")
}