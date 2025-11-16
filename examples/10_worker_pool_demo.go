package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

func main() {
	fmt.Println("=== Worker Pool Demo: High-Concurrency Insert ===")
	fmt.Println()

	// Create client WITH worker pool enabled
	// This prevents "connection refused" errors during dense writes
	client, err := typedbclient.NewClient(&typedbclient.Options{
		Address:  "localhost:1729",
		Username: "admin",
		Password: "password",

		// Enable worker pool for high-concurrency scenarios
		EnableWorkerPool: true,   // Enable pool mode
		WorkerPoolSize:   50,      // Max 50 concurrent workers (safe limit)
		QueueSize:        100000,  // Buffer up to 100k pending tasks
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create test database
	dbName := "worker_pool_demo"
	ctx := context.Background()

	// Clean up existing database
	if err := client.DeleteDatabase(ctx, dbName); err != nil {
		// Ignore error if database doesn't exist
	}

	// Create new database
	if err := client.CreateDatabase(ctx, dbName); err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	db := client.GetDatabase(dbName)

	// Define schema
	schema := `
		define
		entity person, owns name, owns age;
		attribute name value string;
		attribute age value integer;
	`

	if _, err := db.ExecuteSchema(ctx, schema); err != nil {
		log.Fatalf("Failed to define schema: %v", err)
	}

	fmt.Println("✓ Database and schema created")
	fmt.Println()

	// ===== Demo 1: Dense Sequential Writes =====
	fmt.Println("--- Demo 1: Dense Sequential Writes (1000 inserts) ---")
	start := time.Now()

	for i := 0; i < 1000; i++ {
		query := fmt.Sprintf(`
			insert
			$p isa person, has name "Person_%d", has age %d;
		`, i, 20+i%50)

		if _, err := db.ExecuteWrite(ctx, query); err != nil {
			log.Printf("Warning: Insert %d failed: %v", i, err)
		}

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/1000 inserts completed\n", i+1)
		}
	}

	elapsed1 := time.Since(start)
	fmt.Printf("✓ Completed 1000 sequential writes in %v\n", elapsed1)
	fmt.Println()

	// ===== Demo 2: Dense Concurrent Writes =====
	fmt.Println("--- Demo 2: Dense Concurrent Writes (1000 inserts, 100 goroutines) ---")
	start = time.Now()

	var wg sync.WaitGroup
	errors := make(chan error, 1000)

	// Launch 100 goroutines, each doing 10 inserts
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < 10; i++ {
				insertID := goroutineID*10 + i + 1000 // Offset from previous demo
				query := fmt.Sprintf(`
					insert
					$p isa person, has name "Concurrent_%d", has age %d;
				`, insertID, 20+insertID%50)

				if _, err := db.ExecuteWrite(ctx, query); err != nil {
					errors <- fmt.Errorf("goroutine %d, insert %d failed: %w", goroutineID, i, err)
				}
			}
		}(g)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)

	// Count errors
	errorCount := 0
	for err := range errors {
		errorCount++
		if errorCount <= 5 {
			log.Printf("Error: %v", err)
		}
	}

	elapsed2 := time.Since(start)
	if errorCount > 0 {
		fmt.Printf("⚠ Completed with %d errors in %v\n", errorCount, elapsed2)
	} else {
		fmt.Printf("✓ Completed 1000 concurrent writes in %v\n", elapsed2)
	}
	fmt.Println()

	// ===== Verify Results =====
	fmt.Println("--- Verifying Results ---")
	countQuery := `
		match $p isa person;
		reduce $count = count;
	`

	result, err := db.ExecuteRead(ctx, countQuery)
	if err != nil {
		log.Fatalf("Failed to count persons: %v", err)
	}

	rows, _ := result.GetRowsRobust()
	if len(rows) > 0 {
		count, _ := rows[0].GetInt64("count")
		fmt.Printf("Total persons in database: %d\n", count)
		if count == 2000 {
			fmt.Println("✅ SUCCESS: All 2000 inserts completed without data loss!")
		} else {
			fmt.Printf("⚠️  WARNING: Expected 2000, got %d (%.1f%% success rate)\n",
				count, float64(count)/2000*100)
		}
	}

	fmt.Println()

	// ===== Get Pool Statistics =====
	if client.IsWorkerPoolEnabled() {
		queueLen, activeWorkers := client.GetWorkerPoolStats()
		fmt.Printf("Worker Pool Stats:\n")
		fmt.Printf("  Queue Length: %d\n", queueLen)
		fmt.Printf("  Active Workers: %d\n", activeWorkers)
	}

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
	fmt.Println()
	fmt.Println("Key Benefits of Worker Pool:")
	fmt.Println("  1. Prevents 'connection refused' errors during dense writes")
	fmt.Println("  2. Automatic flow control (queue buffers 100k+ requests)")
	fmt.Println("  3. Limits concurrent gRPC streams to safe levels (50 workers)")
	fmt.Println("  4. No code changes needed - just enable in Options")

	// Cleanup
	fmt.Println()
	if err := client.DeleteDatabase(ctx, dbName); err != nil {
		log.Printf("Warning: Failed to clean up database: %v", err)
	} else {
		fmt.Printf("✓ Cleaned up test database: %s\n", dbName)
	}
}
