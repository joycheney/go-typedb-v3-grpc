package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 6: Error Handling and Retry Mechanisms
// This example demonstrates the error handling mechanisms of TypeDB v3 gRPC client, including:
// - Connection error handling and automatic retry
// - Authentication error handling and token refresh
// - Syntax errors and business error handling
// - Timeout errors and resource cleanup
// - Transaction error handling and rollback mechanisms
// - Error classification and recovery strategies
// - Network interruption and recovery handling

func main() {
	fmt.Println("=== TypeDB v3 gRPC Error Handling Mechanisms Example ===")

	// Demo 1: Client connection error handling
	demonstrateConnectionErrors()

	// Demo 2: Authentication errors and token refresh
	demonstrateAuthenticationErrors()

	// Demo 3: Query syntax error handling
	demonstrateQuerySyntaxErrors()

	// Demo 4: Timeout error handling
	demonstrateTimeoutErrors()

	// Demo 5: Transaction error handling
	demonstrateTransactionErrors()

	// Demo 6: Network errors and retry mechanisms
	demonstrateNetworkErrorsAndRetries()

	// Demo 7: Error classification and best practices
	demonstrateErrorClassification()

	fmt.Println("=== Error Handling Mechanisms Example Completed ===")
}

// demonstrateConnectionErrors demonstrates connection error handling
func demonstrateConnectionErrors() {
	fmt.Printf("\n--- Demo 1: Connection Error Handling ---\n")

	// Test 1: Invalid server address
	fmt.Println("1. Testing invalid server address:")
	// Note: Since typedbclient.Config is not exported, we'll demonstrate connection errors
	// by attempting to connect to an invalid server using environment variables
	originalAddress := os.Getenv("TYPEDB_ADDRESS")
	os.Setenv("TYPEDB_ADDRESS", "invalid-server:9999")

	testClient, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("   ✓ Connection error correctly captured: %v\n", err)
		fmt.Printf("   Error type: %s\n", categorizeError(err))
		fmt.Printf("   Suggested action: %s\n", getSuggestion(err))
	} else {
		fmt.Println("   Unexpected: connection to invalid server succeeded")
		defer testClient.Close()
	}

	// Test 2: Network unreachable
	fmt.Println("\n2. Testing network unreachable:")
	// Restore original address and test with unreachable address
	os.Setenv("TYPEDB_ADDRESS", originalAddress)
	os.Setenv("TYPEDB_ADDRESS", "192.0.2.1:1729") // RFC5737 test address

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	testClient2, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("   ✓ Network error correctly captured: %v\n", err)
		fmt.Printf("   Error type: %s\n", categorizeError(err))
	} else {
		// Even if connection succeeds, try operation to trigger error
		_, listErr := testClient2.ListDatabases(ctx)
		if listErr != nil {
			fmt.Printf("   ✓ Network error captured during operation: %v\n", listErr)
		}
		defer testClient2.Close()
	}

	// Test 3: Normal connection comparison
	fmt.Println("\n3. Normal connection comparison:")
	// Restore original server address for normal connection test
	os.Setenv("TYPEDB_ADDRESS", originalAddress)
	testNormalClient, err := typedbclient.NewClient(nil) // Use default config
	if err != nil {
		fmt.Printf("   Failed to connect to default server: %v\n", err)
		fmt.Printf("   Hint: Ensure TypeDB v3 server is running on default port\n")
	} else {
		fmt.Printf("   ✓ Normal connection successful\n")
		defer testNormalClient.Close()
	}
}

// demonstrateAuthenticationErrors demonstrates authentication errors and token refresh
func demonstrateAuthenticationErrors() {
	fmt.Printf("\n--- Demo 2: Authentication Errors and Token Refresh ---\n")

	// Create normal client
	authClient, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer authClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test 1: Incorrect username/password
	fmt.Println("1. Testing incorrect authentication credentials:")
	_, tokenErr := authClient.CreateAuthToken(ctx, "invalid_user", "wrong_password")
	if tokenErr != nil {
		fmt.Printf("   ✓ Authentication error correctly captured: %v\n", tokenErr)
		fmt.Printf("   Error type: %s\n", categorizeError(tokenErr))
		fmt.Printf("   Is authentication error: %v\n", isAuthenticationError(tokenErr))
	}

	// Test 2: Normal authentication comparison
	fmt.Println("\n2. Normal authentication comparison:")
	token, tokenErr := authClient.CreateAuthToken(ctx, "admin", "password")
	if tokenErr != nil {
		fmt.Printf("   Normal authentication also failed: %v\n", tokenErr)
		fmt.Printf("   Possible cause: TypeDB server not running or configuration error\n")
	} else {
		fmt.Printf("   ✓ Normal authentication successful, token length: %d\n", len(token))
	}

	// Test 3: Token refresh mechanism demonstration
	fmt.Println("\n3. Token refresh mechanism:")
	fmt.Printf("   - Client automatically attempts to refresh token on authentication errors\n")
	fmt.Printf("   - Returns authentication error to user after refresh failure\n")
	fmt.Printf("   - Users can manually create new token through CreateAuthToken\n")
}

// demonstrateQuerySyntaxErrors demonstrates query syntax error handling
func demonstrateQuerySyntaxErrors() {
	fmt.Printf("\n--- Demo 3: Query Syntax Error Handling ---\n")

	queryClient, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer queryClient.Close()

	// Create test database
	testDbName := "error_handling_demo"
	ctx := context.Background()

	if err := ensureTestDatabase(ctx, queryClient, testDbName); err != nil {
		fmt.Printf("Failed to prepare test database: %v\n", err)
		return
	}
	defer cleanupTestDatabase(ctx, queryClient, testDbName)

	database := queryClient.GetDatabase(testDbName)

	// Test various syntax errors
	syntaxErrorTests := []struct {
		name  string
		query string
		desc  string
	}{
		{
			"Invalid keywords",
			"invalid query syntax here",
			"Completely invalid query syntax",
		},
		{
			"Incomplete match statement",
			"match $x",
			"Missing query conditions",
		},
		{
			"Incorrect get syntax",
			"match $x sub entity; select $y;",
			"Referencing undefined variable in select (GET is deprecated, use SELECT)",
		},
		{
			"Mismatched brackets",
			"match $x sub entity {",
			"Brackets don't match",
		},
		{
			"Invalid insert syntax",
			"insert $x isa nonexistent_type;",
			"Inserting non-existent type",
		},
	}

	fmt.Println("Syntax error tests:")
	for i, test := range syntaxErrorTests {
		fmt.Printf("\n%d. %s (%s):\n", i+1, test.name, test.desc)
		fmt.Printf("   Query: %s\n", test.query)

		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := database.ExecuteRead(queryCtx, test.query)
		cancel()

		if err != nil {
			fmt.Printf("   ✓ Syntax error correctly captured: %v\n", err)
			fmt.Printf("   Error type: %s\n", categorizeError(err))
			fmt.Printf("   Suggestion: %s\n", getSuggestion(err))
		} else {
			fmt.Printf("   Unexpected: syntax error was not captured\n")
		}
	}

	// Demonstrate correct SELECT syntax as replacement for deprecated GET
	fmt.Println("\nCorrect SELECT syntax verification:")
	// First define a simple schema for testing
	_, err = database.ExecuteSchema(context.Background(), "define entity test_entity, owns test_name; attribute test_name, value string;")
	if err != nil {
		fmt.Printf("   Schema setup failed: %v\n", err)
	} else {
		// Test correct SELECT syntax
		correctQuery := "match $x isa test_entity; select $x; limit 1;"
		queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = database.ExecuteRead(queryCtx, correctQuery)
		cancel()

		if err != nil {
			fmt.Printf("   SELECT syntax test failed: %v\n", err)
		} else {
			fmt.Printf("   ✓ SELECT syntax verified: '%s' executed successfully\n", correctQuery)
			fmt.Printf("   Note: SELECT replaces deprecated GET keyword in TypeDB v3\n")
		}
	}
}

// demonstrateTimeoutErrors demonstrates timeout error handling
func demonstrateTimeoutErrors() {
	fmt.Printf("\n--- Demo 4: Timeout Error Handling ---\n")

	timeoutClient, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer timeoutClient.Close()

	// Test different timeout scenarios
	timeoutTests := []struct {
		name    string
		timeout time.Duration
		desc    string
	}{
		{"Very short timeout", 1 * time.Millisecond, "Simulate network delay timeout"},
		{"Short timeout", 10 * time.Millisecond, "Simulate operation timeout"},
		{"Normal timeout", 5 * time.Second, "Normal operation time"},
	}

	fmt.Println("Timeout error tests:")
	for i, test := range timeoutTests {
		fmt.Printf("\n%d. %s (%s):\n", i+1, test.name, test.desc)
		fmt.Printf("   Timeout setting: %v\n", test.timeout)

		ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
		_, err := timeoutClient.ListDatabases(ctx)
		cancel()

		if err != nil {
			if isTimeoutError(err) {
				fmt.Printf("   ✓ Timeout error correctly handled: %v\n", err)
			} else {
				fmt.Printf("   Other error: %v\n", err)
			}
			fmt.Printf("   Error type: %s\n", categorizeError(err))
		} else {
			fmt.Printf("   ✓ Operation completed within time limit\n")
		}
	}

	// Demonstrate timeout handling best practices
	fmt.Println("\nTimeout handling best practices:")
	fmt.Printf("1. Set reasonable timeout for each operation\n")
	fmt.Printf("2. Distinguish between network timeout and operation timeout\n")
	fmt.Printf("3. Provide cleanup mechanism after timeout\n")
	fmt.Printf("4. Use longer timeout for long-running operations\n")
}

// demonstrateTransactionErrors demonstrates transaction error handling
func demonstrateTransactionErrors() {
	fmt.Printf("\n--- Demo 5: Transaction Error Handling ---\n")

	txClient, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer txClient.Close()

	// Create test database
	testDbName := "transaction_error_demo"
	ctx := context.Background()

	if err := ensureTestDatabase(ctx, txClient, testDbName); err != nil {
		fmt.Printf("Failed to prepare test database: %v\n", err)
		return
	}
	defer cleanupTestDatabase(ctx, txClient, testDbName)

	database := txClient.GetDatabase(testDbName)

	// Test 1: Transaction conflict handling
	fmt.Println("1. Transaction error handling demonstration:")

	// First set up basic schema
	schemaQuery := `
		define
		person sub entity,
			owns name;
		name sub attribute, value string;
	`

	schemaCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, schemaErr := database.ExecuteSchema(schemaCtx, schemaQuery)
	cancel()

	if schemaErr != nil {
		fmt.Printf("   Schema definition error: %v\n", schemaErr)
		return
	}

	// Test various errors in write transactions
	writeTests := []struct {
		name  string
		query string
		desc  string
	}{
		{
			"Inserting non-existent type",
			"insert $x isa nonexistent_type;",
			"Attempting to insert non-existent entity type",
		},
		{
			"Violating schema constraints",
			"insert $p isa person, has name 123;",
			"Attempting to set numeric value for string attribute",
		},
		{
			"Invalid relationship insertion",
			"insert $r (invalid_role: $p) isa nonexistent_relation;",
			"Inserting non-existent relationship type",
		},
	}

	for i, test := range writeTests {
		fmt.Printf("\n   %d.%d %s:\n", 1, i+1, test.name)
		fmt.Printf("      Query: %s\n", test.query)

		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := database.ExecuteWrite(writeCtx, test.query)
		cancel()

		if err != nil {
			fmt.Printf("      ✓ Transaction error correctly captured: %v\n", err)
			fmt.Printf("      Error type: %s\n", categorizeError(err))
		} else {
			fmt.Printf("      Unexpected: error operation was not captured\n")
		}
	}

	// Test 2: Error handling in manual transaction management
	fmt.Println("\n2. Manual transaction management error handling:")

	txCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := database.BeginTransaction(txCtx, typedbclient.Write)
	if err != nil {
		fmt.Printf("   Failed to create transaction: %v\n", err)
		return
	}

	// Execute a bundle with an operation that will fail
	bundle := []typedbclient.BundleOperation{
		{Type: typedbclient.OpExecute, Query: "insert $x isa nonexistent_type;"},
		{Type: typedbclient.OpCommit}, // Won't reach here due to error
		{Type: typedbclient.OpClose},
	}

	_, execErr := tx.ExecuteBundle(txCtx, bundle)
	if execErr != nil {
		fmt.Printf("   ✓ Operation error in bundle: %v\n", execErr)
		// Bundle automatically handles rollback on error
		fmt.Printf("   ✓ Bundle automatically rolled back on error\n")
	} else {
		fmt.Printf("   Unexpected: error operation executed successfully\n")
	}
}

// demonstrateNetworkErrorsAndRetries demonstrates network errors and retry mechanisms
func demonstrateNetworkErrorsAndRetries() {
	fmt.Printf("\n--- Demo 6: Network Errors and Retry Mechanisms ---\n")

	fmt.Println("1. Retry mechanism explanation:")
	fmt.Printf("   - Client has built-in automatic retry for authentication errors\n")
	fmt.Printf("   - Connection errors trigger reconnection mechanism\n")
	fmt.Printf("   - Users can implement custom retry at application layer\n")

	// Demonstrate custom retry logic
	fmt.Println("\n2. Custom retry logic example:")
	retryClient, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer retryClient.Close()

	// Simulate operation with retry
	maxRetries := 3
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("   Attempt %d/%d: ", attempt, maxRetries)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := retryClient.ListDatabases(ctx)
		cancel()

		if err == nil {
			fmt.Printf("✓ Operation successful\n")
			break
		}

		lastErr = err
		fmt.Printf("Failed - %v\n", err)

		if attempt < maxRetries {
			waitTime := time.Duration(attempt) * time.Second
			fmt.Printf("      Waiting %v before retry...\n", waitTime)
			time.Sleep(waitTime)
		}
	}

	if lastErr != nil {
		fmt.Printf("   Final failure: %v\n", lastErr)
		fmt.Printf("   Error type: %s\n", categorizeError(lastErr))
	}

	fmt.Println("\n3. Retry best practices:")
	fmt.Printf("   - Exponential backoff: gradually increase retry intervals\n")
	fmt.Printf("   - Maximum retry count: avoid infinite retries\n")
	fmt.Printf("   - Error classification: only retry for recoverable errors\n")
	fmt.Printf("   - Timeout control: set timeout for each retry\n")
}

// demonstrateErrorClassification demonstrates error classification and best practices
func demonstrateErrorClassification() {
	fmt.Printf("\n--- Demo 7: Error Classification and Best Practices ---\n")

	fmt.Println("1. Error classification system:")
	fmt.Printf("   Connection Error (Connection)  - Network, server unreachable\n")
	fmt.Printf("   Authentication Error (Authentication) - Username, password, token\n")
	fmt.Printf("   Syntax Error (Syntax)      - Incorrect query syntax\n")
	fmt.Printf("   Semantic Error (Semantic)    - Schema mismatch, type errors\n")
	fmt.Printf("   Resource Error (Resource)    - Memory, disk, concurrency limits\n")
	fmt.Printf("   Timeout Error (Timeout)     - Operation timeout\n")
	fmt.Printf("   System Error (System)      - Server internal errors\n")

	fmt.Println("\n2. Error handling strategies:")
	errorStrategies := map[string]string{
		"Connection Error":     "Retry connection, check network and server status",
		"Authentication Error": "Refresh token, verify credentials",
		"Syntax Error":         "Fix query syntax, should not retry",
		"Semantic Error":       "Check schema, fix business logic",
		"Resource Error":       "Wait for resource release or optimize query",
		"Timeout Error":        "Increase timeout duration or decompose operation",
		"System Error":         "Retry later, contact administrator",
	}

	for errType, strategy := range errorStrategies {
		fmt.Printf("   %s: %s\n", errType, strategy)
	}

	fmt.Println("\n3. Error handling best practices:")
	fmt.Printf("   ✓ Always check error return values\n")
	fmt.Printf("   ✓ Use context for timeout control\n")
	fmt.Printf("   ✓ Set reasonable retry strategies\n")
	fmt.Printf("   ✓ Log detailed error information\n")
	fmt.Printf("   ✓ Provide user-friendly error messages\n")
	fmt.Printf("   ✓ Distinguish between recoverable and non-recoverable errors\n")
	fmt.Printf("   ✓ Clean up resources promptly (use defer)\n")

	fmt.Println("\n4. Error monitoring recommendations:")
	fmt.Printf("   - Monitor error rates and error patterns\n")
	fmt.Printf("   - Set up alerts for critical errors\n")
	fmt.Printf("   - Regularly analyze error logs\n")
	fmt.Printf("   - Establish error handling documentation\n")
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

// categorizeError categorizes errors
func categorizeError(err error) string {
	if err == nil {
		return "No error"
	}

	errMsg := strings.ToLower(err.Error())

	// Connection-related errors
	if strings.Contains(errMsg, "connection") ||
		strings.Contains(errMsg, "connect") ||
		strings.Contains(errMsg, "dial") ||
		strings.Contains(errMsg, "network") {
		return "Connection Error (Connection)"
	}

	// Authentication-related errors
	if strings.Contains(errMsg, "auth") ||
		strings.Contains(errMsg, "token") ||
		strings.Contains(errMsg, "credential") ||
		strings.Contains(errMsg, "unauthorized") {
		return "Authentication Error (Authentication)"
	}

	// Timeout-related errors
	if strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "deadline") ||
		strings.Contains(errMsg, "context canceled") {
		return "Timeout Error (Timeout)"
	}

	// Syntax-related errors
	if strings.Contains(errMsg, "syntax") ||
		strings.Contains(errMsg, "parse") ||
		strings.Contains(errMsg, "invalid query") {
		return "Syntax Error (Syntax)"
	}

	// Schema-related errors
	if strings.Contains(errMsg, "schema") ||
		strings.Contains(errMsg, "type") ||
		strings.Contains(errMsg, "constraint") {
		return "Semantic Error (Semantic)"
	}

	// Resource-related errors
	if strings.Contains(errMsg, "resource") ||
		strings.Contains(errMsg, "memory") ||
		strings.Contains(errMsg, "limit") {
		return "Resource Error (Resource)"
	}

	return "System Error (System)"
}

// getSuggestion gets error handling suggestions
func getSuggestion(err error) string {
	category := categorizeError(err)

	suggestions := map[string]string{
		"Connection Error (Connection)":         "Check network connection and server status, consider retry",
		"Authentication Error (Authentication)": "Verify username and password, check token validity",
		"Timeout Error (Timeout)":               "Increase timeout duration or optimize query performance",
		"Syntax Error (Syntax)":                 "Check query syntax, refer to documentation for correction",
		"Semantic Error (Semantic)":             "Check data model and constraint conditions",
		"Resource Error (Resource)":             "Optimize query or wait for resource release",
		"System Error (System)":                 "Contact system administrator, check server logs",
	}

	if suggestion, exists := suggestions[category]; exists {
		return suggestion
	}
	return "Check detailed error information, contact technical support if necessary"
}

// isAuthenticationError checks if error is authentication error
func isAuthenticationError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "auth") ||
		strings.Contains(errMsg, "token") ||
		strings.Contains(errMsg, "credential") ||
		strings.Contains(errMsg, "unauthorized")
}

// isTimeoutError checks if error is timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "deadline") ||
		strings.Contains(errMsg, "context canceled")
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running
// 2. Run this example: go run examples/06_error_handling.go
// 3. Observe handling mechanisms for various error scenarios
//
// Error handling covered in this example:
// - typedbclient.NewClient() connection errors
// - typedbclient.CreateAuthToken() authentication errors
// - database.ExecuteRead/Write/Schema() query errors
// - context.WithTimeout() timeout errors
// - transaction.Execute() transaction errors
// - Custom retry logic and error classification
//
// Error handling principles:
// 1. Error classification: distinguish different types of errors
// 2. Graceful degradation: provide alternative solutions
// 3. Fail fast: return immediately for non-recoverable errors
// 4. Smart retry: retry for recoverable errors
// 5. Resource cleanup: ensure resources are properly released
// 6. User friendly: provide clear error information

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
