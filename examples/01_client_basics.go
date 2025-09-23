package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joycheney/go-typedb-v3-grpc/typedbclient"
)

// Example 1: Client Connection Management
// This example demonstrates the basic connection features of TypeDB v3 gRPC client, including:
// - Creating client with default configuration
// - Creating client with custom configuration
// - Configuring gRPC parameters (keepalive, message size, etc.)
// - Independent authentication token creation
// - Server discovery
// - Resource cleanup

func main() {
	fmt.Println("=== TypeDB v3 gRPC Client Basic Features Example ===")

	// Demo 1: Using default configuration
	demonstrateDefaultConfig()

	// Demo 2: Using custom configuration
	demonstrateCustomConfig()

	// Demo 3: Independent authentication token creation
	demonstrateTokenCreation()

	// Demo 4: Server discovery
	demonstrateServerDiscovery()

	// Demo 5: New API design features (Builder pattern and convenience functions)
	demonstrateNewAPIFeatures()

	fmt.Println("=== Client Basic Features Example Completed ===")
}

// demonstrateDefaultConfig demonstrates creating a client with default configuration
func demonstrateDefaultConfig() {
	fmt.Println("\n--- Demo 1: Default Configuration Client ---")

	// Get default configuration
	config := typedbclient.DefaultOptions()
	fmt.Printf("Default configuration:\n")
	fmt.Printf("  Address: %s\n", config.Address)
	fmt.Printf("  Username: %s\n", config.Username)
	fmt.Printf("  Keep-alive time: %v\n", config.KeepAliveTime)
	fmt.Printf("  Keep-alive timeout: %v\n", config.KeepAliveTimeout)
	fmt.Printf("  Max receive message size: %d bytes\n", config.MaxRecvMsgSize)
	fmt.Printf("  Max send message size: %d bytes\n", config.MaxSendMsgSize)

	// Try to create client (Note: TypeDB server needs to be running)
	client, err := typedbclient.NewClient(config)
	if err != nil {
		fmt.Printf("Failed to create client (Normal if TypeDB server is not running): %v\n", err)
		return
	}

	fmt.Println("✓ Default configuration client created successfully")

	// Clean up resources
	if err := client.Close(); err != nil {
		fmt.Printf("Failed to close client: %v\n", err)
	} else {
		fmt.Println("✓ Client safely closed")
	}
}

// demonstrateCustomConfig demonstrates creating a client with custom configuration
func demonstrateCustomConfig() {
	fmt.Println("\n--- Demo 2: Custom Configuration Client ---")

	// Create custom configuration
	config := &typedbclient.Options{
		Address:          "127.0.0.1:1729", // TypeDB v3 default port
		Username:         "admin",
		Password:         "password",
		KeepAliveTime:    60 * time.Second, // Longer keep-alive time
		KeepAliveTimeout: 15 * time.Second, // Longer keep-alive timeout
		MaxRecvMsgSize:   32 * 1024 * 1024, // 32MB receive message limit
		MaxSendMsgSize:   32 * 1024 * 1024, // 32MB send message limit
	}

	fmt.Printf("Custom configuration:\n")
	fmt.Printf("  Address: %s\n", config.Address)
	fmt.Printf("  Keep-alive time: %v\n", config.KeepAliveTime)
	fmt.Printf("  Keep-alive timeout: %v\n", config.KeepAliveTimeout)
	fmt.Printf("  Max receive message size: %d bytes\n", config.MaxRecvMsgSize)
	fmt.Printf("  Max send message size: %d bytes\n", config.MaxSendMsgSize)

	// Try to create client
	client, err := typedbclient.NewClient(config)
	if err != nil {
		fmt.Printf("Failed to create client (Normal if TypeDB server is not running): %v\n", err)
		return
	}

	fmt.Println("✓ Custom configuration client created successfully")

	// Clean up resources
	if err := client.Close(); err != nil {
		fmt.Printf("Failed to close client: %v\n", err)
	} else {
		fmt.Println("✓ Client safely closed")
	}
}

// demonstrateTokenCreation demonstrates independent authentication token creation
func demonstrateTokenCreation() {
	fmt.Println("\n--- Demo 3: Independent Authentication Token Creation ---")

	// Create client (using default configuration)
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Create context (with timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to create authentication token for different users
	username := "admin"
	password := "password"

	token, err := client.CreateAuthToken(ctx, username, password)
	if err != nil {
		fmt.Printf("Failed to create authentication token: %v\n", err)
		return
	}

	fmt.Printf("✓ Successfully created authentication token for user '%s'\n", username)
	fmt.Printf("Token length: %d characters\n", len(token))
	fmt.Printf("Token prefix: %s...\n", token[:min(20, len(token))])
}

// demonstrateServerDiscovery demonstrates server discovery functionality
func demonstrateServerDiscovery() {
	fmt.Println("\n--- Demo 4: Server Discovery ---")

	// Create client
	client, err := typedbclient.NewClient(nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get server list
	servers, err := client.ListServers(ctx)
	if err != nil {
		fmt.Printf("Failed to get server list: %v\n", err)
		return
	}

	fmt.Printf("✓ Discovered %d server(s):\n", len(servers))
	for i, server := range servers {
		fmt.Printf("  Server %d: %s\n", i+1, server.Address)
	}

	if len(servers) == 0 {
		fmt.Println("  (No servers discovered)")
	}
}

// demonstrateNewAPIFeatures demonstrates new API design features
func demonstrateNewAPIFeatures() {
	fmt.Println("\n--- Demo 5: New API Design Features ---")

	// Demonstrate convenience functions: direct connection to local server
	fmt.Println("1. Using convenience function to connect:")
	client1, err := typedbclient.ConnectLocal()
	if err != nil {
		fmt.Printf("   Local connection failed (Normal if TypeDB server is not running): %v\n", err)
	} else {
		fmt.Println("   ✓ Successfully connected using typedbclient.ConnectLocal()")
		defer client1.Close()
	}

	// Demonstrate convenience functions: connect to specified address
	fmt.Println("\n2. Using convenience function to connect to specified address:")
	client2, err := typedbclient.ConnectDefault("127.0.0.1:1729")
	if err != nil {
		fmt.Printf("   Connection failed (Normal): %v\n", err)
	} else {
		fmt.Println("   ✓ Successfully connected using typedbclient.ConnectDefault()")
		defer client2.Close()
	}

	// Demonstrate Options pattern: Go standard library convention
	fmt.Println("\n3. Using Options pattern (Go standard library convention):")
	opts := &typedbclient.Options{
		Address:          "127.0.0.1:1729",
		Username:         "admin",
		Password:         "password",
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
	}
	client3, err := typedbclient.NewClient(opts)
	if err != nil {
		fmt.Printf("   Options pattern connection failed (Normal): %v\n", err)
	} else {
		fmt.Println("   ✓ Successfully connected using Options pattern")
		defer client3.Close()
	}

	// Demonstrate explicit pointer type
	fmt.Println("\n4. Using explicit pointer type:")
	var client4 *typedbclient.Client  // Explicitly specify pointer type
	client4, err = typedbclient.ConnectLocal()
	if err != nil {
		fmt.Printf("   Explicit pointer type connection failed (Normal): %v\n", err)
	} else {
		fmt.Println("   ✓ Successfully used explicit *typedbclient.Client pointer type")
		defer client4.Close()
	}

	fmt.Println("\n✓ Advantages of new API design:")
	fmt.Println("   - Solves variable shadowing: Using typedbclient package name avoids naming conflicts")
	fmt.Println("   - Options pattern follows Go standard library conventions, easy to understand and use")
	fmt.Println("   - Convenience functions simplify common use cases")
	fmt.Println("   - Explicit pointer types avoid type semantic confusion")
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Running instructions:
// 1. Ensure TypeDB v3 server is running: docker run --name typedb -p 1729:1729 vaticle/typedb:3.0.0
// 2. Run this example: go run examples/01_client_basics.go
// 3. If TypeDB server is not running, some operations will fail, which is normal
//
// APIs covered in this example:
// - typedbclient.DefaultOptions()     // Get default configuration
// - typedbclient.NewClient(opts)      // Create client
// - client.Close()                    // Close client connection
// - client.CreateAuthToken()          // Create authentication token
// - client.ListServers()              // List servers

func init() {
	// Configure log format
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}