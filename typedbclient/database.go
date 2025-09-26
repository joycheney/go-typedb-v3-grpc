package typedbclient

import (
	"context"
	"fmt"

	pb "github.com/joycheney/go-typedb-v3-grpc/pb/proto"
)

// Database database operations interface
type Database struct {
	client *Client
	name   string
}

// GetDatabase gets database operations interface
func (c *Client) GetDatabase(name string) *Database {
	// Ensure database name follows rules (consistent with HTTP client)
	name = ensureValidDatabaseName(name)

	return &Database{
		client: c,
		name:   name,
	}
}

// ensureValidDatabaseName ensures database name follows rules
func ensureValidDatabaseName(name string) string {
	// Database name must start with a letter
	if len(name) > 0 && (name[0] >= '0' && name[0] <= '9') {
		name = "db_" + name
	}

	// Replace illegal characters
	validName := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if (ch >= 'a' && ch <= 'z') ||
		   (ch >= 'A' && ch <= 'Z') ||
		   (ch >= '0' && ch <= '9') ||
		   ch == '_' {
			validName = append(validName, ch)
		} else if ch == '-' {
			validName = append(validName, '_')
		}
	}

	name = string(validName)

	// Limit length
	if len(name) > 63 {
		name = name[:63]
	}

	return name
}

// DatabaseExists checks if database exists
func (c *Client) DatabaseExists(ctx context.Context, name string) (bool, error) {
	name = ensureValidDatabaseName(name)

	// Use executeWithRetry to execute operation
	var exists bool
	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.DatabaseManager_Contains_Req{
			Name: name,
		}

		resp, err := client.DatabasesContains(ctx, req)
		if err != nil {
			return err
		}

		exists = resp.Contains
		return nil
	})

	return exists, err
}

// CreateDatabase creates database
func (c *Client) CreateDatabase(ctx context.Context, name string) error {
	name = ensureValidDatabaseName(name)

	return c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.DatabaseManager_Create_Req{
			Name: name,
		}

		_, err := client.DatabasesCreate(ctx, req)
		return err
	})
}

// DeleteDatabase deletes database
func (c *Client) DeleteDatabase(ctx context.Context, name string) error {
	name = ensureValidDatabaseName(name)

	return c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.Database_Delete_Req{
			Name: name,
		}

		_, err := client.DatabaseDelete(ctx, req)
		return err
	})
}

// ListDatabases lists all databases
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	var databases []string

	err := c.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := c.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		req := &pb.DatabaseManager_All_Req{}

		resp, err := client.DatabasesAll(ctx, req)
		if err != nil {
			return err
		}

		// Extract database names from DatabaseReplicas
		databases = make([]string, 0, len(resp.Databases))
		for _, db := range resp.Databases {
			databases = append(databases, db.Name)
		}
		return nil
	})

	return databases, err
}

// GetSchema gets database schema
func (db *Database) GetSchema(ctx context.Context) (string, error) {
	var schema string

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := db.client.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		req := &pb.Database_Schema_Req{
			Name: db.name,
		}

		resp, err := client.DatabaseSchema(ctx, req)
		if err != nil {
			return err
		}

		schema = resp.Schema
		return nil
	})

	return schema, err
}

// GetTypeSchema gets database type schema
func (db *Database) GetTypeSchema(ctx context.Context) (string, error) {
	var schema string

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Get current connection atomically
		connRef := db.client.connRef.Load()
		if connRef == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		wrapper := connRef.(*connWrapper)
		client := wrapper.grpcClient

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

		req := &pb.Database_TypeSchema_Req{
			Name: db.name,
		}

		resp, err := client.DatabaseTypeSchema(ctx, req)
		if err != nil {
			return err
		}

		schema = resp.Schema
		return nil
	})

	return schema, err
}

// ExecuteRead convenience interface for executing read-only queries (automatic transaction management)
func (db *Database) ExecuteRead(ctx context.Context, query string) (*QueryResult, error) {
	var result *QueryResult

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Begin read-only transaction
		tx, err := db.beginTransaction(ctx, Read)
		if err != nil {
			return fmt.Errorf("failed to begin read transaction: %w", err)
		}

		// Create bundle - ExecuteBundle will automatically add close
		bundle := []BundleOperation{
			{Type: OpExecute, Query: query},
		}

		// Execute bundle atomically (close will be added automatically)
		results, err := tx.executeBundle(ctx, bundle)
		if err != nil {
			return fmt.Errorf("failed to execute bundle: %w", err)
		}

		// Extract query result
		if len(results) > 0 {
			result = results[0]
		}
		return nil
	})

	return result, err
}

// ExecuteWrite convenience interface for executing write queries (automatic transaction management and commit)
func (db *Database) ExecuteWrite(ctx context.Context, query string) (*QueryResult, error) {
	var result *QueryResult

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Begin write transaction
		tx, err := db.beginTransaction(ctx, Write)
		if err != nil {
			return fmt.Errorf("failed to begin write transaction: %w", err)
		}

		// Create bundle - ExecuteBundle will automatically add commit and close
		bundle := []BundleOperation{
			{Type: OpExecute, Query: query},
		}

		// Execute bundle atomically (commit and close will be added automatically)
		results, err := tx.executeBundle(ctx, bundle)
		if err != nil {
			return fmt.Errorf("failed to execute bundle: %w", err)
		}

		// Extract query result
		if len(results) > 0 {
			result = results[0]
		}
		return nil
	})

	return result, err
}

// ExecuteSchema convenience interface for executing schema queries (automatic transaction management and commit)
func (db *Database) ExecuteSchema(ctx context.Context, query string) (*QueryResult, error) {
	var result *QueryResult

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Begin schema transaction
		tx, err := db.beginTransaction(ctx, Schema)
		if err != nil {
			return fmt.Errorf("failed to begin schema transaction: %w", err)
		}

		// Create bundle - ExecuteBundle will automatically add commit and close
		bundle := []BundleOperation{
			{Type: OpExecute, Query: query},
		}

		// Execute bundle atomically (commit and close will be added automatically)
		results, err := tx.executeBundle(ctx, bundle)
		if err != nil {
			return fmt.Errorf("failed to execute bundle: %w", err)
		}

		// Extract query result
		if len(results) > 0 {
			result = results[0]
		}
		return nil
	})

	return result, err
}

// ExecuteBundle executes a complete bundle with automatic transaction management
// This is the simplest API - creates transaction, executes bundle, and closes automatically
// The bundle will be automatically completed with OpOpen (if needed), OpCommit, and OpClose
func (db *Database) ExecuteBundle(ctx context.Context, txType TransactionType, operations []BundleOperation) ([]*QueryResult, error) {
	var results []*QueryResult

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Begin transaction (delayed initialization)
		tx, err := db.beginTransaction(ctx, txType)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// ExecuteBundle will automatically add OpOpen (if needed), OpCommit and OpClose
		results, err = tx.executeBundle(ctx, operations)
		if err != nil {
			return fmt.Errorf("failed to execute bundle: %w", err)
		}

		return nil
	})

	return results, err
}