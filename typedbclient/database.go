package typedbclient

import (
	"context"
	"fmt"

	pb "github.com/mcp-software-think-execute-server/go-typedb-v3-grpc/pb/proto"
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
		c.connMu.RLock()
		client := c.grpcClient
		c.connMu.RUnlock()

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

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
		c.connMu.RLock()
		client := c.grpcClient
		c.connMu.RUnlock()

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

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
		c.connMu.RLock()
		client := c.grpcClient
		c.connMu.RUnlock()

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

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
		c.connMu.RLock()
		client := c.grpcClient
		c.connMu.RUnlock()

		if client == nil {
			return fmt.Errorf("gRPC client not initialized")
		}

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
		db.client.connMu.RLock()
		client := db.client.grpcClient
		db.client.connMu.RUnlock()

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
		db.client.connMu.RLock()
		client := db.client.grpcClient
		db.client.connMu.RUnlock()

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
		tx, err := db.BeginTransaction(ctx, Read)
		if err != nil {
			return fmt.Errorf("failed to begin read transaction: %w", err)
		}

		// Ensure transaction is closed (cleanup even if error occurs)
		defer func() {
			if closeErr := tx.Close(ctx); closeErr != nil {
				// Log close error but don't affect main result
				// Should use appropriate logging in production environment
			}
		}()

		// Execute query
		queryResult, err := tx.Execute(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		result = queryResult
		return nil
	})

	return result, err
}

// ExecuteWrite convenience interface for executing write queries (automatic transaction management and commit)
func (db *Database) ExecuteWrite(ctx context.Context, query string) (*QueryResult, error) {
	var result *QueryResult

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Begin write transaction
		tx, err := db.BeginTransaction(ctx, Write)
		if err != nil {
			return fmt.Errorf("failed to begin write transaction: %w", err)
		}

		// Ensure transaction is cleaned up (commit or rollback)
		defer func() {
			if closeErr := tx.Close(ctx); closeErr != nil {
				// Log close error but don't affect main result
			}
		}()

		// Execute query
		queryResult, err := tx.Execute(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		// Commit transaction to persist changes
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		result = queryResult
		return nil
	})

	return result, err
}

// ExecuteSchema convenience interface for executing schema queries (automatic transaction management and commit)
func (db *Database) ExecuteSchema(ctx context.Context, query string) (*QueryResult, error) {
	var result *QueryResult

	err := db.client.executeWithRetry(ctx, func(ctx context.Context) error {
		// Begin schema transaction
		tx, err := db.BeginTransaction(ctx, Schema)
		if err != nil {
			return fmt.Errorf("failed to begin schema transaction: %w", err)
		}

		// Ensure transaction is cleaned up (commit or rollback)
		defer func() {
			if closeErr := tx.Close(ctx); closeErr != nil {
				// Log close error but don't affect main result
			}
		}()

		// Execute query
		queryResult, err := tx.Execute(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		// Commit transaction to persist schema changes
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		result = queryResult
		return nil
	})

	return result, err
}