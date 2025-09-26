# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go client library for TypeDB v3 using gRPC protocol. Provides simplified APIs for database operations, transaction management, and TypeQL query execution with enterprise-grade connection pooling and error handling.

## Essential Commands

### Build and Test
```bash
# Run examples (TypeDB server must be running on localhost:1729)
go run examples/01_client_basics.go
go run examples/02_database_management.go
# ... through 09_cleanup.go

# Run all tests
go test ./...

# Build the library
go build ./...

# Generate protobuf files (if needed)
./generate_proto.sh
```

### Quick Start Usage
```go
import "github.com/joycheney/go-typedb-v3-grpc/typedbclient"

// Connect with default options
config := typedbclient.DefaultOptions()
client, _ := typedbclient.NewClient(config)
defer client.Close()

// Or with custom options
client, _ := typedbclient.NewClient(&typedbclient.Options{
    Address:  "192.168.1.100:1729",
    Username: "admin",
    Password: "password",
    KeepAliveTime:    30 * time.Second,
    KeepAliveTimeout: 10 * time.Second,
    MaxRecvMsgSize:   16 * 1024 * 1024,  // 16MB
    MaxSendMsgSize:   16 * 1024 * 1024,  // 16MB
})
```

## Architecture Overview

### Package Structure
```
go-typedb-v3-grpc/
├── typedbclient/      # Main client package - simplified APIs
│   ├── client.go      # Client connection management
│   ├── database.go    # Database operations wrapper
│   ├── transaction.go # Transaction lifecycle management
│   └── result.go      # Result processing utilities
├── client/            # Low-level gRPC client implementation
│   ├── client.go      # Raw gRPC connection handling
│   ├── database.go    # Database-level operations
│   └── transaction.go # Transaction protocol implementation
├── pb/                # Generated protobuf code
└── examples/          # Comprehensive usage examples
```

### Key Design Patterns

1. **Two-Layer Client Architecture**:
   - `typedbclient/`: High-level simplified APIs with convenience methods
   - `client/`: Low-level gRPC protocol implementation

2. **Connection Management**:
   - Automatic reconnection on network failures
   - Connection pooling for concurrent operations
   - Graceful shutdown with resource cleanup

3. **Transaction Handling**:
   - Convenience methods: `ExecuteRead()`, `ExecuteWrite()` for simple operations
   - Full control: `BeginRead()`, `BeginWrite()` for complex transactions
   - Automatic resource cleanup with defer patterns

4. **Result Processing**:
   - Streaming results with `RowIterator` and `DocumentIterator`
   - Automatic type mapping from TypeDB to Go types
   - Memory-efficient processing of large result sets

## TypeQL v3 Syntax Reference

For TypeQL v3 syntax and query patterns, refer to:
- **📚 Complete Syntax Guide**: [`../docs/TYPEQL_V3_SYNTAX_GUIDE.md`](../docs/TYPEQL_V3_SYNTAX_GUIDE.md)

Key TypeQL v3 changes from v2:
- Schema definition: `entity person` instead of `person sub entity`
- Query results: `select` instead of `get`
- Aggregations: `reduce` clause instead of inline aggregates
- Relations: `links` keyword for connecting role players

## Error Handling Patterns

The client provides structured error types for different failure scenarios:
- Connection errors: Network issues, authentication failures
- Transaction errors: Conflicts, invalid operations
- Query errors: Syntax errors, schema violations
- Server errors: Internal TypeDB server issues

Example error handling:
```go
result, err := db.ExecuteRead(ctx, query)
if err != nil {
    if errors.Is(err, typedbclient.ErrConnection) {
        // Handle connection error
    } else if errors.Is(err, typedbclient.ErrTransaction) {
        // Handle transaction error
    }
}
```

## Environment Configuration

The client respects these environment variables:
- `TYPEDB_ADDRESS`: Server address (default: "localhost:1729")
- `TYPEDB_USERNAME`: Authentication username (default: "admin")
- `TYPEDB_PASSWORD`: Authentication password (default: "password")

## Development Tips

1. **Start with examples**: The `examples/` directory contains working code for all common scenarios
2. **Use convenience methods**: For simple queries, use `ExecuteRead()`/`ExecuteWrite()`
3. **Handle streaming results**: Always close iterators to free resources
4. **Check TypeDB server logs**: For debugging connection or query issues
5. **Version compatibility**: This client works with TypeDB v3.0+, tested with v3.4.4 and v3.5.0

## Common Tasks

### Create and query a database
```go
// Create database
client.CreateDatabase(ctx, "my_database")

// Get database handle
db := client.GetDatabase("my_database")

// Define schema
db.ExecuteWrite(ctx, `
    define
    entity person, owns name, owns age;
    attribute name value string;
    attribute age value integer;
`)

// Insert data
db.ExecuteWrite(ctx, `
    insert
    $p isa person, has name "Alice", has age 30;
`)

// Query data
result, _ := db.ExecuteRead(ctx, `
    match $p isa person, has name $n;
    select $p, $n;
`)
```

### Process query results
```go
// Row-based results
rows, _ := db.QueryRows(ctx, query)
defer rows.Close()

for rows.Next() {
    row := rows.Current()
    // Process row data
}

// Document-based results (JSON-like)
docs, _ := db.QueryDocuments(ctx, fetchQuery)
defer docs.Close()

for docs.Next() {
    doc := docs.Current()
    // Process document
}
```

## Testing Approach

- Unit tests: Located alongside implementation files
- Integration tests: In `examples/` directory
- Manual testing: Use numbered examples (01-09) for end-to-end validation
- TypeDB server required: All tests need a running TypeDB v3 instance

## TypeDB Version Compatibility

| TypeDB Version | Compatibility | Notes |
|----------------|---------------|-------|
| v3.0.x - v3.x.x | ✅ Fully Supported | All features work |
| v2.x | ❌ Not Supported | Use TypeDB v2 client |