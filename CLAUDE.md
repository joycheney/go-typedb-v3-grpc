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

5. **Robust Result Access (New)**:
   - Seamless automatic conversion between row and document formats
   - Users don't need to memorize which queries return rows vs documents
   - Intelligent error messages when conversion isn't possible

## Robust Query Result Handling

The library provides robust accessor methods that automatically handle result type conversion, making it easier to work with different query types without needing to remember whether a query returns rows or documents.

### Key Features

1. **Automatic Row ↔ Document Conversion**:
   - Row data can always be converted to documents (column names → field names)
   - Flat documents can be converted to rows (field names → column names)
   - Nested documents provide clear error messages with suggestions

2. **Seamless Developer Experience**:
   - Use `GetRowsRobust()` regardless of query type - it auto-converts if possible
   - Use `GetDocumentsRobust()` regardless of query type - it auto-converts if possible
   - Use `CountRobust()` to get result count without checking type flags

3. **Type-Safe with Helpful Errors**:
   - Nested structures that can't be flattened return descriptive errors
   - Error messages suggest the correct API or query modification

### Usage Examples

#### Example 1: Seamless Row Access (with auto-conversion)
```go
// Query returns documents (fetch query)
result, _ := db.ExecuteRead(ctx, `
    match $p isa person;
    fetch $p: name, age;
`)

// User calls row accessor (previously would get empty/zero)
// Now: automatically converts documents to rows if they're flat
rows, err := result.GetRowsRobust()
if err != nil {
    // Only fails if documents have nested structures
    log.Printf("Cannot convert to rows: %v", err)
} else {
    // Successfully converted! User gets the data they expected
    for _, row := range rows {
        name, _ := row.GetString("name")
        age, _ := row.GetInt64("age")
        fmt.Printf("%s: %d\n", name, age)
    }
}
```

#### Example 2: Seamless Document Access (always succeeds)
```go
// Query returns rows (match/select query)
result, _ := db.ExecuteRead(ctx, `
    match $p isa person, has name $n, has age $a;
    select $n, $a;
`)

// User calls document accessor
// Automatically converts rows to documents (always succeeds)
docs, err := result.GetDocumentsRobust()
// err is always nil for row→document conversion
for _, doc := range docs {
    name, _ := doc.GetString("n")
    age, _ := doc.GetInt64("a")
    fmt.Printf("%s: %d\n", name, age)
}
```

#### Example 3: Universal Counting
```go
// Works for any result type
count := result.CountRobust()  // No need to check IsRowStream/IsDocumentStream
fmt.Printf("Got %d results\n", count)
```

#### Example 4: Error Handling for Nested Structures
```go
// Query with nested data
result, _ := db.ExecuteRead(ctx, `
    match $p isa person;
    fetch $p: name, hobbies;  // hobbies is a list (nested)
`)

// Attempt conversion to rows
rows, err := result.GetRowsRobust()
if err != nil {
    // Clear error message:
    // "field 'hobbies' contains a list (nested structure).
    //  Use GetDocuments() or modify your fetch query to return flat data"

    // Fallback to documents (always works)
    docs, _ := result.GetDocumentsRobust()
    // Process nested structure
}
```

### Comparison: Traditional vs Robust APIs

| Scenario | Traditional API | Robust API |
|----------|----------------|------------|
| Match query, need documents | Check `IsRowStream`, manually convert | `GetDocumentsRobust()` - auto-converts |
| Fetch query, need rows | Check `IsDocumentStream`, get confused | `GetRowsRobust()` - auto-converts if flat |
| Just need count | `if IsRowStream { len(TypedRows) } else if IsDocumentStream { len(TypedDocuments) }` | `CountRobust()` |
| Wrong accessor called | Silent failure (empty/zero) | Automatic conversion or clear error |

### When Conversion Fails

Document → Row conversion fails only when documents contain:
- **Lists/Arrays**: Field contains nested list data
- **Relations with Links**: Concept has relational links
- **Inconsistent Fields**: Documents have different field sets
- **Unknown Types**: Field contains unrecognized type

Error messages always include:
- Which field caused the problem
- What type of nesting was detected
- Suggested fix (use `GetDocuments()` or modify query)

### Best Practices

1. **Use Robust APIs by default**: `GetRowsRobust()`, `GetDocumentsRobust()`, `CountRobust()`
2. **Handle errors gracefully**: Even robust APIs can fail on genuinely incompatible structures
3. **Trust auto-conversion**: Row→Document always succeeds; Document→Row succeeds for flat data
4. **Read error messages**: They provide actionable guidance when conversion fails

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
- ~/github.com/typedb-driver/ 中rust driver是完整实现的, 其他语言并不完整
  ~/github.com/typedb/ 中有typedb源代码