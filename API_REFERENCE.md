# TypeDB v3 Go Client - API Reference

## Core Types

### Client
```go
type Client struct {
    // Lock-free connection management
}

// Creation
func NewClient(opts *Options) (*Client, error)
func ConnectLocal() (*Client, error)                    // 127.0.0.1:1729, admin, password
func ConnectDefault(address string) (*Client, error)   // Custom address, admin, password
func Connect(address, username, password string) (*Client, error)

// Lifecycle
func (c *Client) Close() error
func (c *Client) CreateAuthToken(ctx context.Context, username, password string) (string, error)
func (c *Client) ListServers(ctx context.Context) ([]*Server, error)

// Database Operations
func (c *Client) CreateDatabase(ctx context.Context, name string) error
func (c *Client) DeleteDatabase(ctx context.Context, name string) error
func (c *Client) ListDatabases(ctx context.Context) ([]string, error)
func (c *Client) DatabaseExists(ctx context.Context, name string) (bool, error)
func (c *Client) GetDatabase(name string) *Database
```

### Options
```go
type Options struct {
    Address  string  // TypeDB server address (default: 127.0.0.1:1729)
    Username string  // Authentication username (default: admin)
    Password string  // Authentication password (default: password)
}

func DefaultOptions() *Options
func LocalOptions() *Options
func ParseURL(rawurl string) (*Options, error)
```

### Database
```go
type Database struct {
    // Database handle with automatic transaction management
}

// Simple Query Execution (Recommended)
func (db *Database) ExecuteRead(ctx context.Context, query string) (*Result, error)
func (db *Database) ExecuteWrite(ctx context.Context, query string) (*Result, error)
func (db *Database) ExecuteSchema(ctx context.Context, query string) (*Result, error)

// Batch Operations
func (db *Database) ExecuteBundle(ctx context.Context, txType TransactionType, ops []BundleOperation) ([]*Result, error)

// Advanced Transaction Control
func (db *Database) BeginRead(ctx context.Context) (*Transaction, error)
func (db *Database) BeginWrite(ctx context.Context) (*Transaction, error)
func (db *Database) BeginSchema(ctx context.Context) (*Transaction, error)

// Metadata
func (db *Database) Name() string
func (db *Database) GetSchema(ctx context.Context) (string, error)
func (db *Database) GetTypeSchema(ctx context.Context) (string, error)
```

### Transaction
```go
type Transaction struct {
    // Manual transaction lifecycle control
}

func (tx *Transaction) Execute(ctx context.Context, query string) (*Result, error)
func (tx *Transaction) Commit(ctx context.Context) error
func (tx *Transaction) Rollback(ctx context.Context) error
func (tx *Transaction) Close(ctx context.Context) error
func (tx *Transaction) Type() TransactionType
func (tx *Transaction) IsOpen() bool
```

### Result
```go
type Result struct {
    Type           ResultType    // RowStream, DocumentStream, Done
    QueryType      QueryType     // READ, WRITE, SCHEMA
    TypedRows      []TypedRow    // Structured row data
    RawRows        []RawRow      // Raw protobuf data
    DocumentStream []Document    // JSON-like documents
    ColumnNames    []string      // Query result columns
}

// Data Access
func (r *Result) GetRowCount() int
func (r *Result) GetDocumentCount() int
func (r *Result) IsEmpty() bool
```

### TypedRow
```go
type TypedRow struct {
    Columns map[string]*TypedValue  // Column name -> typed value
}

// Get methods with error handling
func (row *TypedRow) GetString(column string) (string, error)
func (row *TypedRow) GetInt64(column string) (int64, error)
func (row *TypedRow) GetBool(column string) (bool, error)
func (row *TypedRow) GetFloat64(column string) (float64, error)

// Convenience methods with default values (v1.5.4+)
func (row *TypedRow) GetStringOr(column string, defaultValue string) string
func (row *TypedRow) GetInt64Or(column string, defaultValue int64) int64
func (row *TypedRow) GetBoolOr(column string, defaultValue bool) bool
func (row *TypedRow) GetFloat64Or(column string, defaultValue float64) float64
```

### TypedDocument
```go
type TypedDocument struct {
    Fields map[string]*TypedValue  // Field name -> typed value
}

// Get methods with error handling
func (doc *TypedDocument) GetString(field string) (string, error)
func (doc *TypedDocument) GetInt64(field string) (int64, error)
func (doc *TypedDocument) GetBool(field string) (bool, error)
func (doc *TypedDocument) GetFloat64(field string) (float64, error)

// Convenience methods with default values (v1.5.4+)
func (doc *TypedDocument) GetStringOr(field string, defaultValue string) string
func (doc *TypedDocument) GetInt64Or(field string, defaultValue int64) int64
func (doc *TypedDocument) GetBoolOr(field string, defaultValue bool) bool
func (doc *TypedDocument) GetFloat64Or(field string, defaultValue float64) float64
```

## Bundle Operations

### BundleOperation
```go
type BundleOperation struct {
    Type  OperationType  // OpExecute, OpCommit, OpClose
    Query string         // TypeQL query (for OpExecute)
}

// Operation Types
const (
    OpExecute OperationType = "execute"
    OpCommit  OperationType = "commit"
    OpClose   OperationType = "close"
)
```

### TransactionType
```go
const (
    Read   TransactionType = "read"
    Write  TransactionType = "write"
    Schema TransactionType = "schema"
)
```

## Usage Patterns

### 1. Simple Connection
```go
// Zero configuration
client, _ := typedbclient.NewClient(nil)
defer client.Close()

// Custom server
client, _ := typedbclient.ConnectDefault("192.168.1.100:1729")
defer client.Close()
```

### 2. Database Operations
```go
// Create and use database
client.CreateDatabase(ctx, "my_app")
db := client.GetDatabase("my_app")

// Execute queries
result, _ := db.ExecuteSchema(ctx, `define entity person, owns name;`)
result, _ := db.ExecuteWrite(ctx, `insert $p isa person, has name "Alice";`)
result, _ := db.ExecuteRead(ctx, `match $p isa person; select $p;`)
```

### 3. Batch Operations
```go
results, _ := db.ExecuteBundle(ctx, typedbclient.Write, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: `insert $p isa person, has name "Bob";`},
    {Type: typedbclient.OpExecute, Query: `insert $p isa person, has name "Carol";`},
    // OpCommit and OpClose added automatically
})
```

### 4. Result Processing

#### Standard approach (with error handling)
```go
result, _ := db.ExecuteRead(ctx, `match $p isa person, has name $n; select $p, $n;`)

for i, row := range result.TypedRows {
    name, err := row.GetString("n")
    if err != nil {
        name = "Unknown"
    }
    fmt.Printf("Person %d: %s\n", i+1, name)
}
```

#### Convenient approach (with defaults) - v1.5.4+
```go
result, _ := db.ExecuteRead(ctx, `match $p isa person, has name $n, has age $a; select $p, $n, $a;`)

for i, row := range result.TypedRows {
    // One-liner per field, automatic defaults for missing fields
    name := row.GetStringOr("n", "Unknown")
    age := row.GetInt64Or("a", 0)
    premium := row.GetBoolOr("premium", false)

    fmt.Printf("Person %d: %s (age: %d, premium: %v)\n", i+1, name, age, premium)
}
```

### 5. Error Handling
```go
result, err := db.ExecuteRead(ctx, query)
if err != nil {
    // Automatic retry, reconnection, and cleanup already handled
    log.Printf("Query failed: %v", err)
}
```

## Key Features

- **Zero-Config**: Only 3 parameters (address, username, password)
- **Automatic**: Transaction lifecycle, error recovery, resource cleanup
- **Thread-Safe**: Lock-free atomic operations
- **TypeQL v3**: Full support for simplified TypeDB v3 syntax
- **Enterprise**: 2-hour keep-alive, connection pooling, retry logic
- **Type-Safe**: Strongly typed result processing