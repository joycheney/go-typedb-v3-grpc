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
    Columns []TypedValue  // Column values with type information
}

func (row *TypedRow) GetString(index int) string
func (row *TypedRow) GetInt(index int) int64
func (row *TypedRow) GetDouble(index int) float64
func (row *TypedRow) GetBool(index int) bool
func (row *TypedRow) GetDateTime(index int) time.Time
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
```go
result, _ := db.ExecuteRead(ctx, `match $p isa person, has name $n; select $p, $n;`)

for i, row := range result.TypedRows {
    name := row.GetString(1)  // Second column is name
    fmt.Printf("Person %d: %s\n", i+1, name)
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