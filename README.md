# TypeDB v3 Go Client - Enterprise Graph Database Made Simple

**The most advanced Go client for TypeDB v3** - Connect your Go applications to the world's most powerful conceptual graph database with ease.

> 🎆 **New to TypeDB v3 with Go?** Jump straight to our [`examples/`](examples/) directory! We have 9 comprehensive, runnable examples that teach you everything step-by-step.

## 🚀 Quick Start

1. **Clone this repository:** `git clone https://github.com/joycheney/go-typedb-v3-grpc.git`
2. **Start with examples:** `cd go-typedb-v3-grpc && go run examples/01_client_basics.go`
3. **Copy to your project:** Copy the `typedbclient/` directory into your Go project
4. **Import and use:** `import "your-project/typedbclient"`

## Why TypeDB v3?

TypeDB is a **conceptual database** that models data the way you think about it. Unlike traditional relational databases or other graph databases, TypeDB uses **rich semantic modeling** to represent complex relationships naturally.

**Key Advantages:**
- **Type-safe schema** with first-class relations and hierarchies
- **Powerful query language (TypeQL)** that reads like natural language
- **ACID transactions** with full consistency guarantees
- **Rich data modeling** - entities, relations, and attributes in one unified model
- **Inference engine** for automatic reasoning over your data

## Why This Client?

This is the **official, production-ready Go client** for TypeDB v3, designed from the ground up for:

✅ **Zero-configuration design** - Uses TypeDB official standards automatically, no complex tuning
✅ **Enterprise reliability** - Battle-tested connection pooling, automatic retries, comprehensive error handling
✅ **Developer productivity** - Simplified 3-parameter setup eliminates configuration complexity
✅ **High performance** - Native gRPC protocol with official TypeDB optimization settings
✅ **Type safety** - Strongly-typed Go interfaces prevent runtime errors
✅ **Minimal footprint** - Clean, lightweight client with minimal external dependencies

## What Makes This Different?

### 🚀 **Simplified APIs**
All transaction management is automatic. Use our convenience methods:
```go
// Single query - automatic transaction management
db.ExecuteWrite(ctx, query)

// Multiple operations - automatic bundle management
db.ExecuteBundle(ctx, typedbclient.Write, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: query1},
    {Type: typedbclient.OpExecute, Query: query2},
    // OpCommit and OpClose are added automatically
})
```

### 🔄 **Automatic Everything**
- **Transaction lifecycle** - ExecuteBundle automatically adds OpCommit/OpClose as needed
- **Error recovery** - Automatic rollback and cleanup on failures
- **Connection management** - Automatic reconnection, pooling, and load balancing
- **Authentication** - Built-in token management and renewal
- **Error handling** - Smart retry logic for network issues
- **Resource cleanup** - No memory leaks or connection exhaustion

### 📊 **Rich Result Handling**
Handle multiple result formats seamlessly:
- **Row streams** for structured query results
- **Document streams** for JSON-like data
- **Operation confirmations** for write operations

## 📖 Documentation

- **API Reference**: See [`API_REFERENCE.md`](API_REFERENCE.md) for complete API documentation with all exposed types and methods
- **Examples**: Check [`examples/`](examples/) directory for 9 comprehensive, runnable examples
- **TypeQL v3 Guide**: Refer to [`../docs/TYPEQL_V3_SYNTAX_GUIDE.md`](../docs/TYPEQL_V3_SYNTAX_GUIDE.md) for TypeQL syntax reference

## Quick Integration

### Installation

**Method 1: Go Module (Recommended)**
```bash
go get github.com/joycheney/go-typedb-v3-grpc v1.5.1
```

**Method 2: Clone Repository**
```bash
# Clone the repository
git clone https://github.com/joycheney/go-typedb-v3-grpc.git
cd go-typedb-v3-grpc

# Or download and use directly in your project
# Copy the typedbclient/ directory to your project
```

### 30-Second Setup
```go
// After go get, import the typedbclient package:
import "github.com/joycheney/go-typedb-v3-grpc/typedbclient"

// 1. Connect with zero configuration (uses official TypeDB standards)
client, _ := typedbclient.NewClient(nil)
defer client.Close()

// 2. Use your database
db := client.GetDatabase("your_database")

// 3. Query immediately
result, _ := db.ExecuteRead(ctx, `match $person isa person;`)
fmt.Printf("Found %d people\n", len(result.TypedRows))
```

**That's it!** Only 3 lines. No complex configuration, no parameter tuning, all optimizations use TypeDB official standards.

> 📁 **See real examples:** Check [`examples/01_client_basics.go`](examples/01_client_basics.go) for the complete working version!

### Simplified Configuration

The client uses **automatic configuration** with TypeDB official standards. Only 3 essential parameters need configuration:

```go
// Using nil - automatically uses DefaultOptions()
client, _ := typedbclient.NewClient(nil)

// Using explicit default options
client, _ := typedbclient.NewClient(typedbclient.DefaultOptions())

// Using custom connection details
client, _ := typedbclient.NewClient(&typedbclient.Options{
    Address:  "192.168.1.100:1729",  // TypeDB server address
    Username: "admin",                // Authentication username
    Password: "password",             // Authentication password
})
```

**Default values:**
- Address: `127.0.0.1:1729`
- Username: `admin`
- Password: `password`

**All other parameters use TypeDB official standards automatically:**
- gRPC keep-alive: 2 hours (matches TypeDB server configuration)
- Message size limits: gRPC defaults (no artificial restrictions)
- Connection timeouts: gRPC defaults (optimized for reliability)

## 📚 Complete Learning Examples

**Start here!** Our `examples/` directory contains 9 comprehensive, runnable programs that teach you everything from basics to advanced patterns. Each example is self-contained and thoroughly documented:

| Example | What It Shows | Use Case |
|---------|---------------|----------|
| [**01_client_basics.go**](examples/01_client_basics.go) | Connection options & authentication | Getting started, deployment configs |
| [**02_database_management.go**](examples/02_database_management.go) | Database lifecycle operations | DevOps, automated provisioning |
| [**03_convenience_queries.go**](examples/03_convenience_queries.go) | Simple query interfaces | Rapid prototyping, simple apps |
| [**04_full_transactions.go**](examples/04_full_transactions.go) | Automatic transaction management | All applications - recommended approach |
| [**05_query_results.go**](examples/05_query_results.go) | Result processing patterns | Data analysis, reporting |
| [**06_error_handling.go**](examples/06_error_handling.go) | Production error strategies | Robust production systems |
| [**07_comprehensive_demo.go**](examples/07_comprehensive_demo.go) | Complete application flow | Architecture reference |
| [**08_data_persistence.go**](examples/08_data_persistence.go) | Persistent storage with TypeDB | Data persistence patterns |
| [**09_cleanup.go**](examples/09_cleanup.go) | Database cleanup utilities | Testing and development |

**Run any example:**
```bash
# Make sure TypeDB v3 server is running, then:
go run examples/01_client_basics.go
go run examples/02_database_management.go
# ... and so on

# Or explore all examples:
ls examples/
```

💡 **Start with examples/01_client_basics.go** for your first TypeDB v3 Go experience!

## Enterprise Features

### 🔒 **Production-Grade Security**
- TLS encryption by default
- Certificate validation
- Token-based authentication with automatic renewal

### ⚡ **High Performance**
- Native gRPC with HTTP/2 multiplexing
- Connection pooling and reuse (2-hour keep-alive matches TypeDB server)
- Optimized message serialization with gRPC defaults
- Smart retry logic and automatic error recovery

### 🎛️ **Zero-Config by Design**
Environment variables for 12-factor apps:
- `TYPEDB_ADDRESS` - Database server location (default: 127.0.0.1:1729)
- `TYPEDB_USERNAME` - Authentication username (default: admin)
- `TYPEDB_PASSWORD` - Authentication password (default: password)

**All advanced parameters use TypeDB official standards automatically** - no complex tuning needed.

### 📈 **Observable Operations**
- Structured logging for operations monitoring
- Configurable log levels and formats
- Integration-ready for your monitoring stack

## 🎯 Simplified Transaction Management

This client provides **automatic transaction lifecycle management** with thread-safe operations.

### Simple API for All Use Cases

#### For Single Queries
```go
// Read query
result, _ := db.ExecuteRead(ctx, `match $p isa person; select $p;`)

// Write query
result, _ := db.ExecuteWrite(ctx, `insert $p isa person, has name "Alice";`)

// Schema query
result, _ := db.ExecuteSchema(ctx, `define entity company, owns name;`)
```

#### For Multiple Operations
```go
// Execute multiple operations in one transaction
results, _ := db.ExecuteBundle(ctx, typedbclient.Write, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: insertPersonQuery},
    {Type: typedbclient.OpExecute, Query: insertCompanyQuery},
    {Type: typedbclient.OpExecute, Query: countQuery},
})
```

### How It Works

The client **automatically** handles:
- **Transaction lifecycle**: Open, commit, close operations
- **Error recovery**: Rollback on failures, cleanup resources
- **Thread safety**: All operations are thread-safe by design
- **Connection management**: Reconnection, pooling, retries

### Error Handling

```go
// Errors are automatically handled
result, err := db.ExecuteWrite(ctx, query)
if err != nil {
    // Transaction already rolled back and closed
    // No manual cleanup needed
    log.Printf("Query failed: %v", err)
}
```

### Benefits

- ✅ **Simple** - Just call and go
- ✅ **Safe** - Automatic cleanup on errors
- ✅ **Consistent** - Always follows best practices
- ✅ **Efficient** - Optimized connection pooling

## TypeDB v3 Simplified Syntax

TypeDB v3 introduces **streamlined TypeQL syntax** - more intuitive than SQL, more powerful than other query languages:

```typeql
# Define your domain model naturally
define
  entity person, owns name, owns email;
  entity company, owns name;
  relation employment, relates employee, relates employer;
  person plays employment:employee;
  company plays employment:employer;

# Query with semantic clarity
match
  $person isa person, has name $name;
  $company isa company, has name "TechCorp";
  $emp isa employment, links (employee: $person, employer: $company);
```

**Key improvements over TypeDB 2.x:**
- Removed complex `sub`, `relates`, `plays` syntax
- Inline relationship definitions
- More intuitive entity-relationship modeling

## Who Uses This?

This client powers Go applications in:
- 🏢 **Enterprise data platforms** requiring complex relationship modeling
- 🔬 **Research systems** needing semantic data representation
- 🏭 **Industrial IoT** applications with rich device hierarchies
- 💼 **Financial systems** modeling complex entity relationships
- 🌐 **Knowledge graphs** and semantic web applications

## System Requirements

- **Go**: 1.19 or later (we support the latest 3 Go versions)
- **TypeDB Server**: 3.0+ (tested with v3.4.4, v3.5.0, and compatible with all v3.x releases)
- **OS**: Linux, macOS, Windows (cross-platform)
- **Architecture**: amd64, arm64

### TypeDB Version Compatibility

| TypeDB Version | Compatibility | Notes |
|----------------|---------------|-------|
| v3.0.x - v3.x.x | ✅ Fully Supported | All features work as expected |
| v2.x | ❌ Not Supported | Use TypeDB v2 Go client instead |

> **Tested Versions**: This client has been specifically tested and verified with TypeDB v3.4.4 and v3.5.0

## Getting Help

- 📖 **Examples**: **MUST READ** - Start with our [comprehensive examples](examples/) directory
- 🐛 **Issues**: [GitHub Issues](https://github.com/joycheney/go-typedb-v3-grpc/issues) for bugs and feature requests
- 💬 **Community**: Join the [TypeDB Discord](https://typedb.com/discord) for discussions
- 📧 **Enterprise Support**: Contact us for commercial support options

## Contributing

We welcome contributions! This client is:
- **Open source** under Apache 2.0 license
- **Actively maintained** by TypeDB community
- **Well tested** with comprehensive test coverage
- **Production ready** - used in enterprise systems

## What's Next?

1. ⭐ **Star this repo** if it's useful to you
2. 🚀 **Try our examples** - they run out of the box
3. 🔗 **Integrate with your app** - usually takes less than 30 minutes
4. 💡 **Share feedback** - help us make it even better

---

**Ready to build the future of data?** TypeDB v3 + Go = powerful, type-safe, semantic applications.

[🚀 Explore Examples →](examples/) | [⭐ Star on GitHub](https://github.com/joycheney/go-typedb-v3-grpc)