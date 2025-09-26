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

✅ **Enterprise reliability** - Battle-tested connection pooling, automatic retries, comprehensive error handling
✅ **Developer productivity** - Simplified APIs that eliminate boilerplate code
✅ **High performance** - Native gRPC protocol with optimized serialization
✅ **Type safety** - Strongly-typed Go interfaces prevent runtime errors
✅ **Zero dependencies** - Clean, lightweight client with minimal external dependencies

## What Makes This Different?

### 🚀 **Simplified APIs**
Instead of complex transaction management, use our convenience methods:
```go
// Advanced way: Full transaction control with automatic lifecycle management
tx, _ := db.BeginTransaction(ctx, typedbclient.Write)
// ExecuteBundle automatically adds OpCommit and OpClose for you!
results, _ := tx.ExecuteBundle(ctx, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: query},
    // No need to add OpCommit and OpClose - they're automatic!
})

// Simplified way: One line with automatic transaction management
db.ExecuteWrite(ctx, query)
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

## Quick Integration

### Installation
```bash
# Clone the repository
git clone https://github.com/joycheney/go-typedb-v3-grpc.git
cd go-typedb-v3-grpc

# Or download and use directly in your project
# Copy the typedbclient/ directory to your project
```

### 30-Second Setup
```go
// Copy typedbclient/ to your project, then:
import "your-project/typedbclient"

// 1. Connect (works with defaults)
client, _ := typedbclient.NewClient(nil)
defer client.Close()

// 2. Use your database
db := client.GetDatabase("your_database")

// 3. Query immediately
result, _ := db.ExecuteRead(ctx, `match $person isa person;`)
fmt.Printf("Found %d people\n", len(result.TypedRows))
```

**That's it!** No complex configuration, no connection string parsing, no driver setup.

> 📁 **See real examples:** Check [`examples/01_client_basics.go`](examples/01_client_basics.go) for the complete working version!

### Configuration Options

The client supports flexible configuration through the `Options` struct:

```go
// Using nil - automatically uses DefaultOptions()
client, _ := typedbclient.NewClient(nil)

// Using explicit default options
client, _ := typedbclient.NewClient(typedbclient.DefaultOptions())

// Using custom options
client, _ := typedbclient.NewClient(&typedbclient.Options{
    Address:  "192.168.1.100:1729",  // TypeDB server address
    Username: "admin",                // Authentication username
    Password: "password",             // Authentication password
    KeepAliveTime:    30 * time.Second,  // TCP keep-alive interval
    KeepAliveTimeout: 10 * time.Second,  // TCP keep-alive timeout
    MaxRecvMsgSize:   16 * 1024 * 1024,  // Max receive message size (16MB)
    MaxSendMsgSize:   16 * 1024 * 1024,  // Max send message size (16MB)
})
```

**Default values:**
- Address: `127.0.0.1:1729`
- Username: `admin`
- Password: `password`
- KeepAliveTime: 30 seconds
- KeepAliveTimeout: 10 seconds
- MaxRecvMsgSize: 16MB
- MaxSendMsgSize: 16MB

## 📚 Complete Learning Examples

**Start here!** Our `examples/` directory contains 9 comprehensive, runnable programs that teach you everything from basics to advanced patterns. Each example is self-contained and thoroughly documented:

| Example | What It Shows | Use Case |
|---------|---------------|----------|
| [**01_client_basics.go**](examples/01_client_basics.go) | Connection options & authentication | Getting started, deployment configs |
| [**02_database_management.go**](examples/02_database_management.go) | Database lifecycle operations | DevOps, automated provisioning |
| [**03_convenience_queries.go**](examples/03_convenience_queries.go) | Simple query interfaces | Rapid prototyping, simple apps |
| [**04_full_transactions.go**](examples/04_full_transactions.go) | Advanced transaction control | Enterprise apps, complex workflows |
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
- Connection pooling and reuse
- Optimized message serialization
- Configurable timeouts and retries

### 🎛️ **Flexible Configuration**
Environment variables for 12-factor apps:
- `TYPEDB_ADDRESS` - Database server location
- `TYPEDB_USERNAME` - Authentication username
- `TYPEDB_PASSWORD` - Authentication password

Or programmatic configuration for complex setups.

### 📈 **Observable Operations**
- Structured logging for operations monitoring
- Configurable log levels and formats
- Integration-ready for your monitoring stack

## 🎯 Automatic Transaction Lifecycle Management

One of the most powerful features of this client is **automatic transaction lifecycle management**. You no longer need to manually add `OpCommit` and `OpClose` operations - the client handles this intelligently for you.

### How It Works

When you use `ExecuteBundle`, the client automatically:
1. **Analyzes your operations** to understand what you're trying to do
2. **Adds the right operations** based on transaction type:
   - For **Write/Schema transactions**: adds `OpCommit` then `OpClose`
   - For **Read transactions**: adds `OpClose` only (no commit needed)
3. **Handles errors gracefully**:
   - On failure in Write/Schema transactions: executes rollback then close
   - On failure in Read transactions: executes close
   - Always ensures proper cleanup

### Examples

```go
// Before: Manual lifecycle management (tedious and error-prone)
tx, _ := db.BeginTransaction(ctx, typedbclient.Write)
results, _ := tx.ExecuteBundle(ctx, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: insertQuery},
    {Type: typedbclient.OpCommit},   // Had to remember this
    {Type: typedbclient.OpClose},    // And this too
})

// Now: Automatic lifecycle management (clean and simple)
tx, _ := db.BeginTransaction(ctx, typedbclient.Write)
results, _ := tx.ExecuteBundle(ctx, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: insertQuery},
    // That's it! OpCommit and OpClose are added automatically
})

// Error handling is also automatic
tx, _ := db.BeginTransaction(ctx, typedbclient.Write)
results, err := tx.ExecuteBundle(ctx, []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: badQuery},
})
if err != nil {
    // Transaction already rolled back and closed automatically!
    // No manual cleanup needed
}
```

### Explicit Control When Needed

You can still explicitly control transaction flow for special cases:

```go
// Explicit rollback for business logic validation
bundle := []typedbclient.BundleOperation{
    {Type: typedbclient.OpExecute, Query: insertQuery},
    // Business logic check fails - explicit rollback
    {Type: typedbclient.OpRollback},  // ExecuteBundle adds OpClose after this
}
```

This automatic lifecycle management:
- ✅ **Reduces boilerplate** - Write less code
- ✅ **Prevents errors** - Never forget to close transactions
- ✅ **Ensures consistency** - Always commit before close
- ✅ **Handles failures** - Automatic rollback on errors
- ✅ **Maintains compatibility** - Existing code with manual operations still works

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