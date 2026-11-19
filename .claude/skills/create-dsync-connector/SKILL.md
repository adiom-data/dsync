---
name: create-dsync-connector
description: Create a new dsync connector with test suite. Use when adding support for a new data source/sink type (e.g., file, S3, database).
---

# Create DSync Connector

## Prerequisites
- Understand the data source/sink you're connecting to
- Know the URI scheme (e.g., `file://`, `s3://`, `mongodb://`)
- Understand what operations the source supports (read, write, streaming)
- Understand data types that need to be supported (JSON and/or BSON)
- Check how to run the data source/sink locally (e.g. via Docker) for testing or if the user needs to provision it manually

## Instructions

### 0. Review Data Source/Sink capabilities
- How is the data stored and in what format (and what's the underlying storage engine, where applicable)
- What does the API look like and if there's a Go driver for it
- How does it write and can it support massively parallel batch writes (for sinks)
- How does it read and can it support massively parallelized reads (for sources)
- How best to parallelize the reads using existing APIs and taking advantage of the data source architecture (for sources)
- How does it do CDC or change tracking - how can we get the full record on updates (for sources)
- When multiple versions are available, what are the behavioral differences

### 1. Study an Existing Connector
Read a similar connector as reference. Good examples:
- `connectors/s3/` - File-based sink with batching
- `connectors/postgres/` - Full-featured with CDC using logical replication
- `connectors/mongo/` - Full-featured with streaming
- `connectors/sqlbatch/` - Connector for SQL sources using a custom query and change-tracking with polling for CDC
- `connectors/null/` - Minimal sink-only
- `connectors/random/` - Source-only

Key files to understand:
- `connector.go` - Main implementation
- `internal/app/options/connectorflags.go` - Registration and CLI flags
- `pkg/test/connector.go` - Test suite framework

### 2. Create Connector Directory
```
connectors/<name>/
├── connector.go      # Main implementation
└── connector_test.go # Tests
```

### 3. Implement connector.go

Required elements:
1. **Package declaration** matching directory name
2. **ConnectorSettings struct** with Uri and connector-specific options
3. **Sentinel errors** with descriptive messages for users
4. **NewConn() factory function** that:
   - Parses the connection URI
   - Validates settings
   - Returns `adiomv1connect.ConnectorServiceHandler`
5. **GetInfo()** returning capabilities:
   - `DbType` identifier
   - `Source` capabilities if readable
   - `Sink` capabilities if writable
6. **Interface methods** - implement or return `Unimplemented`:
   - `GeneratePlan` - partition data for reading
   - `GetNamespaceMetadata` - count records
   - `ListData` - read data
   - `WriteData` - write data
   - `StreamUpdates` - CDC (or Unimplemented)
   - `StreamLSN` - LSN tracking (or Unimplemented)
   - `WriteUpdates` - incremental updates (or Unimplemented)
7. **Teardown()** for cleanup

Notes:
- Read planning should attempt to partition source data set into random equally sized tasks in an efficient way, avoiding a complete table scan.
- Writing should be done in an efficient way, using batch writes where possible, handling write errors. Writes should deterministically overwrite pre-existing data.

Error message guidelines:
- Include context (file path, namespace, operation)
- Explain what went wrong AND what was expected
- Use `fmt.Errorf("failed to X for Y: %w", err)` pattern

### 4. Register the Connector

Edit `internal/app/options/connectorflags.go`:

1. Add import:
```go
myconnector "github.com/adiom-data/dsync/connectors/<name>"
```

2. Add to `GetRegisteredConnectors()`:
```go
{
    Name: "<Name>",
    IsConnector: func(s string) bool {
        return strings.HasPrefix(strings.ToLower(s), "<scheme>://")
    },
    Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
        settings := myconnector.ConnectorSettings{Uri: args[0]}
        return CreateHelper("<name>", "<usage>", MyConnectorFlags(&settings), 
            func(_ *cli.Context, _ []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
                return myconnector.NewConn(settings)
            })(args, as)
    },
},
```

3. Add flags function if needed:
```go
func MyConnectorFlags(settings *myconnector.ConnectorSettings) []cli.Flag {
    return []cli.Flag{
        // Define CLI flags
    }
}
```

### 5. Create Test Data

Create `connectors/<name>/test_data/` with:
- Valid data files
- Edge cases (empty, single row, special characters)
- Invalid data (malformed, missing required fields)
- Nested directories if namespace hierarchy matters

### 6. Write Tests

Create `connector_test.go`:

1. **Unit tests** for each public function
2. **Integration test** using the test suite:
```go
func TestMyConnectorSuite(t *testing.T) {
    tSuite := pkgtest.NewConnectorTestSuite(
        "namespace",
        func() adiomv1connect.ConnectorServiceClient {
            conn, _ := NewConn(ConnectorSettings{Uri: "..."})
            return pkgtest.ClientFromHandler(conn)
        },
        bootstrapFunc,  // or nil
        insertUpdatesFunc,  // or nil
        numPages,
        numItems,
    )
    // Set flags for unsupported features:
    tSuite.SkipDuplicateTest = true      // If output order non-deterministic
    tSuite.SkipWriteUpdatesTest = true   // If WriteUpdates not supported
    suite.Run(t, tSuite)
}
```

3. **Error case tests** with descriptive messages

### 7. Verify

Run:
```bash
go build ./...
go test ./connectors/<name>/... -v
```

Check:
- [ ] All tests pass
- [ ] `--help` shows connector options
- [ ] Error messages are user-friendly
- [ ] No sensitive data in logs

### 8. Integration Testing

Run dsync with the new connector as destination:
```bash
go run main.go /dev/fakesource <new-connector-uri> [options]
```
Evaluate output for: namespaces, data content, warnings, errors.

Run dsync with the new connector as source:
```bash
go run main.go <new-connector-uri> /dev/null --log-json
```
Evaluate output for: namespaces, data content, warnings, errors.

Try running both (one from /dev/fakesource to the new connector, and another one from the new connector to /dev/null) at the same time to ensure that the CDC is working correctly.
