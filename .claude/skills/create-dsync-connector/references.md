# References

## Key Files

### Connector Implementation
- `connectors/s3/connector.go` - Good example of file-based connector with batching
- `connectors/mongo/conn.go` - Full-featured connector with streaming support
- `connectors/null/connector.go` - Minimal sink-only connector
- `connectors/random/connector.go` - Source-only connector
- `connectors/file/connector.go` - Recent example with CSV support

### Registration
- `internal/app/options/connectorflags.go` - Connector registration and CLI flags

### Test Framework
- `pkg/test/connector.go` - `NewConnectorTestSuite` and `ClientFromHandler`

### Proto Definitions
- `gen/adiom/v1/adiomv1connect/` - Generated Connect-RPC interfaces
- `gen/adiom/v1/messages.pb.go` - Data types (`DataType_DATA_TYPE_JSON_ID`, `DataType_DATA_TYPE_MONGO_BSON`)

## Existing Connector Tests
- `connectors/file/connector_test.go` - Comprehensive tests with test data
- `connectors/random/connector_test.go` - Simple suite test example
- `connectors/mongo/connector_test.go` - External database test example

## Test Data Examples
- `connectors/file/test_data/` - Various CSV edge cases
- `connectors/testconn/` - Bootstrap/updates JSON format
