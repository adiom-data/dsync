# Connector Creation Checklist

## Pre-Implementation
- [ ] Identified similar existing connector to use as reference
- [ ] Defined URI scheme for the connector
- [ ] Listed required configuration options
- [ ] Determined supported data types (JSON_ID, MONGO_BSON)
- [ ] Determined supported operations (source, sink, streaming)

## Implementation
- [ ] Created `connectors/<name>/` directory
- [ ] Implemented `ConnectorSettings` struct
- [ ] Implemented `NewConn()` factory function
- [ ] Implemented `GetInfo()` with correct capabilities
- [ ] Implemented `GeneratePlan()` for data partitioning
- [ ] Implemented `GetNamespaceMetadata()` for record counts
- [ ] Implemented `ListData()` for reading
- [ ] Implemented `WriteData()` for writing (or Unimplemented)
- [ ] Implemented `StreamUpdates()` (or Unimplemented)
- [ ] Implemented `StreamLSN()` (or Unimplemented)
- [ ] Implemented `WriteUpdates()` (or Unimplemented)
- [ ] Implemented `Teardown()` for cleanup

## Registration
- [ ] Added import in `connectorflags.go`
- [ ] Added `RegisteredConnector` entry
- [ ] Added CLI flags function (if needed)

## Error Handling
- [ ] All errors include context (path, namespace, etc.)
- [ ] Error messages explain what went wrong
- [ ] Error messages suggest expected format/values
- [ ] No sensitive data exposed in error messages

## Testing
- [ ] Created `test_data/` directory with fixtures
- [ ] Test data includes valid cases
- [ ] Test data includes edge cases (empty, single item)
- [ ] Test data includes invalid cases
- [ ] Unit tests for each public function
- [ ] Suite test using `NewConnectorTestSuite`
- [ ] Set `SkipDuplicateTest` if output non-deterministic
- [ ] Set `SkipWriteUpdatesTest` if updates not supported

## Verification
- [ ] `go build ./...` succeeds
- [ ] `go test ./connectors/<name>/... -v` passes
- [ ] `go run main.go <connector> --help` shows options
- [ ] Integration test as destination with `/dev/fakesource`
- [ ] Integration test as source with `/dev/null --log-json`
