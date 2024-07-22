# Testing

## Approach

Since we have defined protocol and interfaces for specific classes, the testing objective for a given implementation is simply to establish conformance to the specific specification. Thus, each implementation (e.g. MongoConnector) needs to be tested against a common interface test suite (e.g. ConnectorTestSuite), although additional implementation-specific tests are permitted and encouraged.

The tests are expected to be executed against a database with prepopulated data (e.g. a test container) but no active load.

## Organization

Shared test suites are located in the 'protocol.iface.test' subdirectory. 
Interface mocks generated via "mockery" are in the 'protocol.iface.mocks' directory. To regenerate:
```
cd protocol/iface
mockery --output ./mocks --name Coordinator
mockery --output ./mocks --name Transport
```
Tests for specific connectors should be in "_test.go" files next to the actual implementation as per the Go convention.

For an example of creating a test for a connector, see [Mongo Connector test](../connector/connectorMongo/connectormongo_test.go)
