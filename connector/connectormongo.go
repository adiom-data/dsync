package connector

import "github.com/adiom-data/dsync/protocol/iface"

type MongoConnector struct {
}

func (mc *MongoConnector) Setup(t iface.Transport) {
	// Perform setup logic specific to MongoConnector
}

func (mc *MongoConnector) Teardown() {
	// Perform teardown logic specific to MongoConnector
}

func (mc *MongoConnector) SetParameters(reqCap iface.ConnectorCapabilities) {
	// Implement SetParameters logic specific to MongoConnector
}
