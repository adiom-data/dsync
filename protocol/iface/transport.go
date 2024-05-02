package iface

type Transport interface {
	// Receives the coordinator endpoint
	GetCoordinatorEndpoint(location string) CoordinatorIConnectorSignal
}
