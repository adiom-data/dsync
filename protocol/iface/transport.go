package iface

type Transport interface {
	// Gives the coordinator endpoint as a signalling interface
	GetCoordinatorEndpoint(location string) (CoordinatorIConnectorSignal, error)
}
