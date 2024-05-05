package iface

type DataMessage struct {
}

type DataChannel struct { // Unique identifier for a channel
	ID     string
	Writer chan DataMessage
	Reader chan DataMessage
}

type Transport interface {
	// Gives the coordinator endpoint as a signalling interface
	GetCoordinatorEndpoint(location string) (CoordinatorIConnectorSignal, error)

	// Creates a data channel
	CreateDataChannel() (DataChannel, error)

	// Closes a data channel
	CloseDataChannel(id string)
}
