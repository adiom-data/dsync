package iface

type DataMessage struct {
	Data *[]byte
}

type DataChannelID struct {
	ID string
}

type Transport interface {
	// Gives the coordinator endpoint as a signalling interface
	GetCoordinatorEndpoint(location string) (CoordinatorIConnectorSignal, error)

	// Creates a data channel
	CreateDataChannel() (DataChannelID, error)

	// Gets a data channel endpoint by ID
	GetDataChannelEndpoint(DataChannelID) (chan DataMessage, error)

	// Closes a data channel
	CloseDataChannel(DataChannelID)
}
