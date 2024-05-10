package iface

// TODO: should this be more abstract? (e.g. an array)
type Location struct {
	Database   string
	Collection string
}

type DataMessage struct {
	// payload
	Data *[]byte

	// header
	OpType uint //Maybe this should be renamed MutationType in the future
	Loc    Location
}

const (
	OpType_Reserved = iota
	OpType_Insert
	OpType_Update
	OpType_Delete
)

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
