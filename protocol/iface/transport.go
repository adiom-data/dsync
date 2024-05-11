package iface

// TODO: should this be more abstract? (e.g. an array)
type Location struct {
	Database   string
	Collection string
}

type DataMessage struct {
	// payload (CRDT state)
	Data *[]byte

	// header
	MutationType uint //TODO: Maybe this should be renamed MutationType
	Loc          Location
}

const (
	MutationType_Reserved = iota
	MutationType_Insert
	MutationType_Update //TODO: don't think we know what this is as its very specific to mongo rather than general CRDTs
	MutationType_Delete
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
