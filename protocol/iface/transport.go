package iface

// TODO: should this be more abstract? (e.g. an array)
type Location struct {
	Database   string
	Collection string
}

// TODO: byte[] should be in Avro or Protobuf format (maybe subject to negotiation based on capabilities - like a preference list advertized by a connector)
type DataMessage struct {
	// payload (CRDT state)
	Data *[]byte
	// special case payload for inserts to allow the reader to send the whole batch for a single location (for efficiency)
	DataBatch *[][]byte

	// header
	MutationType uint     //required
	Loc          Location //required
	Id           *[]byte  //required except for inserts (for efficiency)
	IdType       byte     //required when Id is present
}

const (
	MutationType_Reserved = iota
	MutationType_Insert
	MutationType_InsertBatch
	MutationType_Update
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
