package iface

type FlowID struct {
	ID string
}

type FlowOptions struct {
	Type uint

	// for unidirectional flows
	SrcId, DstId ConnectorID
}

const (
	UnidirectionalFlowType = iota
	BidirectionalFlowType
)
