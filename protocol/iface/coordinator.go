package iface

import "context"

type ConnectorID struct {
	ID string
}

// General coordinator interface
type Coordinator interface {
	// General
	Setup(ctx context.Context, t Transport, s Statestore)
	Teardown()

	// User
	GetConnectors() []ConnectorDetails

	FlowCreate(o FlowOptions) (FlowID, error)
	FlowStart(fid FlowID) error
	FlowStop(fid FlowID)
	FlowDestroy(fid FlowID)
	WaitForFlowDone(flowId FlowID) error                                        // Wait for the flow to be done
	PerformFlowIntegrityCheck(fid FlowID) (FlowDataIntegrityCheckResult, error) // Perform an integrity check on the flow (synchronous)

	CoordinatorIConnectorSignal
}

type ConnectorDetails struct {
	Id   ConnectorID
	Desc string
	Type ConnectorType
	Cap  ConnectorCapabilities
}

// Singalling coordinator interface for use by connectors
type CoordinatorIConnectorSignal interface {
	// Register a connector with type, capabilities, and endpoint for its signalling interface
	RegisterConnector(details ConnectorDetails, cep ConnectorICoordinatorSignal) (ConnectorID, error)
	DelistConnector(ConnectorID)

	// Done event (for a connector to announce that they finished the flow)
	NotifyDone(flowId FlowID, conn ConnectorID) error
}
