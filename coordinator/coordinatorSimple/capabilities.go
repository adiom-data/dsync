package coordinatorSimple

import (
	"fmt"

	"github.com/adiom-data/dsync/protocol/iface"
)

// Determine the shared capabilities between two connectors
func calcSharedCapabilities(c1Caps iface.ConnectorCapabilities, c2Caps iface.ConnectorCapabilities) iface.ConnectorCapabilities {
	// effectively a bitmask
	// XXX: is there a better way to do this? Maybe a real bitmask?
	caps := iface.ConnectorCapabilities{true, true, true, true}

	// we only care about resumability right now
	caps.Resumability = c1Caps.Resumability && c2Caps.Resumability

	return caps
}

func calcReqCapabilities(origCaps iface.ConnectorCapabilities, sharedCaps iface.ConnectorCapabilities) iface.ConnectorCapabilities {
	capsReq := origCaps

	capsReq.Source = origCaps.Source && sharedCaps.Source
	capsReq.Sink = origCaps.Sink && sharedCaps.Sink
	capsReq.IntegrityCheck = origCaps.IntegrityCheck && sharedCaps.IntegrityCheck
	capsReq.Resumability = origCaps.Resumability && sharedCaps.Resumability

	return capsReq
}

// validate connector capabilities for flow
func (c *SimpleCoordinator) validateConnectorCapabilitiesForFlow(o iface.FlowOptions) error {
	// Get the source and destination connectors
	src, ok := c.getConnector(o.SrcId)
	if !ok {
		return fmt.Errorf("source connector %v not found", o.SrcId)
	}
	dst, ok := c.getConnector(o.DstId)
	if !ok {
		return fmt.Errorf("destination connector %v not found", o.DstId)
	}

	if !src.Details.Cap.Source {
		return fmt.Errorf("source connector %v doesn't support Source mode", src.Details.Id)
	}

	if !dst.Details.Cap.Sink {
		return fmt.Errorf("destination connector %v doesn't support Sink mode", dst.Details.Id)
	}
	return nil
}
