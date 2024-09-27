/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package simple

import (
	"fmt"

	"github.com/adiom-data/dsync/protocol/iface"
)

// Determine the shared capabilities between two connectors
func calcSharedCapabilities(c1Caps iface.ConnectorCapabilities, c2Caps iface.ConnectorCapabilities) iface.ConnectorCapabilities {
	// effectively a bitmask that will be later applied to individual connectors' capabilities

	// XXX: is there a better way to do this? Maybe a real bitmask?
	caps := iface.ConnectorCapabilities{true, true, true, true, true}

	// Source and Sink are always true as they don't need to be shared
	// we only care about resumability right now
	//TODO: integrity check should follow the same path
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
func (c *Simple) validateConnectorCapabilitiesForFlow(o iface.FlowOptions) error {
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
