/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

type FlowID string

// Provided by the user and uniquely determines the flow
type FlowOptions struct {
	Type uint

	// for unidirectional flows
	SrcId, DstId        ConnectorID
	SrcConnectorOptions ConnectorOptions
}

const (
	UnidirectionalFlowType = iota
	BidirectionalFlowType
)

type FlowDataIntegrityCheckResult struct {
	Passed bool
}

type FlowStatus struct {
	// For uni-directional flows
	SrcStatus, DstStatus ConnectorStatus
}
