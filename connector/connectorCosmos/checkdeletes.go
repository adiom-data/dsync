/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorCosmos

import (
	"github.com/adiom-data/dsync/protocol/iface"
)

/**
 * Check for deletes in Cosmos
 *
 * This function is used to simulate deletes in Cosmos since Cosmos does not support deletes in changestream
 */
func (cc *CosmosConnector) CheckForDeletesSync(flowId iface.FlowID) {

	//TODO - Implement this function
	return
}
