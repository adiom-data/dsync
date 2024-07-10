/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

import "context"

type Statestore interface {
	// General
	Setup(ctx context.Context) error
	Teardown()

	// Read plan persistence
	//TODO: should we persist the whole flow state? (we may need an object for this)
	//TODO: should this be more generic here and the specifics delegated to the coordinator impl?
	FlowPlanPersist(FlowID, ConnectorReadPlan) error
}
