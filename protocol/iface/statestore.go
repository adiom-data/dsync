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
}
