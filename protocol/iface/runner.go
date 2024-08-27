// Copyright (c) 2024. Adiom, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package iface

import (
	"context"
)

type Runner interface {
	// General
	Setup(ctx context.Context) error
	Run()
	Teardown()
}
