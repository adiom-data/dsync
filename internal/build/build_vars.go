/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package build

import (
	"fmt"
)

var (
	VersionStr   = "0.8-alpha"
	CopyrightStr = "Adiom Inc., 2024"
)

// VersionInfo returns a version string for the application
func VersionInfo() string {
	return fmt.Sprintf(
		"%v",
		VersionStr,
	)
}
