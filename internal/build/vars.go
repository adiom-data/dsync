/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package build

import (
	"fmt"
	"runtime/debug"
)

var (
	VersionStr   = "0.18.0"
	CopyrightStr = "Adiom Inc., 2025"
)

var Commit = func() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}
	return ""
}()

// VersionInfo returns a version string for the application
func VersionInfo() string {
	if Commit != "" {
		return fmt.Sprintf(
			"%v (git commit %v)",
			VersionStr,
			Commit,
		)
	}

	return fmt.Sprintf(
		"%v",
		VersionStr,
	)
}
