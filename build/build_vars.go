package build

import (
	"fmt"
)

var (
	VersionStr   = "0.2-alpha"
	CopyrightStr = "Adiom Inc., 2024"
)

// VersionInfo returns a version string for the application
func VersionInfo() string {
	return fmt.Sprintf(
		"dsync, version %v",
		VersionStr,
	)
}
