package options

import (
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

// Default values for the options when missing.
const (
	DefaultVerbosity = "DEBUG"
)

// Define all CLI options as Flags
// Additionally, return BeforeFunc for parsing a config file
func GetFlagsAndBeforeFunc() ([]cli.Flag, cli.BeforeFunc) {
	flags := []cli.Flag{
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "verbosity",
			Usage:       "set the verbosity level (DEBUG,INFO)",
			Value:       DefaultVerbosity,
			DefaultText: DefaultVerbosity,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:  "logFile",
			Usage: "write logs to a file",
		}),
		&cli.StringFlag{
			Name:  "config",
			Usage: "specify the path of the config file",
		},
		cli.VersionFlag,
	}

	return flags, altsrc.InitInputSourceWithContext(flags, altsrc.NewYamlSourceFromFlagFunc("config"))
}
