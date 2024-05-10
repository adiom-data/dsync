package options

import (
	"fmt"
	"slices"
	"strings"

	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

// Default values for the options when missing.
const (
	DefaultVerbosity = "DEBUG"
)

var (
	allowedVerbosities = []string{"DEBUG", "INFO", "WARN", "ERROR"}
)

// Define all CLI options as Flags
// Additionally, return BeforeFunc for parsing a config file
func GetFlagsAndBeforeFunc() ([]cli.Flag, cli.BeforeFunc) {
	flags := []cli.Flag{
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "verbosity",
			Usage:       fmt.Sprintf("set the verbosity level (%s)", strings.Join(allowedVerbosities, ",")),
			Value:       DefaultVerbosity,
			DefaultText: DefaultVerbosity,
			Action: func(ctx *cli.Context, v string) error {
				if !slices.Contains(allowedVerbosities, v) {
					return fmt.Errorf("unsupported verbosity setting %v", v)
				}
				return nil
			},
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "source",
			Usage:    "Source connection string",
			Aliases:  []string{"s"},
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "destination",
			Usage:    "Destination connection string",
			Aliases:  []string{"d"},
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "metadata",
			Usage:    "Metadata store connection string",
			Aliases:  []string{"m"},
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:    "namespace",
			Usage:   "Namespace 'database.collection' to sync from on the source",
			Aliases: []string{"ns", "nsFrom"},
		}),
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "specify the path of the config file",
		},
		cli.VersionFlag,
	}

	return flags, altsrc.InitInputSourceWithContext(flags, altsrc.NewYamlSourceFromFlagFunc("config"))
}
