/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package options

import (
	"fmt"
	"slices"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

// DefaultVerbosity is the default verbosity level for the application.
const DefaultVerbosity = "INFO"

// XXX: should these definitions be moved to the RunnerLocal?
var validVerbosities = []string{"DEBUG", "INFO", "WARN", "ERROR"}

var validLoadLevels = []string{"Low", "Medium", "High", "Beast"}

const (
	// DefaultPprofPort is the default port for pprof profiling.
	DefaultPprofPort = 8081
	// DefaultWebPort is the default port for the web server.
	DefaultWebPort = 8080
	// DefaultMaxNumNamespaces is the default maximum number of namespaces that can be copied from the CosmosDB connector.
	cosmosDefaultMaxNumNamespaces = 8
)

var validModes = []string{iface.SyncModeFull, iface.SyncModeCDC, iface.SyncModeSnapshot}

const defaultMode = iface.SyncModeFull

// GetFlagsAndBeforeFunc defines all CLI options as flags and returns
// a BeforeFunc to parse a configuration file before any other actions.
func GetFlagsAndBeforeFunc() ([]cli.Flag, cli.BeforeFunc) {
	flags := []cli.Flag{
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "verbosity",
			Usage:       fmt.Sprintf("set the verbosity level (%s)", strings.Join(validVerbosities, ",")),
			Value:       DefaultVerbosity,
			DefaultText: DefaultVerbosity,
			Action: func(ctx *cli.Context, verbosity string) error {
				if !slices.Contains(validVerbosities, verbosity) {
					return fmt.Errorf("unsupported verbosity setting %v", verbosity)
				}
				return nil
			},
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "metadata",
			Usage:    "metadata store connection string. Will default to the destination if not provided",
			Aliases:  []string{"m"},
			Category: "Endpoint Configuration",
			Required: false,
		}),
		altsrc.NewStringSliceFlag(&cli.StringSliceFlag{
			Name:     "namespace",
			Usage:    "list of namespaces 'db1,db2.collection,db3.collection:otherdb.othercollection' (comma-separated) to sync from on the source and a colon to map names.",
			Aliases:  []string{"ns", "nsFrom"},
			Category: "Flow Options",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:     "verify",
			Usage:    "perform a data integrity check for an existing flow",
			Category: "Special Commands",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:     "verify-quick-count",
			Usage:    "perform a data integrity check for an existing flow by doing a quick count by namespace",
			Category: "Special Commands",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:     "cleanup",
			Usage:    "cleanup metadata for an existing flow",
			Category: "Special Commands",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:     "reverse",
			Usage:    "start the flow in reverse mode - destination to source, and skip the initial data copy",
			Category: "Special Commands",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "progress",
			Usage: "displays detailed progress of the sync, logfile required",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:  "logfile",
			Usage: "log file path, sends logs to file instead of stdout, default logs to stdout",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:  "load-level",
			Usage: fmt.Sprintf("load level (%s). When not specified, will default to connector-specific settings", strings.Join(validLoadLevels, ",")),
			Action: func(ctx *cli.Context, source string) error {
				if !slices.Contains(validLoadLevels, source) {
					return fmt.Errorf("unsupported load level setting %v", source)
				}
				return nil
			},
			Required: false,
			Category: "Flow Options",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:  "mode",
			Usage: fmt.Sprintf("mode of operation: %s.", strings.Join(validModes, ",")),
			Action: func(ctx *cli.Context, source string) error {
				if !slices.Contains(validModes, source) {
					return fmt.Errorf("unsupported mode setting %v", source)
				}
				return nil
			},
			DefaultText: defaultMode,
			Required:    false,
			Category:    "Flow Options",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "pprof",
			Usage: "enable pprof profiling on localhost:8081",
		}),
		altsrc.NewUintFlag(&cli.UintFlag{
			Name:  "pprof-port",
			Usage: "specify the port for pprof profiling",
			Value: DefaultPprofPort,
		}),
		altsrc.NewUintFlag(&cli.UintFlag{
			Name:  "web-port",
			Usage: "specify the port for web server",
			Value: DefaultWebPort,
		}),
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "specify the path of the config file",
		},
		cli.VersionFlag,
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "writer-batch-size",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "parallel-copiers",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "parallel-writers",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "parallel-integrity-check-workers",
			Required: false,
			Hidden:   true,
		}),
	}

	before := func(c *cli.Context) error {
		if c.IsSet("progress") && !c.IsSet("logfile") {
			return fmt.Errorf("logfile is required to display progress")
		}
		return altsrc.InitInputSourceWithContext(flags, altsrc.NewYamlSourceFromFlagFunc("config"))(c)
	}
	return flags, before
}
