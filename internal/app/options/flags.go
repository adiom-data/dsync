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

var validSources = []string{"MongoDB", "CosmosDB"}

var validDestinations = []string{"MongoDB", "CosmosDB"}

var validLoadLevels = []string{"Low", "Medium", "High", "Beast"}

const (
	// DefaultPprofPort is the default port for pprof profiling.
	DefaultPprofPort = 8081
	// DefaultWebPort is the default port for the web server.
	DefaultWebPort = 8080
	// DefaultMaxNumNamespaces is the default maximum number of namespaces that can be copied from the CosmosDB connector.
	cosmosDefaultMaxNumNamespaces = 8
)

var validModes = []string{iface.SyncModeFull, iface.SyncModeCDC}

const defaultMode = iface.SyncModeFull

type ListFlag struct {
	Values []string
}

func (f *ListFlag) Set(value string) error {
	value = strings.ReplaceAll(value, " ", "")
	f.Values = strings.Split(value, ",")
	return nil
}

func (f *ListFlag) String() string {
	return strings.Join(f.Values, ", ")
}

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
			Name:  "sourcetype",
			Usage: fmt.Sprintf("source database type (%s). When not specified, will autodetect using the source URI", strings.Join(validSources, ",")),
			Action: func(ctx *cli.Context, source string) error {
				if !slices.Contains(validSources, source) {
					return fmt.Errorf("unsupported sourcetype setting %v", source)
				}
				return nil
			},
			Category: "Endpoint Configuration",
			Required: false,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "source",
			Usage:    "source connection string",
			Aliases:  []string{"s"},
			Category: "Endpoint Configuration",
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "destination",
			Usage:    "destination connection string",
			Aliases:  []string{"d"},
			Category: "Endpoint Configuration",
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:  "destinationtype",
			Usage: fmt.Sprintf("destination database type (%s). When not specified, will autodetect using the destination URI", strings.Join(validDestinations, ",")),
			Action: func(ctx *cli.Context, destination string) error {
				if !slices.Contains(validDestinations, destination) {
					return fmt.Errorf("unsupported destinationtype setting %v", destination)
				}
				return nil
			},
			Required: false,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "metadata",
			Usage:    "metadata store connection string. Will default to the destination if not provided",
			Aliases:  []string{"m"},
			Category: "Endpoint Configuration",
			Required: false,
		}),
		altsrc.NewGenericFlag(&cli.GenericFlag{
			Name:     "namespace",
			Usage:    "list of namespaces 'db1,db2.collection' (comma-separated) to sync from on the source",
			Aliases:  []string{"ns", "nsFrom"},
			Category: "Flow Options",
			Value:    &ListFlag{},
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:     "verify",
			Usage:    "perform a data integrity check for an existing flow",
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
			Name:     "cosmos-deletes-cdc",
			Usage:    "generate CDC events for CosmosDB deletes",
			Category: "Cosmos DB-specific Options",
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
			Name:     "cosmos-source-max-namespaces",
			Usage:    "maximum number of namespaces that can be copied from the CosmosDB connector. Recommended to keep this number under 15 to avoid performance issues.",
			Value:    cosmosDefaultMaxNumNamespaces,
			Required: false,
			Category: "Cosmos DB-specific Options",
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "cosmos-server-timeout",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "cosmos-ping-timeout",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "resume-token-interval",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "writer-batch-size",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewInt64Flag(&cli.Int64Flag{
			Name:     "cosmos-doc-partition",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "cosmos-delete-interval",
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
			Name:     "parallel-integrity-check",
			Required: false,
			Hidden:   true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:     "cosmos-parallel-partition-workers",
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
