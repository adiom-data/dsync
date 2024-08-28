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

	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

// DefaultVerbosity is the default verbosity level for the application.
const DefaultVerbosity = "INFO"

var DefaultMaxNumNamespaces = 8

var validVerbosities = []string{"DEBUG", "INFO", "WARN", "ERROR"}

var validSources = []string{"MongoDB", "CosmosDB"}

var validLoadLevels = []string{"Low", "Medium", "High", "Beast"}

type ListFlag struct {
	Values []string
}

func (f *ListFlag) Set(value string) error {
	value = strings.ReplaceAll(value, " ", "")
	f.Values = strings.Split(value, ",")
	return nil
}

func (f *ListFlag) String() string {
	return strings.Join(f.Values, ",")
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
			Required: false,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "source",
			Usage:    "source connection string",
			Aliases:  []string{"s"},
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "destination",
			Usage:    "destination connection string",
			Aliases:  []string{"d"},
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "metadata",
			Usage:    "metadata store connection string. Will default to the destination if not provided",
			Aliases:  []string{"m"},
			Required: false,
		}),
		altsrc.NewGenericFlag(&cli.GenericFlag{
			Name:    "namespace",
			Usage:   "list of namespaces 'db1,db2.collection' (comma-separated) to sync from on the source",
			Aliases: []string{"ns", "nsFrom"},
			Value:   &ListFlag{},
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "verify",
			Usage: "perform a data integrity check for an existing flow",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "cleanup",
			Usage: "cleanup metadata for an existing flow",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "cosmos-deletes-cdc",
			Usage: "generate CDC events for CosmosDB deletes",
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
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "pprof",
			Usage: "enable pprof profiling on localhost:8080",
		}),
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "specify the path of the config file",
		},
		cli.VersionFlag,
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "num-namespaces",
			Usage: "maximum number of namespaces that can be copied. Recommended to keep this number under 15 to avoid performance issues",
			Required: false,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "server-timeout",
			Usage: "duration for Cosmos server connection timeout. Set a higher value for slower connections",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "ping-timeout",
			Usage: "duration for ping request timeout. Set a higher value for slower connections",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "resume-token-interval",
			Usage: "interval in seconds to update the resume token",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "writer-batch-size",
			Usage: "number of documents to write in a batch. Defaults to 0 for no limit",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewInt64Flag(&cli.Int64Flag{
			Name:  "doc-partition",
			Usage: "target number of docs per partition. 0 means no limit in # of documents",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "delete-interval",
			Usage: "interval in seconds to check and emulate deletes",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "parallel-copiers",
			Usage: "number of initial sync parallel copiers",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "parallel-writers",
			Usage: "number of initial sync parallel writers",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "parallel-integrity-check",
			Usage: "number of parallel integrity check tasks",
			Required: false,
			Hidden: true,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:  "parallel-partition-workers",
			Usage: "number of parallel partition workers",
			Required: false,
			Hidden: true,
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
