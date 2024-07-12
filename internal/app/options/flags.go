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
const DefaultVerbosity = "DEBUG"

var validVerbosities = []string{"DEBUG", "INFO", "WARN", "ERROR"}

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
		altsrc.NewGenericFlag(&cli.GenericFlag{
			Name:    "namespace",
			Usage:   "List of namespaces 'db1,db2.collection' (comma-separated) to sync from on the source",
			Aliases: []string{"ns", "nsFrom"},
			Value:   &ListFlag{},
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "verify",
			Usage: "Perform a data integrity check for an existing flow",
		}),
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "Specify the path of the config file",
		},
		cli.VersionFlag,
	}

	return flags, altsrc.InitInputSourceWithContext(flags, altsrc.NewYamlSourceFromFlagFunc("config"))
}
