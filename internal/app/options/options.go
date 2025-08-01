/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package options

import (
	"time"

	"github.com/urfave/cli/v2"
)

type Options struct {
	Verbosity string

	StateStoreConnString string

	Logfile string

	NamespaceFrom []string

	Verify           bool
	VerifyQuickCount bool
	Cleanup          bool
	Progress         bool
	Reverse          bool

	Pprof bool

	PprofPort uint
	WebPort   uint

	LoadLevel                      string
	InitialSyncNumParallelCopiers  int
	NumParallelWriters             int
	NumParallelIntegrityCheckTasks int
	CdcResumeTokenUpdateInterval   time.Duration
	WriterMaxBatchSize             int
	Mode                           string
}

// works with a copy of the struct to avoid modifying the original
func RedactSensitiveInfo(o Options) Options {
	o.StateStoreConnString = "REDACTED"
	return o
}

func NewFromCLIContext(c *cli.Context) (Options, error) {
	o := Options{}

	o.Verbosity = c.String("verbosity")
	o.StateStoreConnString = c.String("metadata")
	o.Logfile = c.String("logfile")
	o.NamespaceFrom = c.StringSlice("namespace")
	o.Verify = c.Bool("verify")
	o.VerifyQuickCount = c.Bool("verify-quick-count")
	o.Cleanup = c.Bool("cleanup")
	o.Progress = c.Bool("progress")
	o.Pprof = c.Bool("pprof")
	o.LoadLevel = c.String("load-level")
	o.PprofPort = c.Uint("pprof-port")
	o.WebPort = c.Uint("web-port")

	o.InitialSyncNumParallelCopiers = c.Int("parallel-copiers")
	o.NumParallelWriters = c.Int("parallel-writers")
	o.NumParallelIntegrityCheckTasks = c.Int("parallel-integrity-check-workers")
	o.CdcResumeTokenUpdateInterval = time.Duration(c.Int("cdc-resume-token-interval")) * time.Second
	o.WriterMaxBatchSize = c.Int("writer-batch-size")
	o.Mode = c.String("mode")
	o.Reverse = c.Bool("reverse")

	return o, nil
}
