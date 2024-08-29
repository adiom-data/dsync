/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package options

import (
	"fmt"
	"time"

	connectorMongo "github.com/adiom-data/dsync/connectors/mongo"
	"github.com/urfave/cli/v2"
)

type Options struct {
	Verbosity  string
	Sourcetype string

	SrcConnString        string
	DstConnString        string
	StateStoreConnString string

	Logfile string

	NamespaceFrom []string

	Verify   bool
	Cleanup  bool
	Progress bool

	CosmosDeletesEmu bool

	Pprof bool

	LoadLevel string
	InitialSyncNumParallelCopiers int
	NumParallelWriters int
	NumParallelIntegrityCheckTasks int
	NumParallelPartitionWorkers int
	MaxNumNamespaces int
	ServerConnectTimeout time.Duration
	PingTimeout time.Duration
	CdcResumeTokenUpdateInterval time.Duration
	WriterMaxBatchSize int
	TargetDocCountPerPartition int64
	DeletesCheckInterval time.Duration
}

func NewFromCLIContext(c *cli.Context) (Options, error) {
	o := Options{}

	o.Verbosity = c.String("verbosity")
	o.Sourcetype = c.String("sourcetype")
	o.SrcConnString = c.String("source")
	o.DstConnString = c.String("destination")
	o.StateStoreConnString = c.String("metadata")
	o.Logfile = c.String("logfile")
	o.NamespaceFrom = c.Generic("namespace").(*ListFlag).Values
	o.Verify = c.Bool("verify")
	o.Cleanup = c.Bool("cleanup")
	o.Progress = c.Bool("progress")
	o.Pprof = c.Bool("pprof")
	o.LoadLevel = c.String("load-level")
	o.InitialSyncNumParallelCopiers = c.Int("parallel-copiers")
	o.NumParallelWriters = c.Int("parallel-writers")
	o.NumParallelIntegrityCheckTasks = c.Int("parallel-integrity-check")
	o.NumParallelPartitionWorkers = c.Int("parallel-partition-workers")
	o.MaxNumNamespaces = c.Int("num-namespaces")
	o.ServerConnectTimeout = time.Duration(c.Int("server-timeout")) * time.Second
	o.PingTimeout = c.Duration("ping-timeout")
	o.CdcResumeTokenUpdateInterval = c.Duration("resume-token-interval")
	o.WriterMaxBatchSize = c.Int("writer-batch-size")
	o.TargetDocCountPerPartition = c.Int64("doc-partition")
	o.DeletesCheckInterval = c.Duration("delete-interval")
	

	// Infer source type if not provided
	if o.Sourcetype == "" && o.SrcConnString != "/dev/random" {
		mongoFlavor := connectorMongo.GetMongoFlavor(o.SrcConnString)
		if mongoFlavor == connectorMongo.FlavorCosmosDB {
			o.Sourcetype = "CosmosDB"
		} else {
			o.Sourcetype = "MongoDB"
		}
		fmt.Printf("Inferred source type: %v\n", o.Sourcetype)
	}

	o.CosmosDeletesEmu = c.Bool("cosmos-deletes-cdc")
	if o.Sourcetype != "CosmosDB" && o.CosmosDeletesEmu {
		return o, fmt.Errorf("cosmos-deletes-cdc flag is only valid for CosmosDB source")
	}
	if (o.DstConnString == "/dev/null") && o.CosmosDeletesEmu {
		// /dev/null doesn't offer a persistent index
		return o, fmt.Errorf("cosmos-deletes-cdc flag cannot be used with /dev/null destination")
	}

	return o, nil
}
