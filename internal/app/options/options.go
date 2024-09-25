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
	Verbosity       string
	Sourcetype      string
	Destinationtype string

	SrcConnString        string
	DstConnString        string
	StateStoreConnString string

	Logfile string

	NamespaceFrom []string

	Verify   bool
	Cleanup  bool
	Progress bool
	Reverse  bool

	CosmosDeletesEmu bool

	Pprof bool

	PprofPort uint
	WebPort   uint

	LoadLevel                            string
	InitialSyncNumParallelCopiers  int
	NumParallelWriters             int
	NumParallelIntegrityCheckTasks int
	CosmosNumParallelPartitionWorkers    int
	CosmosReaderMaxNumNamespaces               int
	ServerConnectTimeout           time.Duration
	PingTimeout                    time.Duration
	CdcResumeTokenUpdateInterval   time.Duration
	WriterMaxBatchSize             int
	CosmosTargetDocCountPerPartition     int64
	CosmosDeletesCheckInterval           time.Duration
	Mode                                 string
}

func NewFromCLIContext(c *cli.Context) (Options, error) {
	o := Options{}

	o.Verbosity = c.String("verbosity")
	o.Sourcetype = c.String("sourcetype")
	o.Destinationtype = c.String("destinationtype")
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
	o.PprofPort = c.Uint("pprof-port")
	o.WebPort = c.Uint("web-port")

	o.InitialSyncNumParallelCopiers = c.Int("parallel-copiers")
	o.NumParallelWriters = c.Int("parallel-writers")
	o.NumParallelIntegrityCheckTasks = c.Int("parallel-integrity-check")
	o.CosmosNumParallelPartitionWorkers = c.Int("cosmos-parallel-partition-workers")
	o.CosmosReaderMaxNumNamespaces = c.Int("cosmos-reader-max-namespaces")
	o.ServerConnectTimeout = time.Duration(c.Int("server-timeout")) * time.Second
	o.PingTimeout = time.Duration(c.Int("ping-timeout")) * time.Second
	o.CdcResumeTokenUpdateInterval = time.Duration(c.Int("cdc-resume-token-interval")) * time.Second
	o.WriterMaxBatchSize = c.Int("writer-batch-size")
	o.CosmosTargetDocCountPerPartition = c.Int64("cosmos-doc-partition")
	o.CosmosDeletesCheckInterval = time.Duration(c.Int("cosmos-delete-interval")) * time.Second
	o.Mode = c.String("mode")
	o.Reverse = c.Bool("reverse")

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

	// Infer destination type if not provided
	if o.Destinationtype == "" && o.DstConnString != "/dev/null" {
		mongoFlavor := connectorMongo.GetMongoFlavor(o.DstConnString)
		if mongoFlavor == connectorMongo.FlavorCosmosDB {
			o.Destinationtype = "CosmosDB"
		} else {
			o.Destinationtype = "MongoDB"
		}
		fmt.Printf("Inferred destination type: %v\n", o.Destinationtype)
	}

	o.CosmosDeletesEmu = c.Bool("cosmos-deletes-cdc")
	if !o.Reverse && o.Sourcetype != "CosmosDB" && o.CosmosDeletesEmu {
		return o, fmt.Errorf("cosmos-deletes-cdc flag is only valid for CosmosDB source")
	}
	if !o.Reverse && (o.DstConnString == "/dev/null") && o.CosmosDeletesEmu {
		// /dev/null doesn't offer a persistent index
		return o, fmt.Errorf("cosmos-deletes-cdc flag cannot be used with /dev/null destination")
	}

	// can't use /dev/null connector as destination when reverse is enabled
	if o.Reverse && o.DstConnString == "/dev/null" {
		return o, fmt.Errorf("reverse flag cannot be used with /dev/null destination")
	}
	// can't use /dev/random connector as source when reverse is enabled
	if o.Reverse && o.SrcConnString == "/dev/random" {
		return o, fmt.Errorf("reverse flag cannot be used with /dev/random source")
	}

	return o, nil
}
