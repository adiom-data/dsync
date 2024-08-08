/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package dsync

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/internal/build"
	"github.com/adiom-data/dsync/logger"
	runner "github.com/adiom-data/dsync/runner/runnerLocal"
	"github.com/urfave/cli/v2"
)

// NewApp starts the dsync container process.
func NewApp() *cli.App {
	flags, before := options.GetFlagsAndBeforeFunc()

	app := &cli.App{
		Before:    before,
		Flags:     flags,
		Name:      "dsync",
		Usage:     "Copies data from the source to the destination",
		UsageText: "dsync [options]",
		Version:   build.VersionInfo(),
		Copyright: build.CopyrightStr,
		Action:    runDsync,
	}

	return app
}

func runDsync(c *cli.Context) error {
	o, err := options.NewFromCLIContext(c)
	if err != nil {
		return err
	}

	lo := logger.Options{Verbosity: o.Verbosity}

	logger.Setup(lo)
	slog.Debug(fmt.Sprintf("Parsed options: %+v", o))

	r := runner.NewRunnerLocal(runner.RunnerLocalSettings{
		SrcConnString:                   o.SrcConnString,
		DstConnString:                   o.DstConnString,
		SrcType:                         o.Sourcetype,
		StateStoreConnString:            o.StateStoreConnString,
		NsFromString:                    o.NamespaceFrom,
		VerifyRequestedFlag:             o.Verify,
		CleanupRequestedFlag:            o.Cleanup,
		FlowStatusReportingIntervalSecs: 10,
		CosmosDeletesEmuRequestedFlag:   o.CosmosDeletesEmu,
	})
	defer r.Teardown()

	err = r.Setup(c.Context)
	if err != nil {
		return err
	}

	err = r.Run()
	if err != nil {
		return err
	}

	return err
}
