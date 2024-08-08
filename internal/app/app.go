/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package dsync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/internal/build"
	"github.com/adiom-data/dsync/logger"
	runner "github.com/adiom-data/dsync/runner/runnerLocal"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
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
	lo := logger.Options{Verbosity: o.Verbosity, Logfile: o.Logfile}
	logbuffer := bytes.Buffer{}

	logFile := logger.Setup(lo, logbuffer)

	if logFile != nil {
		defer logFile.Close()
	}

	defer func() {
		fmt.Printf("dsync has stopped running\n")
		if logbuffer.Len() > 0 {
			fmt.Printf("%v\n", logbuffer.String())
		} else {
			fmt.Printf("No logs generated\n")
		}
	}()

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

	var wg sync.WaitGroup
	runnerCtx, runnerCancelFunc := context.WithCancel(c.Context)

	if o.Progress {
		wg.Add(1)

		// Start the status reporting goroutine
		go func() {
			defer wg.Done()

			errorTextView := tview.NewTextView()
			tviewApp := tview.NewApplication()
			defer tviewApp.Stop()

			// Custom signal handler for Ctrl+C within tview
			tviewApp.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyCtrlC {
					tviewApp.Stop()
					runnerCancelFunc() // Cancel the runner
					return nil
				}
				return event
			})
			r.SetUpDisplay(tviewApp, errorTextView)

			// Start the status reporting goroutine
			go func() {
				for {
					select {
					case <-runnerCtx.Done():
						return
					default:
						r.GetStatusReport2()
						time.Sleep(1 * time.Second)
					}
				}

			}()

			if err := tviewApp.Run(); err != nil {
				slog.Error(fmt.Sprintf("Error running tview app: %v", err))
			}
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := r.Setup(runnerCtx)
		if err == nil {
			r.Run()
		}
		r.Teardown()

	}()

	wg.Wait()

	return nil
}
