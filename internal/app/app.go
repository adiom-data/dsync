/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package dsync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/internal/build"
	"github.com/adiom-data/dsync/logger"
	runner "github.com/adiom-data/dsync/runners/local"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/urfave/cli/v2"
)

const (
	memoryUsagePrintInterval = 10 * time.Second
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

	if o.Pprof {
		go func() {
			http.ListenAndServe("localhost:8080", nil)
		}()
	}

	// set up logging
	lo := logger.Options{Verbosity: o.Verbosity}

	errorTextView := tview.NewTextView().SetScrollable(true).SetDynamicColors(true).ScrollToEnd()
	if o.Logfile != "" { // need to log to a file
		logFile, err := os.OpenFile(o.Logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
		defer logFile.Close()
		lo.Logfile = logFile

		if o.Progress { // log to tview as well
			lo.ErrorView = errorTextView
		}
	}
	logger.Setup(lo)

	slog.Info(fmt.Sprintf("Starting dsync %v", build.VersionInfo()))

	slog.Debug(fmt.Sprintf("Parsed options: %+v", o))

	var advancedProgressRecalcInterval time.Duration
	if !o.Progress {
		advancedProgressRecalcInterval = 0
	} else {
		advancedProgressRecalcInterval = throughputUpdateInterval
	}

	r := runner.NewRunnerLocal(runner.RunnerLocalSettings{
		SrcConnString:                  o.SrcConnString,
		DstConnString:                  o.DstConnString,
		SrcType:                        o.Sourcetype,
		StateStoreConnString:           o.StateStoreConnString,
		NsFromString:                   o.NamespaceFrom,
		VerifyRequestedFlag:            o.Verify,
		CleanupRequestedFlag:           o.Cleanup,
		FlowStatusReportingInterval:    10,
		CosmosDeletesEmuRequestedFlag:  o.CosmosDeletesEmu,
		AdvancedProgressRecalcInterval: advancedProgressRecalcInterval,
		LoadLevel:                      o.LoadLevel,
		CosmosInitialSyncNumParallelCopiers:  o.CosmosInitialSyncNumParallelCopiers,
		CosmosNumParallelWriters:			 	o.CosmosNumParallelWriters,
		CosmosNumParallelIntegrityCheckTasks: o.CosmosNumParallelIntegrityCheckTasks,
		CosmosNumParallelPartitionWorkers:    o.CosmosNumParallelPartitionWorkers,
		CosmosMaxNumNamespaces: 			    o.CosmosMaxNumNamespaces,
		CosmosServerConnectTimeout:           o.CosmosServerConnectTimeout, 
		CosmosPingTimeout:                    o.CosmosPingTimeout, 
		CosmosCdcResumeTokenUpdateInterval:   o.CosmosCdcResumeTokenUpdateInterval,
		CosmosWriterMaxBatchSize:       	    o.CosmosWriterMaxBatchSize,
		CosmosTargetDocCountPerPartition: 	o.CosmosTargetDocCountPerPartition,
		CosmosDeletesCheckInterval:           o.CosmosDeletesCheckInterval,
	})

	var wg sync.WaitGroup
	runnerCtx, runnerCancelFunc := context.WithCancel(c.Context)

	//start a goroutine to print memory usage
	go func() {
		for {
			select {
			case <-runnerCtx.Done():
				return
			default:
				slog.Debug(GetMemUsageString())
				time.Sleep(memoryUsagePrintInterval)
			}
		}
	}()

	var userInterrupted bool

	if o.Progress {
		wg.Add(1)

		// Start the status reporting goroutine
		go func() {
			defer wg.Done()

			tviewApp := tview.NewApplication()
			defer tviewApp.Stop()

			// Custom signal handler for Ctrl+C within tview
			tviewApp.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
				if event.Key() == tcell.KeyCtrlC {
					userInterrupted = true
					tviewApp.Stop()
					runnerCancelFunc() // Cancel the runner
					return nil
				}
				return event
			})
			tv := &TViewDetails{}
			tv.SetUpDisplay(tviewApp, errorTextView)

			// Start the status reporting goroutine
			go func() {
				for {
					select {
					case <-runnerCtx.Done():
						tviewApp.Stop() //need to make sure we stop the tview app when context is cancelled
						return
					default:
						r.UpdateRunnerProgress()
						tv.GetStatusReport(r.GetRunnerProgress())
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

	var runnerErr error
	go func() {
		defer wg.Done()
		err := r.Setup(runnerCtx)
		if err == nil {
			err = r.Run()
			if !o.Verify { //if verification was requested, the user should be able to see the results
				runnerCancelFunc()
			}
		} else {
			slog.Error(fmt.Sprintf("%v", err))
			runnerCancelFunc() //stop tview since we failed
		}
		r.Teardown()
		runnerErr = err
	}()

	wg.Wait()

	if userInterrupted {
		fmt.Println("user interrupted the process")
	}

	return runnerErr
}

// Gets the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func GetMemUsageString() string {
	var m runtime.MemStats
	str := ""
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	str += fmt.Sprintf("Alloc = %v MiB", bToMb(m.Alloc))
	str += fmt.Sprintf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	str += fmt.Sprintf("\tSys = %v MiB", bToMb(m.Sys))
	str += fmt.Sprintf("\tNumGC = %v", m.NumGC)
	return str
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
