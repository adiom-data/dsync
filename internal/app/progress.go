/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package dsync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/logger"
	"github.com/adiom-data/dsync/protocol/iface"
	runnerLocal "github.com/adiom-data/dsync/runners/local"
	"github.com/rivo/tview"
)

const (
	throughputUpdateInterval = 10 * time.Second
)

var (
	cdcPaginatorPosition = 0
)

type TViewDetails struct {
	app  *tview.Application
	root *tview.Flex
}

// Set up the initial display for the tview application, with the header, table, progress bar, and error logs components
func (tv *TViewDetails) SetUpDisplay(app *tview.Application, errorText *tview.TextView) {
	tv.app = app
	headerTextView := tview.NewTextView().SetText("Dsync Progress Report").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	table := tview.NewTable()
	progressBarTextView := tview.NewTextView().SetText("Progress Bar").SetDynamicColors(true).SetRegions(true).SetWrap(true).SetWordWrap(true)
	errorText.SetText("Error Logs\n").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(headerTextView, 0, 1, false).
		AddItem(table, 0, 1, false).
		AddItem(progressBarTextView, 1, 1, false).
		AddItem(errorText, 0, 1, false)
	tv.root = root //indices are 0, 1, 2, 3, corresponding to header, table, progressBar, and errorLogs respectively
	tv.app.SetRoot(root, true)
}

// Get the latest status report based on the runner progress struct and update the tview components accordingly
func (tv *TViewDetails) GetStatusReport(runnerProgress runnerLocal.RunnerSyncProgress) {
	//get tview components and clear them
	header := tv.root.GetItem(0).(*tview.TextView)
	header.Clear()

	table := tv.root.GetItem(1).(*tview.Table)
	table.Clear()

	progressBar := tv.root.GetItem(2).(*tview.TextView)
	progressBar.Clear()

	//get the time elapsed
	totalTimeElapsed := time.Since(runnerProgress.StartTime)
	hours := int(totalTimeElapsed.Hours())
	minutes := int(totalTimeElapsed.Minutes()) % 60
	seconds := int(totalTimeElapsed.Seconds()) % 60

	switch runnerProgress.SyncState {
	case iface.SetupSyncState:
		headerString := fmt.Sprintf("Dsync Progress Report : %s\nTime Elapsed: %02d:%02d:%02d\nSetting up the sync\n", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)
	case iface.ReadPlanningSyncState:
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nCreating the read plan\n", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)
	case iface.InitialSyncSyncState:
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d		%d/%d Namespaces synced		Docs Synced: %d\n", runnerProgress.SyncState, hours, minutes, seconds, runnerProgress.NumNamespacesCompleted, runnerProgress.TotalNamespaces, runnerProgress.NumDocsSynced)
		if runnerProgress.AdditionalStateInfo != "" {
			headerString += "\n" + runnerProgress.AdditionalStateInfo
		}
		header.SetText(headerString)

		//set the table
		table.SetCell(0, 0, tview.NewTableCell("Namespace").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 1, tview.NewTableCell("Percent Complete").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 2, tview.NewTableCell("Tasks Completed").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 3, tview.NewTableCell("Docs Synced").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 4, tview.NewTableCell("Throughput: Docs/s").SetAlign(tview.AlignLeft).SetExpansion(1))
		for row, key := range runnerProgress.Namespaces {
			nsString := key.Db + "." + key.Col
			ns := runnerProgress.NsProgressMap[key]

			docsCopied := atomic.LoadInt64(&ns.DocsCopied)
			percentComplete, _, _ := percentCompleteNamespace(ns)

			table.SetCell(row+1, 0, tview.NewTableCell(nsString).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 1, tview.NewTableCell(fmt.Sprintf("%.0f%%", percentComplete)).SetAlign(tview.AlignLeft).SetExpansion(1))

			tasksInProgressString := ""
			if ns.TasksStarted > 0 {
				tasksInProgressString = fmt.Sprintf(" (%d active)", ns.TasksStarted)
			}
			table.SetCell(row+1, 2, tview.NewTableCell(fmt.Sprintf("%d/%d%s", atomic.LoadInt64(&ns.TasksCompleted), len(ns.Tasks), tasksInProgressString)).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 3, tview.NewTableCell(fmt.Sprintf("%d", docsCopied)).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 4, tview.NewTableCell(fmt.Sprintf("%.0f", ns.Throughput)).SetAlign(tview.AlignLeft).SetExpansion(1))
		}

		//set the progress bar
		progressBarWidth := 80

		totalPercentComplete := percentCompleteTotal(runnerProgress)

		progress := int(math.Floor(totalPercentComplete / 100 * float64(progressBarWidth)))
		progress = min(progressBarWidth, max(0, progress))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%		%.2f docs/sec\n\n",
			strings.Repeat(string('#'), progress),
			strings.Repeat(" ", max(0, progressBarWidth-progress)),
			totalPercentComplete, runnerProgress.Throughput)
		progressBar.SetText(progressBarString)

	case iface.ChangeStreamSyncState:
		//TODO: Don't print deletes info if deletes emulation isn't enabled (applies to the web server as well)
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d        %d/%d Namespaces synced\nProcessing change stream events\n\nChange Stream Events- %d		Delete Events- %d		Events to catch up: %d",
			runnerProgress.SyncState, hours, minutes, seconds, runnerProgress.NumNamespacesCompleted, runnerProgress.TotalNamespaces, runnerProgress.ChangeStreamEvents, runnerProgress.DeletesCaught, runnerProgress.Lag)

		if runnerProgress.AdditionalStateInfo != "" {
			headerString += "\n" + runnerProgress.AdditionalStateInfo
		}
		header.SetText(headerString)

		//set the indefinite progress bar
		progressBarWidth := 80
		cdcPaginatorPosition = (cdcPaginatorPosition + 5) % (progressBarWidth - 4)
		progressBarString := fmt.Sprintf("[%s%s%s] %.2f events/sec\n\n",
			strings.Repeat(string('-'), max(0, cdcPaginatorPosition)),
			strings.Repeat(">", 3),
			strings.Repeat("-", max(0, progressBarWidth-cdcPaginatorPosition-2)),
			runnerProgress.Throughput)
		progressBar.SetText(progressBarString)

	case iface.CleanupSyncState:
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nCleaning up flow data\n", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)

	case iface.VerifySyncState:
		//set the header text
		stateString := "Performing Data Integrity Check"
		if runnerProgress.VerificationResult != "" {
			stateString = fmt.Sprintf("Data Integrity Check: %s        Press Ctrl+C to exit", runnerProgress.VerificationResult)
		}
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\n%s", runnerProgress.SyncState, hours, minutes, seconds, stateString)
		header.SetText(headerString)

		table.SetCell(0, 0, tview.NewTableCell("Namespace").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 1, tview.NewTableCell("Percent Complete").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 2, tview.NewTableCell("Tasks Completed").SetAlign(tview.AlignLeft).SetExpansion(1))

		numerator := 0
		denominator := 0
		for row, status := range runnerProgress.NamespaceVerifyProgresses {
			numerator += status.TasksCompleted
			denominator += status.TasksTotal
			percentComplete := float64(status.TasksCompleted) / float64(status.TasksTotal) * 100

			table.SetCell(row+1, 0, tview.NewTableCell(status.Namespace).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 1, tview.NewTableCell(fmt.Sprintf("%.0f%%", percentComplete)).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 2, tview.NewTableCell(fmt.Sprintf("%d/%d", status.TasksCompleted, status.TasksTotal)).SetAlign(tview.AlignLeft).SetExpansion(1))
		}

		//set the progress bar
		progressBarWidth := 80

		if denominator < 1 {
			denominator = 1
		}

		totalPercentComplete := float64(numerator) / float64(denominator) * 100

		progress := int(math.Floor(totalPercentComplete / 100 * float64(progressBarWidth)))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%\n\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete)
		progressBar.SetText(progressBarString)

	default:
		headerString := "This connector does not support progress reporting yet\n"
		header.SetText(headerString)
	}
	tv.app.Draw()
}

// Calculate the total percent complete for all namespaces
func percentCompleteTotal(progress runnerLocal.RunnerSyncProgress) float64 {
	var percentComplete float64
	docsCopied, totalDocs := float64(0), float64(0)
	for _, ns := range progress.NsProgressMap {
		_, numerator, denominator := percentCompleteNamespace(ns)
		docsCopied += numerator
		totalDocs += denominator
	}
	if totalDocs == 0 { // don't divide by 0
		return 0
	}

	percentComplete = docsCopied / totalDocs * 100

	return min(100, percentComplete)

}

// Calculates the percent complete for the given namespace, returns (percentComplete, numerator, denominator)
func percentCompleteNamespace(nsStatus *iface.NamespaceStatus) (float64, float64, float64) {
	var percentComplete float64
	var numerator, denominator float64
	if len(nsStatus.Tasks) == 1 {
		//no partitioning
		docCount := nsStatus.EstimatedDocCount
		if docCount == 0 {
			percentComplete = 100
			numerator = 1
			denominator = 1
		} else {
			percentComplete = float64(atomic.LoadInt64(&nsStatus.DocsCopied)) / float64(docCount) * 100
			numerator = float64(atomic.LoadInt64(&nsStatus.DocsCopied))
			denominator = float64(docCount)
		}
	} else {
		//partitioning
		numDocsCopied := atomic.LoadInt64(&nsStatus.EstimatedDocsCopied)

		// iterate over the tasks and calculate the necessary totals
		numCompletedDocs := int64(0)
		numInProgressDocsMax := int64(0)
		numDocsTotal := int64(0)
		for _, task := range nsStatus.Tasks {
			numDocsTotal += task.EstimatedDocCount
			if task.Status == iface.ReadPlanTaskStatus_Completed {
				numCompletedDocs += task.EstimatedDocCount
			}
			if nsStatus.ActiveTasksList[task.Id] {
				numInProgressDocsMax += task.EstimatedDocCount
			}
		}

		numDocsInProgress := numDocsCopied - numCompletedDocs
		numDocsLeft := numDocsTotal - numCompletedDocs

		if numDocsInProgress >= int64(numInProgressDocsMax) && len(nsStatus.Tasks) != int(atomic.LoadInt64(&nsStatus.TasksCompleted)) {
			//we are in the middle of a task
			numDocsInProgress = int64(numInProgressDocsMax - 1)
		} else if numDocsInProgress > int64(numInProgressDocsMax) && len(nsStatus.Tasks) == int(atomic.LoadInt64(&nsStatus.TasksCompleted)) {
			numDocsInProgress = int64(numInProgressDocsMax)
		}
		percentComplete = float64(numCompletedDocs+numDocsInProgress) / float64(numCompletedDocs+numDocsLeft) * 100
		numerator = float64(numCompletedDocs + numDocsInProgress)
		denominator = float64(numCompletedDocs + numDocsLeft)
	}
	return min(100, percentComplete), numerator, denominator
}

// Convert the iface.Namespace key to a string ("db.col") to support JSON marshal
func convertNamespaceMapToStringKeys(originalMap map[iface.Namespace]*iface.NamespaceStatus) map[string]*iface.NamespaceStatus {
	newMap := make(map[string]*iface.NamespaceStatus)

	for key, value := range originalMap {
		newKey := fmt.Sprintf("%s.%s", key.Db, key.Col)
		newMap[newKey] = value
	}
	return newMap
}

// Push updates using SSE server
func progressUpdatesHandler(ctx context.Context, runner *runnerLocal.RunnerLocal, errorLog *logger.ReverseBuffer, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Debug("Context cancelled, stopping progress updates.")
			return
		case <-ticker.C:
			runner.UpdateRunnerProgress()
			progress := runner.GetRunnerProgress()
			elapsed := time.Since(progress.StartTime).Round(time.Second)
			convertedNamespaceMap := convertNamespaceMapToStringKeys(progress.NsProgressMap)

			// Construct data struct
			data := struct {
				RunnerSyncProgress runnerLocal.RunnerSyncProgress
				Elapsed            string
				TotalProgress      int64
				TotalThroughput    float64
				ErrorLogString     string
				NsProgressMap      map[string]*iface.NamespaceStatus
			}{
				RunnerSyncProgress: progress,
				Elapsed:            elapsed.String(),
				TotalProgress:      int64(percentCompleteTotal(progress)),
				TotalThroughput:    progress.Throughput,
				ErrorLogString:     errorLog.String(),
				NsProgressMap:      convertedNamespaceMap,
			}

			// Convert data to JSON and send as event to client
			jsonData, err := json.Marshal(data)
			if err != nil {
				slog.Error("Error marshaling JSON:", "err", err)
				break
			}
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}

			if progress.VerificationResult != "" {
				// Verification is complete, send final update and stop
				slog.Debug("Verification complete, stopping progress updates.")
				return
			}
		}
	}
}
