/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package runnerLocal

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/rivo/tview"
)

const (
	targetDocCountPerPartition = 512 * 1000
	throughputUpdateInterval   = 10 * time.Second
)

var (
	cdcPaginatorPosition = 0
)

type tviewDetails struct {
	app  *tview.Application
	root *tview.Flex
}

type runnerSyncProgress struct {
	startTime time.Time
	currTime  time.Time
	syncState string

	totalNamespaces        int64
	numNamespacesCompleted int64

	numDocsSynced int64

	changeStreamEvents int64
	deletesCaught      uint64

	throughput    float64
	nsProgressMap map[iface.Namespace]*iface.NameSpaceStatus
	namespaces    []iface.Namespace //use map and get the keys so print order is consistent

	tasksTotal     int64
	tasksStarted   int64
	tasksCompleted int64
}

// Update the runner progress struct with the latest progress metrics from the flow status
func (r *RunnerLocal) UpdateRunnerProgress(flowId iface.FlowID) {
	flowStatus, err := r.coord.GetFlowStatus(flowId)
	if err != nil {
		slog.Error("Failed to get flow status", err)
		return
	}
	srcStatus := flowStatus.SrcStatus

	r.runnerProgress.syncState = srcStatus.SyncState
	r.runnerProgress.currTime = time.Now()
	r.runnerProgress.numNamespacesCompleted = srcStatus.ProgressMetrics.NumNamespacesCompleted
	r.runnerProgress.totalNamespaces = srcStatus.ProgressMetrics.NumNamespaces
	r.runnerProgress.numDocsSynced = srcStatus.ProgressMetrics.NumDocsSynced
	r.runnerProgress.nsProgressMap = srcStatus.ProgressMetrics.NamespaceProgress

	r.runnerProgress.namespaces = srcStatus.ProgressMetrics.Namespaces
	r.runnerProgress.tasksTotal = srcStatus.ProgressMetrics.TasksTotal
	r.runnerProgress.tasksStarted = srcStatus.ProgressMetrics.TasksStarted
	r.runnerProgress.tasksCompleted = srcStatus.ProgressMetrics.TasksCompleted
	r.runnerProgress.changeStreamEvents = srcStatus.ProgressMetrics.ChangeStreamEvents
	r.runnerProgress.deletesCaught = srcStatus.ProgressMetrics.DeletesCaught

}

// Set up the initial display for the tview application, with the header, table, progress bar, and error logs components
func (r *RunnerLocal) SetUpDisplay(app *tview.Application, errorText *tview.TextView) {
	r.tui.app = app
	headerTextView := tview.NewTextView().SetText("Dsync Progress Report").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	table := tview.NewTable()
	progressBarTextView := tview.NewTextView().SetText("Progress Bar").SetDynamicColors(true).SetRegions(true).SetWrap(true).SetWordWrap(true)
	errorText.SetText("Error Logs\n").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(headerTextView, 0, 1, false).
		AddItem(table, 0, 1, false).
		AddItem(progressBarTextView, 1, 1, false).
		AddItem(errorText, 0, 1, false)
	r.tui.root = root //indices are 0, 1, 2, 3, corresponding to header, table, progressBar, and errorLogs respectively
	r.tui.app.SetRoot(root, true)
}

// Get the latest status report based on the runner progress struct and update the tview components accoringly
func (r *RunnerLocal) GetStatusReport() {
	//get tview components and clear them
	header := r.tui.root.GetItem(0).(*tview.TextView)
	header.Clear()

	table := r.tui.root.GetItem(1).(*tview.Table)
	table.Clear()

	progressBar := r.tui.root.GetItem(2).(*tview.TextView)
	progressBar.Clear()

	//get the time elapsed
	totalTimeElapsed := time.Since(r.runnerProgress.startTime)
	hours := int(totalTimeElapsed.Hours())
	minutes := int(totalTimeElapsed.Minutes()) % 60
	seconds := int(totalTimeElapsed.Seconds()) % 60

	switch r.runnerProgress.syncState {
	case "Setup":
		headerString := fmt.Sprintf("Dsync Progress Report : %s\nTime Elapsed: %02d:%02d:%02d\nSetting up the sync\n", r.runnerProgress.syncState, hours, minutes, seconds)
		header.SetText(headerString)
	case "ReadPlanning":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nCreating the read plan\n", r.runnerProgress.syncState, hours, minutes, seconds)
		header.SetText(headerString)
	case "InitialSync":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d		%d/%d Namespaces synced		Docs Synced: %d	\n", r.runnerProgress.syncState, hours, minutes, seconds, r.runnerProgress.numNamespacesCompleted, r.runnerProgress.totalNamespaces, r.runnerProgress.numDocsSynced)
		header.SetText(headerString)

		//set the table
		table.SetCell(0, 0, tview.NewTableCell("Namespace").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 1, tview.NewTableCell("Percent Complete").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 2, tview.NewTableCell("Tasks Completed").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 3, tview.NewTableCell("Docs Synced").SetAlign(tview.AlignLeft).SetExpansion(1))
		table.SetCell(0, 4, tview.NewTableCell("Throughput: Docs/s").SetAlign(tview.AlignLeft).SetExpansion(1))
		for row, key := range r.runnerProgress.namespaces {
			nsString := key.Db + "." + key.Col
			ns := r.runnerProgress.nsProgressMap[key]

			docsCopied := atomic.LoadInt64(&ns.DocsCopied)
			percentComplete, _, _ := percentCompleteNamespace(ns)

			table.SetCell(row+1, 0, tview.NewTableCell(nsString).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 1, tview.NewTableCell(fmt.Sprintf("%.0f%%", percentComplete)).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 2, tview.NewTableCell(fmt.Sprintf("%d/%d", atomic.LoadInt64(&ns.TasksCompleted), len(ns.Tasks))).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 3, tview.NewTableCell(fmt.Sprintf("%d", docsCopied)).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 4, tview.NewTableCell(fmt.Sprintf("%.0f", ns.Throughput)).SetAlign(tview.AlignLeft).SetExpansion(1))
		}

		//set the progress bar
		progressBarWidth := 80

		totalPercentComplete := percentCompleteTotal(r.runnerProgress)

		progress := int(math.Floor((totalPercentComplete / 100 * float64(progressBarWidth))))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%		%.2f docs/sec\n\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete, r.runnerProgress.throughput)
		progressBar.SetText(progressBarString)

	case "ChangeStream":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d        %d/%d Namespaces synced\nProcessing change stream events\n\nChange Stream Events- %d		Deletes Caught- %d		%.2f Events/Sec", r.runnerProgress.syncState, hours, minutes, seconds, r.runnerProgress.numNamespacesCompleted, r.runnerProgress.totalNamespaces, r.runnerProgress.changeStreamEvents, r.runnerProgress.deletesCaught, r.runnerProgress.throughput)
		header.SetText(headerString)

		//set the indefinite progress bar
		progressBarWidth := 80
		cdcPaginatorPosition = (cdcPaginatorPosition + 1) % (progressBarWidth - 4)
		progressBarString := fmt.Sprintf("[%s%s%s]\n", strings.Repeat(string('-'), cdcPaginatorPosition), strings.Repeat(">", 3), strings.Repeat("-", progressBarWidth-cdcPaginatorPosition-2))
		progressBar.SetText(progressBarString)

	case "Cleanup":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nCleaning up flow data\n", r.runnerProgress.syncState, hours, minutes, seconds)
		header.SetText(headerString)

	case "Verification":
		//set the header text

		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nPerforming Data Integrity Check", r.runnerProgress.syncState, hours, minutes, seconds)
		header.SetText(headerString)

	default:
		headerString := "This connector does not support progress reporting yet\n"
		header.SetText(headerString)
	}
	r.tui.app.Draw()
}

// Calculate the total percent complete for all namespaces
func percentCompleteTotal(progress runnerSyncProgress) float64 {
	var percentComplete float64
	docsCopied, totalDocs := float64(0), float64(0)
	for _, ns := range progress.nsProgressMap {
		_, numerator, denominator := percentCompleteNamespace(ns)
		docsCopied += numerator
		totalDocs += denominator
	}
	percentComplete = docsCopied / totalDocs * 100

	return percentComplete

}

// Calculates the percent complete for the given namespace, returns (percentComplete, numerator, denominator)
func percentCompleteNamespace(nsStatus *iface.NameSpaceStatus) (float64, float64, float64) {
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

		docsPerTask := nsStatus.Tasks[0].Def.EstimatedDocCount
		numCompletedDocs := atomic.LoadInt64(&nsStatus.TasksCompleted) * docsPerTask

		numInProgressDocsMax := int64(atomic.LoadInt64(&nsStatus.TasksStarted)) * docsPerTask
		numDocsInProgress := numDocsCopied - numCompletedDocs
		numDocsLeft := (int64(len(nsStatus.Tasks)) - nsStatus.TasksCompleted) * docsPerTask
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
	return percentComplete, numerator, denominator
}
