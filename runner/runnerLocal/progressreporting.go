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

	totalDocs     int64
	numDocsSynced int64

	changeStreamEvents int64
	deletesCaught      uint64

	throughput    float64
	nsProgressMap map[iface.Namespace]*iface.NameSpaceStatus //map key is namespace "db.col"
	namespaces    []iface.Namespace                          //use map and get the keys so print order is consistent

	tasksTotal     int64
	tasksStarted   int64
	tasksCompleted int64
}

type namespaceProgress struct {
	startTime     time.Time //get from reader
	totalDocs     int       //get from reader
	numDocsSynced int       //get from writer
	throughput    float64   //writer?
}

func (r *RunnerLocal) UpdateRunnerProgress(flowId iface.FlowID) {
	flowStatus, err := r.coord.GetFlowStatus(flowId)
	if err != nil {
		slog.Error("Failed to get flow status", err)
		return
	}
	srcStatus := flowStatus.SrcStatus

	r.runnerProgress.syncState = srcStatus.SyncState

	// Calculate throughput
	if !r.runnerProgress.startTime.IsZero() {
		elapsed := time.Since(r.runnerProgress.currTime).Seconds()
		docsSynced := srcStatus.ProgressMetrics.NumDocsSynced - r.runnerProgress.numDocsSynced
		if elapsed > 0 {
			r.runnerProgress.throughput = math.Floor(float64(docsSynced) / elapsed)
		}
	}
	r.runnerProgress.currTime = time.Now()
	r.runnerProgress.numNamespacesCompleted = srcStatus.ProgressMetrics.NumNamespacesSynced
	r.runnerProgress.totalNamespaces = srcStatus.ProgressMetrics.NumNamespaces
	r.runnerProgress.totalDocs = srcStatus.ProgressMetrics.EstimatedTotalDocCount
	r.runnerProgress.numDocsSynced = srcStatus.ProgressMetrics.NumDocsSynced
	r.runnerProgress.nsProgressMap = srcStatus.ProgressMetrics.NamespaceProgress

	r.runnerProgress.namespaces = srcStatus.ProgressMetrics.Namespaces
	r.runnerProgress.tasksTotal = srcStatus.ProgressMetrics.TasksTotal
	r.runnerProgress.tasksStarted = srcStatus.ProgressMetrics.TasksStarted
	r.runnerProgress.tasksCompleted = srcStatus.ProgressMetrics.TasksCompleted
	r.runnerProgress.changeStreamEvents = srcStatus.ProgressMetrics.ChangeStreamEvents
	r.runnerProgress.deletesCaught = srcStatus.ProgressMetrics.DeletesCaught

}

func (r *RunnerLocal) SetUpDisplay(app *tview.Application, errorText *tview.TextView) {
	r.tui.app = app
	headerTextView := tview.NewTextView().SetText("Dsync Progress Report").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	table := tview.NewTable()
	progressBarTextView := tview.NewTextView().SetText("Progress Bar").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	errorText.SetText("Error Logs\n").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(headerTextView, 0, 1, false).
		AddItem(table, 0, 1, false).
		AddItem(progressBarTextView, 1, 1, false).
		AddItem(errorText, 0, 1, false)
	r.tui.root = root //indices are 0, 1, 2, 3, corresponding to header, table, progressBar, and errorLogs respectively
	r.tui.app.SetRoot(root, true)
}

func (r *RunnerLocal) GetStatusReport() {
	//get tview components
	header := r.tui.root.GetItem(0).(*tview.TextView)
	header.Clear()

	table := r.tui.root.GetItem(1).(*tview.Table)
	table.Clear()

	progressBar := r.tui.root.GetItem(2).(*tview.TextView)
	progressBar.Clear()

	//get the time elapsed
	totalTimeElapsed := time.Since(r.runnerProgress.startTime)
	minutes := int(totalTimeElapsed.Minutes())
	seconds := int(totalTimeElapsed.Seconds()) % 60

	switch r.runnerProgress.syncState {
	case "Setup":
		headerString := fmt.Sprintf("Dsync Progress Report : %s\nTime Elapsed: %02d:%02d\nSetting up the sync\n", r.runnerProgress.syncState, minutes, seconds)
		header.SetText(headerString)
	case "ReadPlanning":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d\nCreating the read plan\n", r.runnerProgress.syncState, minutes, seconds)
		header.SetText(headerString)
	case "InitialSync":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d		%d/%d Namespaces synced		Docs Synced: %d	\n", r.runnerProgress.syncState, minutes, seconds, r.runnerProgress.numNamespacesCompleted, r.runnerProgress.totalNamespaces, r.runnerProgress.numDocsSynced)
		header.SetText(headerString)

		//set the table
		table.SetCellSimple(0, 0, "Namespace		")
		table.SetCellSimple(0, 1, "Percent Complete	")
		table.SetCellSimple(0, 2, "Tasks Completed	")
		table.SetCellSimple(0, 3, "Docs Copied		")
		table.SetCellSimple(0, 4, "Throughput: Docs/s")
		for row, key := range r.runnerProgress.namespaces {
			nsString := key.Db + "." + key.Col
			ns := r.runnerProgress.nsProgressMap[key]

			docsCopied := atomic.LoadInt64(&ns.DocsCopied)
			percentComplete, _, _ := percentCompleteNamespace(ns)

			table.SetCellSimple(row+1, 0, fmt.Sprintf("%s", nsString))
			table.SetCellSimple(row+1, 1, fmt.Sprintf("%.0f%%", percentComplete))
			table.SetCellSimple(row+1, 2, fmt.Sprintf("%d/%d", atomic.LoadInt64(&ns.TasksCompleted), len(ns.Tasks)))
			table.SetCellSimple(row+1, 3, fmt.Sprintf("%d", docsCopied))
			table.SetCellSimple(row+1, 4, fmt.Sprintf("%.0f", ns.Throughput))
		}

		progressBarWidth := 80

		totalPercentComplete := percentCompleteTotal(r.runnerProgress)
		slog.Debug(fmt.Sprintf("Total percent complete: %.2f", totalPercentComplete))

		progress := int(math.Floor((totalPercentComplete / 100 * float64(progressBarWidth))))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%		%.2f docs/sec\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete, r.runnerProgress.throughput)
		progressBar.SetText(progressBarString)

	case "ChangeStream":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d        %d/%d Namespaces synced\nProcessing change stream events\n\nChange Stream Events- %d		Deletes Caught- %d", r.runnerProgress.syncState, minutes, seconds, r.runnerProgress.numNamespacesCompleted, r.runnerProgress.totalNamespaces, r.runnerProgress.changeStreamEvents, r.runnerProgress.deletesCaught)
		header.SetText(headerString)

		progressBarWidth := 80
		cdcPaginatorPosition = (cdcPaginatorPosition + 1) % (progressBarWidth - 4)
		progressBarString := fmt.Sprintf("[%s%s%s]\n", strings.Repeat(string('-'), cdcPaginatorPosition), strings.Repeat(">", 3), strings.Repeat("-", progressBarWidth-cdcPaginatorPosition-2))
		progressBar.SetText(progressBarString)

	case "Cleanup":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d\nCleaning up flow data\n", r.runnerProgress.syncState, minutes, seconds)
		header.SetText(headerString)

	case "Verification":
		//set the header text

		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d\nPerforming Data Integrity Check", r.runnerProgress.syncState, minutes, seconds)
		header.SetText(headerString)
	}
	r.tui.app.Draw()
}

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
		numDocsCopied -= numCompletedDocs
		numDocsLeft := (int64(len(nsStatus.Tasks)) - nsStatus.TasksCompleted) * docsPerTask
		if numDocsCopied >= int64(numInProgressDocsMax) && len(nsStatus.Tasks) != int(atomic.LoadInt64(&nsStatus.TasksCompleted)) {
			//we are in the middle of a task
			numDocsCopied = int64(numInProgressDocsMax - 1)
		} else if numDocsCopied > int64(numInProgressDocsMax) && len(nsStatus.Tasks) == int(atomic.LoadInt64(&nsStatus.TasksCompleted)) {
			numDocsCopied = int64(numInProgressDocsMax)
		}
		percentComplete = float64(numCompletedDocs+numDocsCopied) / float64(numCompletedDocs+numDocsLeft) * 100
		numerator = float64(numCompletedDocs + numDocsCopied)
		denominator = float64(numCompletedDocs + numDocsLeft)
	}
	return percentComplete, numerator, denominator
}
