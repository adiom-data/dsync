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
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/rivo/tview"
)

const (
	targetDocCountPerPartition = 512 * 1000
)

var (
	cdcPaginatorPosition = 0
)

type tviewDetails struct {
	app  *tview.Application
	root *tview.Flex
}
type runnerSyncProgress struct {
	startTime           time.Time
	currTime            time.Time
	syncState           string
	totalNamespaces     int64
	numNamespacesSynced int64
	totalDocs           int64
	numDocsSynced       int64
	throughput          float64
	nsProgressMap       map[string]*iface.NameSpaceStatus //map key is namespace "db.col"
	namespaces          []iface.Namespace                 //use map and get the keys so print order is consistent

	tasksTotal     int64
	tasksStarted   int64
	tasksCompleted int64
}

type namespaceProgress struct {
	startTime     time.Time //get from reader
	totalDocs     int       //get from reader
	numDocsSynced int       //get from writer
	throughput    int       //writer?
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
	r.runnerProgress.numNamespacesSynced = srcStatus.ProgressMetrics.NumNamespacesSynced
	r.runnerProgress.totalNamespaces = srcStatus.ProgressMetrics.NumNamespaces
	r.runnerProgress.totalDocs = srcStatus.EstimatedTotalDocCount
	r.runnerProgress.numDocsSynced = srcStatus.ProgressMetrics.NumDocsSynced
	r.runnerProgress.nsProgressMap = srcStatus.NamespaceProgress

	r.runnerProgress.namespaces = srcStatus.Namespaces
	r.runnerProgress.tasksTotal = srcStatus.ProgressMetrics.TasksTotal
	r.runnerProgress.tasksStarted = srcStatus.ProgressMetrics.TasksStarted
	r.runnerProgress.tasksCompleted = srcStatus.ProgressMetrics.TasksCompleted

}

func (r *RunnerLocal) SetUpDisplay(app *tview.Application, errorText *tview.TextView) {
	r.tui.app = app
	headerTextView := tview.NewTextView().SetText("Dsync Progress Report").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	table := tview.NewTable()
	progressBarTextView := tview.NewTextView().SetText("Progress Bar").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	errorText.SetText("Error Logs").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(headerTextView, 0, 1, false).
		AddItem(table, 0, 1, false).
		AddItem(progressBarTextView, 1, 1, false).
		AddItem(errorText, 0, 1, false)
	r.tui.root = root //indices are 0, 1, 2, 3, corresponding to header, table, progressBar, and errorLogs respectively
	r.tui.app.SetRoot(root, true)
}

func (r *RunnerLocal) GetStatusReport2() {
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
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d		Throughput: %f docs/sec			%d/%d Namespaces synced\n", r.runnerProgress.syncState, minutes, seconds, r.runnerProgress.throughput, r.runnerProgress.numNamespacesSynced, r.runnerProgress.totalNamespaces)
		header.SetText(headerString)

		//set the table
		for row, key := range r.runnerProgress.namespaces {
			nsString := key.Db + "." + key.Col
			ns := r.runnerProgress.nsProgressMap[nsString]

			tasksString := fmt.Sprintf("%d/%d Tasks completed", ns.TasksCompleted.Load(), len(ns.Tasks))

			//no partitioning
			percentComplete := float64(ns.DocsCopied.Load()) / float64(ns.EstimatedDocCount) * 100
			if percentComplete >= 100 && ns.TasksCompleted.Load() != int64(len(ns.Tasks)) {
				percentComplete = 99.9
			}
			//numCompleteTasks := ns.TasksCompleted.Load()
			//percentComplete := math.Floor(float64(ns.DocsCopied.Load()) / float64(ns.EstimatedDocCount) * 100)
			percentCompleteStr := fmt.Sprintf(" %.0f%% complete ", percentComplete)
			timeElapsed := time.Since(ns.StartTime)
			minutes := int(timeElapsed.Minutes())
			seconds := int(timeElapsed.Seconds()) % 60
			timeElapsedStr := fmt.Sprintf(" Time Elapsed: %02d:%02d ", minutes, seconds)

			ns.Throughput = int64(math.Floor(float64(ns.DocsCopied.Load()) / timeElapsed.Seconds()))
			throughputStr := fmt.Sprintf(" Throughput: %v docs/s ", ns.Throughput)

			namespace := " Namespace: " + key.Db + "." + key.Col + " "
			table.SetCellSimple(row, 0, namespace)
			table.SetCellSimple(row, 1, percentCompleteStr)
			table.SetCellSimple(row, 2, tasksString)
			table.SetCellSimple(row, 3, timeElapsedStr)
			table.SetCellSimple(row, 4, throughputStr)
		}

		//set the progress bar
		//if we have no partitioning, we will have one task per namespace
		/*
			if r.runnerProgress.tasksTotal == r.runnerProgress.totalNamespaces {

			}*/
		completedTasksDocs := r.runnerProgress.tasksCompleted * targetDocCountPerPartition
		startedTaskDocsMax := r.runnerProgress.tasksStarted * targetDocCountPerPartition
		inprogressDocs := r.runnerProgress.numDocsSynced - completedTasksDocs
		if inprogressDocs > startedTaskDocsMax {
			inprogressDocs = startedTaskDocsMax
		}
		totalPercentComplete := float64(completedTasksDocs+inprogressDocs) / float64(r.runnerProgress.totalDocs) * 100
		progressBarWidth := 80
		progress := int(totalPercentComplete / 100 * float64(progressBarWidth))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete)
		progressBar.SetText(progressBarString)

	case "ChangeStream":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d        %d/%d Namespaces synced\nProcessing change stream events\n", r.runnerProgress.syncState, minutes, seconds, r.runnerProgress.numNamespacesSynced, r.runnerProgress.totalNamespaces)
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
