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
	deletesCaught      int64

	throughput    float64
	nsProgressMap map[string]*iface.NameSpaceStatus //map key is namespace "db.col"
	namespaces    []iface.Namespace                 //use map and get the keys so print order is consistent

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
			ns := r.runnerProgress.nsProgressMap[nsString]

			docsCopied := ns.DocsCopied.Load()
			//percentComplete := percentCompleteNamespace(ns)

			table.SetCellSimple(row+1, 0, fmt.Sprintf("%s.%s", key.Db, key.Col))
			//table.SetCellSimple(row+1, 1, fmt.Sprintf("%.0f%%", percentComplete))
			table.SetCellSimple(row+1, 2, fmt.Sprintf("%d/%d", ns.TasksCompleted.Load(), len(ns.Tasks)))
			table.SetCellSimple(row+1, 3, fmt.Sprintf("%d", docsCopied))
			table.SetCellSimple(row+1, 4, fmt.Sprintf("%.0f", ns.Throughput))
		}

		progressBarWidth := 80

		/*
			totalPercentComplete := percentCompleteTotal(r.runnerProgress)
			slog.Debug(fmt.Sprintf("Total percent complete: %.2f", totalPercentComplete))

			progress := int(math.Floor((totalPercentComplete / 100 * float64(progressBarWidth))))
			progressBarString := fmt.Sprintf("[%s%s] %.2f%%		%.2f docs/sec\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete, r.runnerProgress.throughput)
			progressBar.SetText(progressBarString) */
		//placeholder for now
		progressBarString := fmt.Sprintf("[%s] %.2f%%		%.2f docs/sec\n", strings.Repeat(" ", progressBarWidth), 0.0, r.runnerProgress.throughput)
		progressBar.SetText(progressBarString)

	case "ChangeStream":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d        %d/%d Namespaces synced\nProcessing change stream events\n", r.runnerProgress.syncState, minutes, seconds, r.runnerProgress.numNamespacesCompleted, r.runnerProgress.totalNamespaces)
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
	if progress.tasksTotal == progress.totalNamespaces {
		//no partitioning
		numDocsCompleted := int64(0)
		maxNumDocsInProgress := int64(0)
		numDocsInProgress := progress.numDocsSynced
		for _, ns := range progress.nsProgressMap {
			if ns.TasksCompleted.Load() == 1 {
				numDocsCompleted += ns.EstimatedDocCount
			} else {
				maxNumDocsInProgress += ns.EstimatedDocCount
			}
		}
		numDocsInProgress = numDocsInProgress - numDocsCompleted
		if numDocsInProgress >= maxNumDocsInProgress && progress.numNamespacesCompleted != progress.totalNamespaces {
			numDocsInProgress = maxNumDocsInProgress - 1
		} else if numDocsInProgress > maxNumDocsInProgress && progress.numNamespacesCompleted == progress.totalNamespaces {
			numDocsInProgress = maxNumDocsInProgress
		}
		percentComplete = float64(numDocsCompleted+numDocsInProgress) / float64(numDocsCompleted+maxNumDocsInProgress) * 100
	} else {
		//partitioning
		totalPercents := float64(0)
		for _, ns := range progress.nsProgressMap {
			totalPercents += percentCompleteNamespace(ns)
			slog.Debug(fmt.Sprintf("Namespace is %.2f%% complete", percentCompleteNamespace(ns)))
		}
		percentComplete = float64(totalPercents) / float64(progress.totalNamespaces)
	}
	return percentComplete

}

func percentCompleteNamespace(nsStatus *iface.NameSpaceStatus) float64 {
	var percentComplete float64
	if len(nsStatus.Tasks) == 1 {
		//no partitioning
		docCount := nsStatus.EstimatedDocCount
		if docCount == 0 {
			percentComplete = 100
		} else {
			percentComplete = float64(nsStatus.DocsCopied.Load()) / float64(nsStatus.EstimatedDocCount) * 100
		}
	} else {
		//partitioning
		numDocsCopied := nsStatus.DocsCopied.Load()
		docsPerTask := nsStatus.Tasks[0].Def.EstimatedDocCount

		numCompletedDocs := int64(nsStatus.TasksCompleted.Load()) * docsPerTask
		numInProgressDocsMax := int64(nsStatus.TasksStarted.Load()) * docsPerTask
		numDocsCopied -= numCompletedDocs
		if numDocsCopied >= int64(numInProgressDocsMax) && len(nsStatus.Tasks) != int(nsStatus.TasksCompleted.Load()) {
			//we are in the middle of a task
			numDocsCopied = int64(numInProgressDocsMax - 1)
		} else if numDocsCopied > int64(numInProgressDocsMax) && len(nsStatus.Tasks) == int(nsStatus.TasksCompleted.Load()) {
			numDocsCopied = int64(numInProgressDocsMax)
		}
		percentComplete = float64(numCompletedDocs+numDocsCopied) / float64(numCompletedDocs+numInProgressDocsMax) * 100
	}
	return percentComplete
}
