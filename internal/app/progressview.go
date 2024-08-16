/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package dsync

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/rivo/tview"
)

const (
	throughputUpdateInterval = 5 * time.Second
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

// Get the latest status report based on the runner progress struct and update the tview components accoringly
func (tv *TViewDetails) GetStatusReport(runnerProgress iface.RunnerSyncProgress) {
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
	case "Setup":
		headerString := fmt.Sprintf("Dsync Progress Report : %s\nTime Elapsed: %02d:%02d:%02d\nSetting up the sync\n", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)
	case "ReadPlanning":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nCreating the read plan\n", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)
	case "InitialSync":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d		%d/%d Namespaces synced		Docs Synced: %d	\n", runnerProgress.SyncState, hours, minutes, seconds, runnerProgress.NumNamespacesCompleted, runnerProgress.TotalNamespaces, runnerProgress.NumDocsSynced)
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
			table.SetCell(row+1, 2, tview.NewTableCell(fmt.Sprintf("%d/%d", atomic.LoadInt64(&ns.TasksCompleted), len(ns.Tasks))).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 3, tview.NewTableCell(fmt.Sprintf("%d", docsCopied)).SetAlign(tview.AlignLeft).SetExpansion(1))
			table.SetCell(row+1, 4, tview.NewTableCell(fmt.Sprintf("%.0f", ns.Throughput)).SetAlign(tview.AlignLeft).SetExpansion(1))
		}

		//set the progress bar
		progressBarWidth := 80

		totalPercentComplete := percentCompleteTotal(runnerProgress)

		progress := int(math.Floor((totalPercentComplete / 100 * float64(progressBarWidth))))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%		%.2f docs/sec\n\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete, runnerProgress.Throughput)
		progressBar.SetText(progressBarString)

	case "ChangeStream":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d        %d/%d Namespaces synced\nProcessing change stream events\n\nChange Stream Events- %d		Deletes Caught- %d		%.2f Events/Sec", runnerProgress.SyncState, hours, minutes, seconds, runnerProgress.NumNamespacesCompleted, runnerProgress.TotalNamespaces, runnerProgress.ChangeStreamEvents, runnerProgress.DeletesCaught, runnerProgress.Throughput)
		header.SetText(headerString)

		//set the indefinite progress bar
		progressBarWidth := 80
		cdcPaginatorPosition = (cdcPaginatorPosition + 1) % (progressBarWidth - 4)
		progressBarString := fmt.Sprintf("[%s%s%s]\n", strings.Repeat(string('-'), cdcPaginatorPosition), strings.Repeat(">", 3), strings.Repeat("-", progressBarWidth-cdcPaginatorPosition-2))
		progressBar.SetText(progressBarString)

	case "Cleanup":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nCleaning up flow data\n", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)

	case "Verification":
		//set the header text

		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %02d:%02d:%02d\nPerforming Data Integrity Check", runnerProgress.SyncState, hours, minutes, seconds)
		header.SetText(headerString)

	default:
		headerString := "This connector does not support progress reporting yet\n"
		header.SetText(headerString)
	}
	tv.app.Draw()
}

// Calculate the total percent complete for all namespaces
func percentCompleteTotal(progress iface.RunnerSyncProgress) float64 {
	var percentComplete float64
	docsCopied, totalDocs := float64(0), float64(0)
	for _, ns := range progress.NsProgressMap {
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
