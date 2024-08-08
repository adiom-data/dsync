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

type runnerSyncProgress struct {
	startTime           time.Time
	syncState           string                                     //get from coordinator
	totalNamespaces     int64                                      //get from reader
	numNamespacesSynced int64                                      //get from writer
	totalDocs           int64                                      //get from reader
	numDocsSynced       int64                                      //get from writer
	throughput          int64                                      //get from coord
	nsProgressMap       map[iface.Namespace]*iface.NameSpaceStatus //get from coord? or writer?
	namespaces          []iface.Namespace                          //use map and get the keys so print order is consistent
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
	srcStatus, _ := flowStatus.SrcStatus, flowStatus.DstStatus
	if srcStatus.InitialSyncActive {
		r.runnerProgress.syncState = "InitialSync"
	} else if srcStatus.CDCActive {
		r.runnerProgress.syncState = "CDC"
	} else if srcStatus.ReadPlanningActive {
		r.runnerProgress.syncState = "ReadPlanning"
	}
	r.runnerProgress.numNamespacesSynced = srcStatus.SyncProgress.NumNamespacesSynced.Load()
	r.runnerProgress.totalNamespaces = srcStatus.SyncProgress.NumNamespaces
	r.runnerProgress.totalDocs = srcStatus.EstimatedTotalDocCount.Load()
	r.runnerProgress.numDocsSynced = srcStatus.SyncProgress.NumDocsSynced.Load()
	r.runnerProgress.nsProgressMap = srcStatus.NamespaceProgress

	r.runnerProgress.namespaces = srcStatus.Namespaces
}

func (r *RunnerLocal) GetStatusReport() {
	totalTimeElapsed := time.Since(r.runnerProgress.startTime).Seconds()
	fmt.Printf("\n\033[2K\rDsync Progress Report : %v\nTime Elapsed: %.2fs\n\n", r.runnerProgress.syncState, totalTimeElapsed)

	for _, key := range r.runnerProgress.namespaces {
		ns := r.runnerProgress.nsProgressMap[key]
		percentComplete := math.Floor(float64(ns.DocsCopied.Load()) / float64(ns.EstimatedDocCount) * 100)
		percentCompleteStr := fmt.Sprintf("%.0f%% complete", percentComplete)
		timeElapsed := time.Since(ns.StartTime).Seconds()
		timeElapsedStr := fmt.Sprintf("Time Elapsed: %.2fs", timeElapsed)
		throughputStr := fmt.Sprintf("Throughput: %v docs/s", ns.Throughput)
		namespace := "Namespace: " + key.Db + "." + key.Col
		fmt.Printf("\033[2K\r%-30s %-30s %-25s %-25s\n", namespace, timeElapsedStr, percentCompleteStr, throughputStr)
	}
	totalPercentComplete := float64(r.runnerProgress.numDocsSynced) / float64(r.runnerProgress.totalDocs) * 100
	progressBarWidth := 50
	progress := int(totalPercentComplete / 100 * float64(progressBarWidth))
	progressBar := fmt.Sprintf("[%s%s] %.2f%%", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete)
	fmt.Printf("\n\033[2K\r%s\n", progressBar)

	/*for i := 0; i < r.runnerProgress.totalNamespaces+6; i++ {
		fmt.Print("\033[F")
	}
	if totalPercentComplete != 100 {
		r.runnerProgress.numDocsSynced += 50
	} */
}

func (r *RunnerLocal) SetUpDisplay(app *tview.Application, errorText *tview.TextView) {
	r.runnerInterface = app
	headerTextView := tview.NewTextView().SetText("Dsync Progress Report").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	table := tview.NewTable()
	progressBarTextView := tview.NewTextView().SetText("Progress Bar").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	errorText.SetText("Error Logs").SetDynamicColors(true).SetRegions(true).SetWordWrap(true)
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(headerTextView, 0, 1, false).
		AddItem(table, 0, 1, false).
		AddItem(progressBarTextView, 1, 1, false).
		AddItem(errorText, 0, 1, false)
	r.root = root //indices are 0, 1, 2, 3, corresponding to header, table, progressBar, and errorLogs respectively
	r.runnerInterface.SetRoot(root, true)
}

func (r *RunnerLocal) GetStatusReport2() {
	//get tview components
	header := r.root.GetItem(0).(*tview.TextView)
	header.Clear()

	table := r.root.GetItem(1).(*tview.Table)
	table.Clear()

	progressBar := r.root.GetItem(2).(*tview.TextView)
	progressBar.Clear()

	//get the time elapsed
	totalTimeElapsed := time.Since(r.runnerProgress.startTime).Seconds()

	switch r.runnerProgress.syncState {
	case "Setup":
		headerString := fmt.Sprintf("Dsync Progress Report : %s\nTime Elapsed: %.2fs\nSetting up the sync\n", r.runnerProgress.syncState, totalTimeElapsed)
		header.SetText(headerString)
	case "ReadPlanning":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %.2fs\nCreating the read plan\n", r.runnerProgress.syncState, totalTimeElapsed)
		header.SetText(headerString)
	case "InitialSync":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %.2fs        %d/%d Namespaces synced\n", r.runnerProgress.syncState, totalTimeElapsed, r.runnerProgress.numNamespacesSynced, r.runnerProgress.totalNamespaces)
		header.SetText(headerString)

		//set the table
		for row, key := range r.runnerProgress.namespaces {
			ns := r.runnerProgress.nsProgressMap[key]
			//percentComplete := math.Floor(float64(ns.DocsCopied.Load()) / float64(ns.EstimatedDocCount) * 100)
			//percentCompleteStr := fmt.Sprintf(" %.0f%% complete ", percentComplete)
			timeElapsed := time.Since(ns.StartTime).Seconds()
			timeElapsedStr := fmt.Sprintf(" Time Elapsed: %.2fs ", timeElapsed)
			//throughputStr := fmt.Sprintf(" Throughput: %v docs/s ", ns.Throughput)
			namespace := " Namespace: " + key.Db + "." + key.Col + " "
			table.SetCellSimple(row, 0, namespace)
			//table.SetCellSimple(row, 1, percentCompleteStr)
			table.SetCellSimple(row, 2, timeElapsedStr)
			//table.SetCellSimple(row, 3, throughputStr)
		}

		//set the progress bar
		totalPercentComplete := float64(r.runnerProgress.numDocsSynced) / float64(r.runnerProgress.totalDocs) * 100
		if totalPercentComplete != 100 {
			r.runnerProgress.numDocsSynced += 50
		}
		progressBarWidth := 50
		progress := int(totalPercentComplete / 100 * float64(progressBarWidth))
		progressBarString := fmt.Sprintf("[%s%s] %.2f%%\n", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete)
		progressBar.SetText(progressBarString)

	case "CDC":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %.2fs        %d/%d Namespaces synced\nProcessing change stream events\n", r.runnerProgress.syncState, totalTimeElapsed, r.runnerProgress.numNamespacesSynced, r.runnerProgress.totalNamespaces)
		header.SetText(headerString)

	case "Cleanup":
		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %.2fs\nCleaning up flow data\n", r.runnerProgress.syncState, totalTimeElapsed)
		header.SetText(headerString)

	case "Verification":
		//set the header text

		headerString := fmt.Sprintf("Dsync Progress Report : %v\nTime Elapsed: %.2fs\nPerforming Data Integrity Check", r.runnerProgress.syncState, totalTimeElapsed)
		header.SetText(headerString)
	}
	r.runnerInterface.Draw()
}
