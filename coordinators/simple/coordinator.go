/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package simple

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"golang.org/x/sync/errgroup"
)

type Simple struct {
	// Implement the necessary fields here
	ctx context.Context
	t   iface.Transport
	s   iface.Statestore

	connectors    map[iface.ConnectorID]ConnectorDetailsWithEp
	mu_connectors sync.RWMutex // to make the map thread-safe

	flows    map[iface.FlowID]*FlowDetails
	mu_flows sync.RWMutex // to make the map thread-safe

	integrityStatus      []iface.FlowIntegrityStatus
	integrityStatusMutex sync.RWMutex

	numParallelIntegrityCheckTasks int
}

func NewSimpleCoordinator(numParallelIntegrityCheckTasks int) *Simple {
	// Implement the NewSimpleCoordinator function
	return &Simple{
		connectors:                     make(map[iface.ConnectorID]ConnectorDetailsWithEp),
		flows:                          make(map[iface.FlowID]*FlowDetails),
		numParallelIntegrityCheckTasks: numParallelIntegrityCheckTasks,
	}
}

// *****
// Thread-safe methods to work with items in the connectors map
// *****

// gets a connector by id
func (c *Simple) getConnector(cid iface.ConnectorID) (ConnectorDetailsWithEp, bool) {
	c.mu_connectors.RLock()
	defer c.mu_connectors.RUnlock()
	connector, ok := c.connectors[cid]
	return connector, ok
}

// deletes a connector from the map
func (c *Simple) delConnector(cid iface.ConnectorID) {
	c.mu_connectors.Lock()
	defer c.mu_connectors.Unlock()
	delete(c.connectors, cid)
}

// adds a connector with a unique ID and returns the ID
func (c *Simple) addConnector(connector ConnectorDetailsWithEp) iface.ConnectorID {
	c.mu_connectors.Lock()
	defer c.mu_connectors.Unlock()

	var cid iface.ConnectorID

	if connector.Details.Id != "" {
		connector.Details.Id += "-" + iface.ConnectorID(connector.Details.Desc)
		cid = connector.Details.Id
	} else {
		// we need to generate a new unique ID
		for {
			cid = generateConnectorID()
			if _, ok := c.connectors[cid]; !ok {
				break
			}
		}
		connector.Details.Id = cid // set the ID in the details for easier operations later
	}

	c.connectors[cid] = connector

	return cid
}

// return a list of ConnectorDetails for all known connectors
func (c *Simple) GetConnectors() []iface.ConnectorDetails {
	c.mu_connectors.RLock()
	defer c.mu_connectors.RUnlock()
	connectors := make([]iface.ConnectorDetails, 0, len(c.connectors))
	for _, details := range c.connectors {
		connectors = append(connectors, details.Details)
	}
	return connectors
}

// *****
// Thread-safe methods to work with items in the flows map
// *****

// gets a flow by id
func (c *Simple) getFlow(fid iface.FlowID) (*FlowDetails, bool) {
	c.mu_flows.RLock()
	defer c.mu_flows.RUnlock()
	flow, ok := c.flows[fid]
	return flow, ok
}

// adds a flow that must already have the ID set (since it's static)
func (c *Simple) addFlow(details *FlowDetails) {
	c.mu_flows.Lock()
	defer c.mu_flows.Unlock()

	var fid iface.FlowID = details.FlowID
	c.flows[fid] = details
}

// removes a flow from the map
func (c *Simple) delFlow(fid iface.FlowID) {
	c.mu_flows.Lock()
	defer c.mu_flows.Unlock()
	delete(c.flows, fid)
}

// *****
// Implement the Coordinator interface methods
// *****

func (c *Simple) Setup(ctx context.Context, t iface.Transport, s iface.Statestore) {
	// Implement the Setup method
	c.ctx = ctx
	c.t = t
	c.s = s
}

func (c *Simple) Teardown() {
	// Implement the Teardown method
}

func (c *Simple) RegisterConnector(details iface.ConnectorDetails, cep iface.ConnectorICoordinatorSignal) (iface.ConnectorID, error) {
	slog.Info("Registering connector with details: " + fmt.Sprintf("%+v", details))

	// Add the connector to the list
	cid := c.addConnector(ConnectorDetailsWithEp{Details: details, Endpoint: cep})
	slog.Debug("assigned connector ID: " + (string)(cid))

	// Implement the RegisterConnector method
	return cid, nil
}

func (c *Simple) DelistConnector(cid iface.ConnectorID) {
	slog.Info("Deregistering connector with ID: " + fmt.Sprintf("%v", cid))

	// Implement the DelistConnector method
	c.delConnector(cid)
}

func (c *Simple) FlowGetOrCreate(o iface.FlowOptions) (iface.FlowID, error) {
	// attempt to get the persistent flow state from the statestore
	fid := generateFlowID(o)
	fdet_temp := FlowDetails{}
	err_persisted_state := c.s.RetrieveObject(flowStateMetadataStore, fid, &fdet_temp)

	// Check flow type and error out if not unidirectional
	if o.Type != iface.UnidirectionalFlowType {
		return iface.FlowID(""), fmt.Errorf("only unidirectional flows are supported")
	}

	// Check if the source and destination connectors support the right modes
	if err := c.validateConnectorCapabilitiesForFlow(o); err != nil {
		return iface.FlowID(""), fmt.Errorf("connector capabilities validation failed: %v", err)
	}

	// for unidirectional flows we need two data channels
	// 0 corresponds to the source and 1 to the destination
	// here we're getting away with a trick to short circuit using a single channel
	// TODO (AK, 6/2024): use different channels for source and destination (could be a thing to negotiate between connectors)
	dc0, err := c.t.CreateDataChannel()
	if err != nil {
		return iface.FlowID(""), fmt.Errorf("failed to create data channel 0: %v", err)
	}
	dataChannels := make([]iface.DataChannelID, 2)
	dataChannels[0] = dc0
	dataChannels[1] = dc0

	doneChannels := make([]chan struct{}, 2)
	doneChannels[0] = make(chan struct{})
	doneChannels[1] = make(chan struct{})

	fdet := FlowDetails{
		FlowID:                   fid,
		Options:                  o,
		dataChannels:             dataChannels,
		doneNotificationChannels: doneChannels,
		flowDone:                 make(chan struct{}),
		readPlanningDone:         make(chan struct{}),
	}
	// recover the plan, if available
	if err_persisted_state == nil {
		slog.Info(fmt.Sprintf("Found an existing flow %v", fdet_temp.FlowID))
		//XXX: do we need to recover anything else?
		fdet.ReadPlan = fdet_temp.ReadPlan
	}

	c.addFlow(&fdet)

	slog.Debug("Initialized flow with ID: " + fmt.Sprintf("%v", fid) + " and options: " + fmt.Sprintf("%+v", o))

	return fid, nil
}

func (c *Simple) maybeCreateReadPlan(srcEndpoint iface.ConnectorICoordinatorSignal, flowDet *FlowDetails) error {
	fid := flowDet.FlowID
	// Check if we are resumable and have the flow plan already
	if (flowDet.ReadPlan.Tasks != nil || flowDet.ReadPlan.CdcResumeToken != nil) && flowDet.Resumable {
		slog.Debug("Using the existing read plan for a resumable flow. Flow ID: " + fmt.Sprintf("%v", fid))
		//reset all in progress tasks to new
	} else {
		// Request the source connector to create a plan for reading
		if err := srcEndpoint.RequestCreateReadPlan(fid, flowDet.Options.SrcConnectorOptions); err != nil {
			slog.Error("Failed to request read planning from source", "err", err)
			return err
		}

		// Wait for the read planning to be done
		//XXX: we should probably make it async and have a timeout
		select {
		case <-flowDet.readPlanningDone:
			if flowDet.readPlanErr != nil {
				slog.Error("Read plan failed", "err", flowDet.readPlanErr)
				return flowDet.readPlanErr
			}
			slog.Debug("Read planning done. Flow ID: " + fmt.Sprintf("%v", fid))
			if flowDet.Resumable {
				slog.Debug(fmt.Sprintf("Persisting the flow plan for flow %v", flowDet))
				err := c.s.PersistObject(flowStateMetadataStore, fid, flowDet)
				if err != nil {
					slog.Error("Failed to persist the flow plan", "err", err)
					return err
				}
			}
		case <-c.ctx.Done():
			slog.Debug("Context cancelled. Flow ID: " + fmt.Sprintf("%v", fid))
			return fmt.Errorf("context cancelled while waiting for read planning to be done")
		}
	}
	return nil
}

func (c *Simple) FlowStart(fid iface.FlowID) error {
	slog.Info("Starting flow with ID: " + fmt.Sprintf("%v", fid))

	// Get the flow details
	flowDet, ok := c.getFlow(fid)
	if !ok {
		return fmt.Errorf("flow %v not found", fid)
	}

	// Get the source and destination connectors
	src, ok := c.getConnector(flowDet.Options.SrcId)
	if !ok {
		return fmt.Errorf("source connector %v not found", flowDet.Options.SrcId)
	}
	dst, ok := c.getConnector(flowDet.Options.DstId)
	if !ok {
		return fmt.Errorf("destination connector %v not found", flowDet.Options.DstId)
	}

	// Set parameters on the source and destination connectors otherwise they may not be aligned on what they're doing
	flowCap := calcSharedCapabilities(src.Details.Cap, dst.Details.Cap)
	slog.Debug("Shared capabilities for the flow: " + fmt.Sprintf("%+v", flowCap))
	srcCapReq := calcReqCapabilities(src.Details.Cap, flowCap)
	dstCapReq := calcReqCapabilities(dst.Details.Cap, flowCap)
	src.Endpoint.SetParameters(fid, srcCapReq)
	dst.Endpoint.SetParameters(fid, dstCapReq)
	// Set resumability flag for the flow
	flowDet.Resumable = flowCap.Resumability

	if (flowDet.ReadPlan.Tasks != nil || flowDet.ReadPlan.CdcResumeToken != nil) && !flowDet.Resumable {
		slog.Error("Flow is not resumable but we have found the old plan. Please clean the metadata before restarting. Flow ID: " + fmt.Sprintf("%v", fid))
		return fmt.Errorf("flow is not resumable but old plan")
	}

	if err := c.maybeCreateReadPlan(src.Endpoint, flowDet); err != nil {
		return err
	}

	// Tell source connector to start reading into the data channel
	if err := src.Endpoint.StartReadToChannel(fid, flowDet.Options.SrcConnectorOptions, flowDet.ReadPlan, flowDet.dataChannels[0]); err != nil {
		slog.Error("Failed to start reading from source", "err", err)
		return err
	}
	// Tell destination connector to start writing from the channel
	if err := dst.Endpoint.StartWriteFromChannel(fid, flowDet.dataChannels[1]); err != nil {
		slog.Error("Failed to start writing to the destination", "err", err)
		return err
	}

	slog.Info("Flow with ID: " + fmt.Sprintf("%v", fid) + " is running")

	go func() {
		// Async wait until both src and dst signal that they are done
		// Exit if the context has been cancelled
		slog.Debug("Waiting for source to finish. Flow ID: " + fmt.Sprintf("%v", fid))
		select {
		case <-flowDet.doneNotificationChannels[0]:
			slog.Debug("Source finished. Flow ID: " + fmt.Sprintf("%v", fid))
		case <-c.ctx.Done():
			slog.Debug("Context cancelled. Flow ID: " + fmt.Sprintf("%v", fid))
		}
		slog.Debug("Waiting for destination to finish. Flow ID: " + fmt.Sprintf("%v", fid))
		select {
		case <-flowDet.doneNotificationChannels[1]:
			slog.Debug("Destination finished. Flow ID: " + fmt.Sprintf("%v", fid))
		case <-c.ctx.Done():
			slog.Debug("Context cancelled. Flow ID: " + fmt.Sprintf("%v", fid))
		}
		slog.Info("Flow with ID: " + fmt.Sprintf("%v", fid) + " is done")
		close(flowDet.flowDone)
	}()

	return nil
}

func (c *Simple) WaitForFlowDone(flowId iface.FlowID) error {
	// Get the flow details
	flowDet, ok := c.getFlow(flowId)
	if !ok {
		return fmt.Errorf("flow not found")
	}

	// Wait for the flow to be done
	<-flowDet.flowDone //TODO (AK, 6/2024): should we just return the channel?

	return nil
}

func (c *Simple) FlowStop(fid iface.FlowID) {
	//TODO (AK, 6/2024): Implement the FlowStop method
}

func (c *Simple) FlowDestroy(fid iface.FlowID) {

	slog.Debug("Destroying flow with ID: " + fmt.Sprintf("%v", fid))

	// Get the flow details
	flowDet, ok := c.getFlow(fid)
	if !ok {
		slog.Error(fmt.Sprintf("Flow %v not found", fid))
	}
	// close the data channels
	for _, ch := range flowDet.dataChannels {
		c.t.CloseDataChannel(ch)
	}

	// close done notification channels - not needed
	// for _, ch := range flowDet.DoneNotificationChannels {
	// 	close(ch)
	// }

	// remove the flow from the map
	c.delFlow(fid)

	// remove the flow state from the statestore
	err := c.s.DeleteObject(flowStateMetadataStore, fid)
	if err != nil {
		slog.Error("Failed to delete flow state", "err", err)
	}
}

func (c *Simple) NotifyDone(flowId iface.FlowID, conn iface.ConnectorID) error {
	// Get the flow details
	flowDet, ok := c.getFlow(flowId)
	if !ok {
		return fmt.Errorf("flow not found")
	}

	// Check if the connector corresponds to the source
	if flowDet.Options.SrcId == conn {
		// Close the first notification channel
		close(flowDet.doneNotificationChannels[0])
		return nil
	}

	// Check if the connector corresponds to the destination
	if flowDet.Options.DstId == conn {
		// Close the second notification channel
		close(flowDet.doneNotificationChannels[1])
		return nil
	}

	return fmt.Errorf("connector not part of the flow")
}

func (c *Simple) NotifyTaskDone(flowId iface.FlowID, conn iface.ConnectorID, taskId iface.ReadPlanTaskID, taskData *iface.TaskDoneMeta) error {
	// Get the flow details
	flowDet, ok := c.getFlow(flowId)
	if !ok {
		return fmt.Errorf("flow not found")
	}

	// Check if the connector corresponds to the source
	if flowDet.Options.SrcId == conn {
		slog.Debug("Task done notification from source connector for task ID: " + fmt.Sprintf("%v", taskId))
		err := updateFlowTaskData(flowDet, taskId, taskData)
		if err != nil {
			return err
		}
		return nil
	}

	// Check if the connector corresponds to the destination
	if flowDet.Options.DstId == conn {
		slog.Debug("Task done notification from destination connector for task ID: " + fmt.Sprintf("%v", taskId))
		// update the flow state to reflect the task completion
		err := updateFlowTaskStatus(flowDet, taskId, iface.ReadPlanTaskStatus_Completed)
		if err != nil {
			return err
		}

		// persist the updated flow state
		if flowDet.Resumable {
			err = c.s.PersistObject(flowStateMetadataStore, flowId, flowDet)
			if err != nil {
				slog.Error("Failed to persist the flow plan", "err", err)
				return err
			}
		}

		return nil
	}

	return fmt.Errorf("connector not part of the flow")
}

func mapNamespace(m map[string]string, namespace string) string {
	if res, ok := m[namespace]; ok {
		return res
	}
	if left, right, ok := strings.Cut(namespace, "."); ok {
		if res, ok := m[left]; ok {
			return res + "." + right
		}
	}
	return namespace
}

func createNamespaceMap(namespaces []string) map[string]string {
	m := map[string]string{}
	for _, namespace := range namespaces {
		if l, r, ok := strings.Cut(namespace, ":"); ok {
			m[l] = r
		}
	}
	return m
}

func (c *Simple) GetFlowIntegrityStatus(fid iface.FlowID) ([]iface.FlowIntegrityStatus, error) {
	c.integrityStatusMutex.RLock()
	defer c.integrityStatusMutex.RUnlock()
	res := make([]iface.FlowIntegrityStatus, len(c.integrityStatus))
	copy(res, c.integrityStatus)
	return res, nil
}

func (c *Simple) PerformFlowIntegrityCheck(ctx context.Context, fid iface.FlowID, options iface.IntegrityCheckOptions) (iface.FlowDataIntegrityCheckResult, error) {
	slog.Info("Initiating flow integrity check for flow with ID: " + fmt.Sprintf("%v", fid))

	res := iface.FlowDataIntegrityCheckResult{}

	// Get the flow details
	flowDet, ok := c.getFlow(fid)
	if !ok {
		return res, fmt.Errorf("flow %v not found", fid)
	}

	// Get the source and destination connectors
	src, ok := c.getConnector(flowDet.Options.SrcId)
	if !ok {
		return res, fmt.Errorf("source connector %v not found", flowDet.Options.SrcId)
	}
	dst, ok := c.getConnector(flowDet.Options.DstId)
	if !ok {
		return res, fmt.Errorf("destination connector %v not found", flowDet.Options.DstId)
	}

	if !src.Details.Cap.IntegrityCheck || !dst.Details.Cap.IntegrityCheck {
		return res, fmt.Errorf("one or both connectors don't support integrity checks")
	}

	if err := c.maybeCreateReadPlan(src.Endpoint, flowDet); err != nil {
		return res, err
	}

	namespaceMap := createNamespaceMap(flowDet.Options.SrcConnectorOptions.Namespace)
	var queries []iface.IntegrityCheckQuery

	c.integrityStatusMutex.Lock()
	c.integrityStatus = []iface.FlowIntegrityStatus{}
	statusIdx := map[string]int{}

	if options.QuickCount {
		dedup := map[string]iface.IntegrityCheckQuery{}
		for _, task := range flowDet.ReadPlan.Tasks {
			k := task.Def.Col
			if task.Def.Db != "" {
				k = task.Def.Db + "." + task.Def.Col
			}
			if _, ok := dedup[k]; !ok {
				statusIdx[k] = len(c.integrityStatus)
				dedup[k] = iface.IntegrityCheckQuery{
					Namespace: k,
					CountOnly: true,
				}
			}
		}
		for _, q := range dedup {
			statusIdx[q.Namespace] = len(c.integrityStatus)
			queries = append(queries, q)
			c.integrityStatus = append(c.integrityStatus, iface.FlowIntegrityStatus{
				Namespace:  q.Namespace,
				TasksTotal: 1,
			})
		}
	} else {
		for _, task := range flowDet.ReadPlan.Tasks {
			k := task.Def.Col
			if task.Def.Db != "" {
				k = task.Def.Db + "." + task.Def.Col
			}
			queries = append(queries, iface.IntegrityCheckQuery{
				Namespace:    k,
				PartitionKey: task.Def.PartitionKey,
				Low:          task.Def.Low,
				High:         task.Def.High,
			})
			if i, ok := statusIdx[k]; ok {
				c.integrityStatus[i].TasksTotal += 1
			} else {
				statusIdx[k] = len(c.integrityStatus)
				c.integrityStatus = append(c.integrityStatus, iface.FlowIntegrityStatus{
					Namespace:  k,
					TasksTotal: 1,
				})
			}
		}
	}
	c.integrityStatusMutex.Unlock()

	numWorkers := c.numParallelIntegrityCheckTasks
	if numWorkers < 1 {
		numWorkers = len(queries)
	}
	queryCh := make(chan iface.IntegrityCheckQuery)
	mismatchErr := errors.New("match failed")

	go func() {
		defer close(queryCh)
		for _, query := range queries {
			select {
			case queryCh <- query:
			case <-ctx.Done():
				slog.Info(fmt.Sprintf("Integrity query canceled %v", query))
				return
			}
		}
	}()

	var eg errgroup.Group
	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for query := range queryCh {
				eg2, ctx := errgroup.WithContext(ctx)

				dstNamespace := mapNamespace(namespaceMap, query.Namespace)
				dstQuery := query
				dstQuery.Namespace = dstNamespace

				var srcRes, dstRes iface.ConnectorDataIntegrityCheckResult
				eg2.Go(func() error {
					var err error
					srcRes, err = src.Endpoint.IntegrityCheck(ctx, query)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							slog.Info(fmt.Sprintf("Source integrity query canceled %v", query))
							return nil
						}
						slog.Error(fmt.Sprintf("Source integrity check failed %v: %v", query, err))
						return err
					}
					return nil
				})

				eg2.Go(func() error {
					var err error
					dstRes, err = dst.Endpoint.IntegrityCheck(ctx, dstQuery)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							slog.Info(fmt.Sprintf("Destination integrity query canceled %v", query))
							return nil
						}
						slog.Error(fmt.Sprintf("Destination integrity check failed %v: %v", query, err))
						return err
					}
					return nil
				})

				if err := eg2.Wait(); err != nil {
					return err
				}

				matches := srcRes == dstRes
				if !matches {
					slog.Info(fmt.Sprintf("Mismatch %v, src: %v, dest: %v", query, srcRes, dstRes))
					return mismatchErr
				}
				slog.Info(fmt.Sprintf("Matched %v, res: %v", query, srcRes))
				c.integrityStatusMutex.Lock()
				c.integrityStatus[statusIdx[query.Namespace]].TasksCompleted += 1
				c.integrityStatusMutex.Unlock()
			}
			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		if errors.Is(err, mismatchErr) {
			return res, nil
		}
		return res, err
	}
	res.Passed = true
	return res, nil
}

func (c *Simple) GetFlowStatus(fid iface.FlowID) (iface.FlowStatus, error) {
	res := iface.FlowStatus{}

	// Get the flow details
	flowDet, ok := c.getFlow(fid)
	if !ok {
		return res, fmt.Errorf("flow %v not found", fid)
	}

	// Get the source and destination connectors
	src, ok := c.getConnector(flowDet.Options.SrcId)
	if !ok {
		return res, fmt.Errorf("source connector %v not found", flowDet.Options.SrcId)
	}
	dst, ok := c.getConnector(flowDet.Options.DstId)
	if !ok {
		return res, fmt.Errorf("destination connector %v not found", flowDet.Options.DstId)
	}

	// Get latest status update from connectors
	flowDet.flowStatus.SrcStatus = src.Endpoint.GetConnectorStatus(fid)
	flowDet.flowStatus.DstStatus = dst.Endpoint.GetConnectorStatus(fid)

	// set the status flag if all the tasks are completed
	// XXX: should we persist this somewhere to make avoid recomputation every time?
	if isFlowReadPlanDone(flowDet) {
		flowDet.flowStatus.AllTasksCompleted = true
	}

	return flowDet.flowStatus, nil
}

func (c *Simple) UpdateConnectorStatus(flowId iface.FlowID, conn iface.ConnectorID, status iface.ConnectorStatus) error {
	// Get the flow details
	flowDet, ok := c.getFlow(flowId)
	if !ok {
		return fmt.Errorf("flow not found")
	}

	// Check if the connector corresponds to the source
	if flowDet.Options.SrcId == conn {
		flowDet.flowStatus.SrcStatus = status
		return nil
	}

	// Check if the connector corresponds to the destination
	if flowDet.Options.DstId == conn {
		flowDet.flowStatus.DstStatus = status
		return nil
	}

	return fmt.Errorf("connector not part of the flow")
}

func (c *Simple) PostReadPlanningResult(flowId iface.FlowID, conn iface.ConnectorID, res iface.ConnectorReadPlanResult) error {
	slog.Debug(fmt.Sprintf("Got read plan result from connector %v for flow %v", conn, flowId))
	// Get the flow details
	flowDet, ok := c.getFlow(flowId)
	if !ok {
		return fmt.Errorf("flow not found")
	}

	defer close(flowDet.readPlanningDone)

	if res.Error != nil {
		flowDet.readPlanErr = res.Error
		return nil
	}

	//sanity check that the connector is the source
	if flowDet.Options.SrcId != conn {
		flowDet.readPlanErr = fmt.Errorf("connector not the source for the flow")
		return nil
	}

	//check that the result was a success
	if !res.Success {
		flowDet.readPlanErr = fmt.Errorf("read planning failed")
		return nil
	}

	flowDet.ReadPlan = res.ReadPlan
	return nil
}

func (c *Simple) UpdateCDCResumeToken(flowId iface.FlowID, conn iface.ConnectorID, resumeToken []byte) error {
	// Get the flow details
	flowDet, ok := c.getFlow(flowId)
	if !ok {
		return fmt.Errorf("flow not found")
	}

	// Check if the connector corresponds to the source
	if flowDet.Options.SrcId == conn {
		slog.Debug("Ignoring CDC resume token update from the source connector")
		return nil
	}

	// Check if the connector corresponds to the destination
	if flowDet.Options.DstId == conn {
		slog.Debug("CDC resume token update from destination connector:" + fmt.Sprintf("%v", resumeToken))
		flowDet.ReadPlan.CdcResumeToken = resumeToken

		// persist the updated flow state
		if flowDet.Resumable {
			err := c.s.PersistObject(flowStateMetadataStore, flowId, flowDet)
			if err != nil {
				slog.Error("Failed to persist the flow plan", "err", err)
				return err
			}
		}

		return nil
	}

	return fmt.Errorf("connector not part of the flow")
}
