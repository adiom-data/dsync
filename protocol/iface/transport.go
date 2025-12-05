/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

import adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

type Location struct {
	Database   string
	Collection string
}

// TODO (AK, 6/2024): byte[] should be in Avro or Protobuf format (maybe subject to negotiation based on capabilities - like a preference list advertized by a connector)
type DataMessage struct {
	// payload (CRDT state)
	Data *[]byte
	// special case payload for inserts to allow the reader to send the whole batch for a single location (for efficiency)
	DataBatch [][]byte

	// header for data messages
	MutationType uint                 //required
	Loc          string               //required
	Id           []*adiomv1.BsonValue //required except for batch inserts (for efficiency)
	SeqNum       int64                //optional field to provide a global ordering of messages

	TaskId uint // Task ID for associating data messages with tasks
	// header for barriers (task completion signals)
	// combining them in a single struct to allow for a single channel for both data and barriers
	// for barriers MutationType will be set to MutationType_Barrier
	BarrierType           uint   //required for barriers
	BarrierTaskId         uint   //required for barriers related to initial data copy
	BarrierCdcResumeToken []byte //required for barriers related to CDC
}

const (
	MutationType_Reserved = iota
	MutationType_Insert
	MutationType_InsertBatch
	MutationType_Update
	MutationType_Delete
	MutationType_Ignore

	MutationType_Barrier
)

const (
	BarrierType_Reserved = iota
	BarrierType_TaskComplete
	BarrierType_CdcResumeTokenUpdate
	BarrierType_Block
)

// Special barrier message used for task completion signals over the data channel
// Allows synchronization with data messages flow
type BarrierMessage struct {
	TaskId uint
	Type   uint
}

type DataChannelID string

type Transport interface {
	// Gives the coordinator endpoint as a signalling interface
	GetCoordinatorEndpoint(location string) (CoordinatorIConnectorSignal, error)

	// Creates a data channel
	CreateDataChannel() (DataChannelID, error)

	// Gets a data channel endpoint by ID
	GetDataChannelEndpoint(DataChannelID) (chan DataMessage, error)

	// Closes a data channel
	CloseDataChannel(DataChannelID)
}
