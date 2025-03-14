package pglib

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type UpdateType int

const (
	UpdateTypeInsert UpdateType = iota
	UpdateTypeUpdate
	UpdateTypeDelete
)

type flushHelper struct {
	items       []flushLSNDeadline // The last item will always be set- we freeze it if the gap is big enough with the one before it
	gap         time.Duration
	minDeadline time.Duration
}

func (r *flushHelper) Add(lsn pglogrepl.LSN) {
	deadline := time.Now().Add(r.minDeadline)
	if len(r.items) < 2 {
		r.items = append(r.items, flushLSNDeadline{
			lsn:      lsn,
			deadline: deadline,
		})
		return
	}

	lastIdx := len(r.items) - 1
	if r.items[lastIdx].deadline.After(r.items[lastIdx-1].deadline.Add(r.gap)) {
		r.items = append(r.items, flushLSNDeadline{
			lsn:      lsn,
			deadline: deadline,
		})
	} else {
		r.items[lastIdx].lsn = lsn
		r.items[lastIdx].deadline = deadline
	}
}

func (r *flushHelper) MaybeFlush() pglogrepl.LSN {
	i := 0
	var lsn pglogrepl.LSN
	for i < len(r.items) && time.Now().After(r.items[i].deadline) {
		if r.items[i].lsn > lsn {
			lsn = r.items[i].lsn
		}
		i += 1
	}
	if lsn > 0 {
		// Not efficient but expect r.items to be small
		r.items = r.items[i:]
	}
	return lsn
}

type flushLSNDeadline struct {
	lsn      pglogrepl.LSN
	deadline time.Time
}

type replState struct {
	standbyMessageDeadline time.Time
	flushLSN               pglogrepl.LSN
	currentLSN             pglogrepl.LSN
	latestLSN              pglogrepl.LSN
	inStream               bool
	relations              map[uint32]*pglogrepl.RelationMessageV2
	typeMap                *pgtype.Map
	updates                []Update
}

type Update struct {
	Namespace string
	TableName string
	Old       map[string]interface{}
	New       map[string]interface{}
	Type      UpdateType
	LSN       uint64
}

type ChangeStream struct {
	ReplicationURL        string
	ReplicationSlotName   string
	PublicationName       string
	StartLSN              uint64
	MinFlushDelay         time.Duration
	StandbyMessageTimeout time.Duration
}

func decodeToMap(state *replState, relationID uint32, columns []*pglogrepl.TupleDataColumn) (string, string, map[string]interface{}, error) {
	relation, ok := state.relations[relationID]
	if !ok {
		return "", "", nil, fmt.Errorf("error fetching relation")
	}
	values := map[string]interface{}{}
	for i, col := range columns {
		colName := relation.Columns[i].Name
		switch col.DataType {
		case 'n':
			values[colName] = nil
		case 't':
			val, err := decodeTextColumnData(state.typeMap, col.Data, relation.Columns[i].DataType)
			if err != nil {
				return "", "", nil, err
			}
			values[colName] = val
		}
	}
	return relation.Namespace, relation.RelationName, values, nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (c *ChangeStream) Run(ctx context.Context, ch chan<- []Update) error {
	standbyMessageTimeout := time.Second * 10
	minFlushDelay := c.MinFlushDelay
	if c.StandbyMessageTimeout > 0 {
		standbyMessageTimeout = c.StandbyMessageTimeout
	}
	startLSN := c.StartLSN

	pgConn, err := pgconn.Connect(ctx, c.ReplicationURL)
	if err != nil {
		return err
	}
	defer pgConn.Close(ctx)

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%v'", c.PublicationName),
		"messages 'true'",
		"streaming 'true'",
	}

	if err := pglogrepl.StartReplication(ctx, pgConn, c.ReplicationSlotName, pglogrepl.LSN(startLSN+1), pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}); err != nil {
		return err
	}

	state := &replState{
		standbyMessageDeadline: time.Now().Add(standbyMessageTimeout),
		flushLSN:               pglogrepl.LSN(startLSN),
		currentLSN:             pglogrepl.LSN(startLSN),
		latestLSN:              pglogrepl.LSN(startLSN),
		inStream:               false,
		relations:              map[uint32]*pglogrepl.RelationMessageV2{},
		typeMap:                pgtype.NewMap(),
	}

	fh := flushHelper{
		items:       []flushLSNDeadline{},
		gap:         minFlushDelay / 10,
		minDeadline: minFlushDelay,
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		maybeUpdateLSN := fh.MaybeFlush()
		if maybeUpdateLSN > state.flushLSN {
			state.flushLSN = maybeUpdateLSN
		}
		if time.Now().After(state.standbyMessageDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, pgConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: state.flushLSN + 1,
				WALFlushPosition: state.flushLSN + 1,
				WALApplyPosition: state.latestLSN + 1,
			}); err != nil {
				return err
			}
			state.standbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, state.standbyMessageDeadline)
		rawMsg, err := pgConn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}
		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("error in pg msg: %v", errMsg)
		}
		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return err
			}
			if pkm.ServerWALEnd > state.latestLSN {
				state.latestLSN = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				state.standbyMessageDeadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return err
			}
			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, state.inStream)
			if err != nil {
				return err
			}
			if xld.ServerWALEnd > state.latestLSN {
				state.latestLSN = xld.ServerWALEnd
			}
			switch logicalMsg := logicalMsg.(type) {
			case *pglogrepl.RelationMessageV2:
				state.relations[logicalMsg.RelationID] = logicalMsg
			case *pglogrepl.BeginMessage:
				state.currentLSN = logicalMsg.FinalLSN
			case *pglogrepl.CommitMessage:
				select {
				case ch <- state.updates:
				case <-ctx.Done():
					return nil
				}
				state.updates = nil
				fh.Add(state.currentLSN)
			case *pglogrepl.InsertMessageV2:
				ns, table, values, err := decodeToMap(state, logicalMsg.RelationID, logicalMsg.Tuple.Columns)
				if err != nil {
					return err
				}
				state.updates = append(state.updates, Update{
					Namespace: ns,
					TableName: table,
					New:       values,
					Type:      UpdateTypeInsert,
					LSN:       uint64(state.currentLSN),
				})
			case *pglogrepl.UpdateMessageV2:
				ns, table, values, err := decodeToMap(state, logicalMsg.RelationID, logicalMsg.NewTuple.Columns)
				if err != nil {
					return err
				}
				var oldValues map[string]interface{}
				if logicalMsg.OldTuple != nil {
					_, _, oldValues, err = decodeToMap(state, logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
					if err != nil {
						return err
					}
				}
				state.updates = append(state.updates, Update{
					Namespace: ns,
					TableName: table,
					Old:       oldValues,
					New:       values,
					Type:      UpdateTypeUpdate,
					LSN:       uint64(state.currentLSN),
				})
			case *pglogrepl.DeleteMessageV2:
				ns, table, values, err := decodeToMap(state, logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
				if err != nil {
					return err
				}
				state.updates = append(state.updates, Update{
					Namespace: ns,
					TableName: table,
					Old:       values,
					Type:      UpdateTypeDelete,
					LSN:       uint64(state.currentLSN),
				})
			case *pglogrepl.StreamStartMessageV2:
				state.inStream = true
			case *pglogrepl.StreamStopMessageV2:
				state.inStream = false
			}
		}
	}
}
