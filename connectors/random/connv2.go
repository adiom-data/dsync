//gosec:disable G404 -- This is a false positive
package random

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
)

type connV2 struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	payload                         map[string]any
	namespacePrefix                 string
	initialSource                   []byte
	numNamespaces                   int
	numPartitionsPerNamespace       int
	numUpdatePartitionsPerNamespace int
	numDocsPerPartition             int64
	batchSize                       int
	updateBatchSize                 int
	sleep                           time.Duration
	jitter                          time.Duration
	updateDuration                  time.Duration
	streamTick                      time.Duration
	maxUpdatesPerTick               int

	idFmt string
}

type updateCursor struct {
	Source         []byte
	PartitionIndex int
	LastTime       time.Time
	BatchDuration  time.Duration
}

func (c *updateCursor) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeUpdateCursor(in []byte) (*updateCursor, error) {
	if len(in) == 0 {
		return &updateCursor{}, nil
	}
	var c updateCursor
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

type partitionCursor struct {
	Low  int
	High int
	Pos  int64
}

func (c *partitionCursor) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodePartitionCursor(in []byte) (*partitionCursor, error) {
	if len(in) == 0 {
		return &partitionCursor{}, nil
	}
	var c partitionCursor
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

type ConnV2Input struct {
	NamespacePrefix                 string
	NumNamespaces                   int
	NumPartitionsPerNamespace       int
	NumUpdatePartitionsPerNamespace int
	BatchSize                       int
	UpdateBatchSize                 int
	MaxUpdatesPerTick               int
	NumDocsPerPartition             int64
	Payload                         map[string]any
	Sleep                           time.Duration
	Jitter                          time.Duration
	UpdateDuration                  time.Duration
	StreamTick                      time.Duration
}

func NewConnV2(input ConnV2Input) *connV2 {
	source := rand.NewPCG(uint64(time.Now().UnixNano()), rand.Uint64())
	initialSource, _ := source.MarshalBinary()

	res := &connV2{
		namespacePrefix:                 input.NamespacePrefix,
		initialSource:                   initialSource,
		numNamespaces:                   input.NumNamespaces,
		numPartitionsPerNamespace:       input.NumPartitionsPerNamespace,
		numUpdatePartitionsPerNamespace: input.NumUpdatePartitionsPerNamespace,
		batchSize:                       input.BatchSize,
		updateBatchSize:                 input.UpdateBatchSize,
		numDocsPerPartition:             input.NumDocsPerPartition,
		payload:                         input.Payload,
		sleep:                           input.Sleep,
		jitter:                          input.Jitter,
		updateDuration:                  input.UpdateDuration,
		maxUpdatesPerTick:               input.MaxUpdatesPerTick,
		streamTick:                      input.StreamTick,
	}

	partitionDigits := int(math.Ceil(math.Log2(float64(1+res.numPartitionsPerNamespace)) / 4))
	idDigits := int(math.Ceil(math.Log2(float64(res.numDocsPerPartition)) / 4))
	res.idFmt = fmt.Sprintf("%%0%vx-%%0%vx", partitionDigits, idDigits)
	return res
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	requestNamespaces := map[string]struct{}{}
	for _, ns := range r.Msg.GetNamespaces() {
		requestNamespaces[ns] = struct{}{}
	}
	var partitions []*adiomv1.Partition
	var updatePartitions []*adiomv1.UpdatesPartition
	if r.Msg.InitialSync {
		for i := range c.numNamespaces {
			ns := fmt.Sprintf("%v%v", c.namespacePrefix, i)

			if _, ok := requestNamespaces[ns]; !ok && len(requestNamespaces) > 0 {
				continue
			}

			for j := range c.numPartitionsPerNamespace {
				cursor := partitionCursor{
					Low:  j,
					High: j + 1,
				}
				encodedCursor, err := cursor.Encode()
				if err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}

				slog.Debug("Partition", "namespace", ns, "index", j, "cursor", cursor)
				partitions = append(partitions, &adiomv1.Partition{
					Namespace:      ns,
					Cursor:         encodedCursor,
					EstimatedCount: uint64(c.numDocsPerPartition),
				})
			}
		}
	}
	if r.Msg.Updates {
		if c.numUpdatePartitionsPerNamespace == 0 {
			cursor := updateCursor{
				Source:         c.initialSource,
				PartitionIndex: 0,
				LastTime:       time.Now(),
				BatchDuration:  c.updateDuration,
			}
			encodedCursor, err := cursor.Encode()
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

			slog.Debug("Update Partition (Single)", "namespace", r.Msg.GetNamespaces(), "cursor", cursor)
			updatePartitions = append(updatePartitions, &adiomv1.UpdatesPartition{
				Namespaces: r.Msg.GetNamespaces(),
				Cursor:     encodedCursor,
			})
		} else {
			initialSource := &rand.PCG{}
			if err := initialSource.UnmarshalBinary(c.initialSource); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			g := rand.New(initialSource)
			for i := range c.numNamespaces {
				ns := fmt.Sprintf("%v%v", c.namespacePrefix, i)

				b, err := initialSource.MarshalBinary()
				if err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}

				if _, ok := requestNamespaces[ns]; !ok && len(requestNamespaces) > 0 {
					continue
				}

				for j := range c.numUpdatePartitionsPerNamespace {
					cursor := updateCursor{
						Source:         b,
						PartitionIndex: j,
						LastTime:       time.Now(),
						BatchDuration:  c.updateDuration + time.Duration(g.Int64N(int64(1+c.jitter))),
					}
					encodedCursor, err := cursor.Encode()
					if err != nil {
						return nil, connect.NewError(connect.CodeInternal, err)
					}

					slog.Debug("Update Partition", "namespace", ns, "index", j, "cursor", cursor)
					updatePartitions = append(updatePartitions, &adiomv1.UpdatesPartition{
						Namespaces: []string{ns},
						Cursor:     encodedCursor,
					})
				}

				_ = initialSource.Uint64()
			}
		}

	}
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: updatePartitions,
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType: "/dev/fakesource",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_JSON_ID, adiomv1.DataType_DATA_TYPE_MONGO_BSON},
				LsnStream:          true,
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(c.batchSize),
	}), nil
}

func (c *connV2) createPayload(typ adiomv1.DataType, id string) ([]byte, error) {
	var data []byte
	var err error

	switch typ {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		m := map[string]any{}
		maps.Copy(m, c.payload)
		m["_id"] = id
		data, err = bson.Marshal(m)
		if err != nil {
			return nil, err
		}
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		m := map[string]any{}
		maps.Copy(m, c.payload)
		m["id"] = id
		data, err = json.Marshal(m)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported type %v", typ)
	}
	return data, nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	if c.sleep > 0 {
		select {
		case <-time.After(c.sleep + time.Duration(rand.Int64N(int64(1+c.jitter)))):
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
	rawCursor := r.Msg.GetPartition().GetCursor()
	if r.Msg.GetCursor() != nil {
		rawCursor = r.Msg.GetCursor()
	}
	cursor, err := DecodePartitionCursor(rawCursor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var res [][]byte
	for i := range c.batchSize {
		id := fmt.Sprintf(c.idFmt, cursor.Low, cursor.Pos+int64(i))

		data, err := c.createPayload(r.Msg.GetType(), id)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		res = append(res, data)
		if cursor.Pos+int64(i+1) >= c.numDocsPerPartition {
			return connect.NewResponse(&adiomv1.ListDataResponse{
				Data: res,
			}), nil
		}
	}
	cursor.Pos += int64(c.batchSize)

	nextCursor, err := cursor.Encode()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       res,
		NextCursor: nextCursor,
	}), nil
}

func streamCommon[T any](ctx context.Context, c *connV2, encodedCursor []byte, namespaces []string, itemCallback func(string) (T, error), groupCallback func(string, []T, []byte) error) error {
	cursor, err := DecodeUpdateCursor(encodedCursor)
	if err != nil {
		return err
	}

	source := &rand.PCG{}
	if err := source.UnmarshalBinary(cursor.Source); err != nil {
		return err
	}
	g := rand.New(source)

	adjustedNumDocsPerPartition := c.numDocsPerPartition
	if c.numUpdatePartitionsPerNamespace > 1 {
		adjustedNumDocsPerPartition = (c.numDocsPerPartition / int64(c.numUpdatePartitionsPerNamespace))
	}

	for {
		select {
		case <-time.After(c.streamTick + time.Duration(g.Int64N(int64(1+c.jitter)))):
			var count int
			for {
				if cursor.LastTime.After(time.Now()) {
					break
				}
				if count > c.maxUpdatesPerTick {
					break
				}
				var ns string
				if len(namespaces) == 0 {
					ns = fmt.Sprintf("%v%v", c.namespacePrefix, g.IntN(c.numNamespaces))
				} else {
					ns = namespaces[g.IntN(len(namespaces))]
				}
				var updates []T
				for range c.updateBatchSize {
					partition := g.IntN(c.numPartitionsPerNamespace)
					doc := g.Int64N(adjustedNumDocsPerPartition)
					if c.numUpdatePartitionsPerNamespace > 1 {
						doc = (doc * int64(c.numUpdatePartitionsPerNamespace)) + int64(cursor.PartitionIndex)
					}
					id := fmt.Sprintf(c.idFmt, partition, doc)
					update, err := itemCallback(id)
					if err != nil {
						return err
					}
					updates = append(updates, update)
				}
				count += c.updateBatchSize
				cursor.LastTime = cursor.LastTime.Add(cursor.BatchDuration)
				cursor.Source, err = source.MarshalBinary()
				if err != nil {
					return err
				}
				nextCursor, err := cursor.Encode()
				if err != nil {
					return err
				}

				if err := groupCallback(ns, updates, nextCursor); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	if r.Msg.GetCursor() == nil {
		return nil
	}

	var lsn uint64
	return streamCommon(ctx, c, r.Msg.GetCursor(), r.Msg.GetNamespaces(), func(id string) (struct{}, error) {
		return struct{}{}, nil
	}, func(ns string, updates []struct{}, nextCursor []byte) error {
		lsn += uint64(len(updates))
		return s.Send(&adiomv1.StreamLSNResponse{
			NextCursor: nextCursor,
			Lsn:        lsn,
		})
	})
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	if r.Msg.GetCursor() == nil {
		return nil
	}

	var idKey string
	switch r.Msg.GetType() {
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		idKey = "id"
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		idKey = "_id"
	default:
		return connect.NewError(connect.CodeInternal, fmt.Errorf("unsupported type %v", r.Msg.GetType()))
	}

	return streamCommon(ctx, c, r.Msg.GetCursor(), r.Msg.GetNamespaces(), func(id string) (*adiomv1.Update, error) {
		t, d, err := bson.MarshalValue(id)
		if err != nil {
			return nil, err
		}
		bsonId := []*adiomv1.BsonValue{
			{
				Data: d,
				Type: uint32(t),
				Name: idKey,
			},
		}

		data, err := c.createPayload(r.Msg.GetType(), id)
		if err != nil {
			return nil, err
		}

		return &adiomv1.Update{
			Id:   bsonId,
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: data,
		}, nil
	}, func(ns string, updates []*adiomv1.Update, nextCursor []byte) error {
		return s.Send(&adiomv1.StreamUpdatesResponse{
			Updates:    updates,
			Namespace:  ns,
			NextCursor: nextCursor,
		})
	})
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	panic("unimplemented")
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *connV2) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	panic("unimplemented")
}

var _ adiomv1connect.ConnectorServiceHandler = &connV2{}
