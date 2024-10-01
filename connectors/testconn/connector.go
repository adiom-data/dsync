package testconn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type connector struct {
	id             iface.ConnectorID
	desc           string
	coord          iface.CoordinatorIConnectorSignal
	t              iface.Transport
	path           string
	loop           bool
	flowCtx        context.Context
	flowCancelFunc context.CancelFunc
}

// NewConnector creates a testconnector for use in testing with fake data
// Specify the connection string as a directory location to save data to or read data from
// Generally, you want to use this as a source to populate some destinations, and
// then verify between those destinations, or populate one destination that serves as
// a source for another one
// Note that the namespace is always made up and not persisted, so it is always whatever
// the configuration says.
//
// Source:
// you can specify a namespace to target
// while running, this will apply bootstrap.json as a bulk write, and then updates.json as CDC writes
// for now, run with 1 parallel writers to make sure we don't clobber the bulk inserts
//
// Destination:
// make sure to specify a namespace and it will create a bootstrap.json based off existing data
// while running, making updates to your source will be recorded in updates.json
func NewConnector(desc string, path string) iface.Connector {
	return &connector{
		desc: desc,
		path: path,
		loop: false, // TODO: make configurable
	}
}

// GetConnectorStatus implements iface.Connector.
func (c *connector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return iface.ConnectorStatus{
		WriteLSN:        0,
		CDCActive:       false,
		SyncState:       iface.SetupSyncState,
		AdditionalInfo:  "",
		ProgressMetrics: iface.ProgressMetrics{},
	}
}

// Interrupt implements iface.Connector.
func (c *connector) Interrupt(flowId iface.FlowID) error {
	if c.flowCancelFunc != nil {
		c.flowCancelFunc()
	}
	return nil
}

// RequestCreateReadPlan implements iface.Connector.
func (c *connector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	namespaces := options.Namespace
	if len(namespaces) < 1 {
		namespaces = []string{"testconn.testconncol"}
	}
	var tasks []iface.ReadPlanTask
	for _, namespace := range namespaces {
		splitted := strings.SplitN(namespace, ".", 2)
		if len(splitted) != 2 {
			return errors.New(fmt.Sprintf("Unexpected namespace %v", namespace))
		}
		db := splitted[0]
		col := splitted[1]
		tasks = append(tasks, iface.ReadPlanTask{
			Id:     1,
			Status: iface.ReadPlanTaskStatus_New,
			Def: struct {
				Db           string
				Col          string
				PartitionKey string
				Low          interface{}
				High         interface{}
			}{
				Db:  db,
				Col: col,
			},
			EstimatedDocCount: 0,
			DocsCopied:        0,
		})
	}

	err := c.coord.PostReadPlanningResult(flowId, c.id, iface.ConnectorReadPlanResult{
		ReadPlan: iface.ConnectorReadPlan{
			Tasks:          tasks,
			CdcResumeToken: []byte{1},
			CreatedAtEpoch: 0,
		},
		Success: true,
	})
	if err != nil {
		return err
	}

	return nil
}

// RequestDataIntegrityCheck implements iface.Connector.
func (c *connector) IntegrityCheck(ctx context.Context, flowId iface.FlowID, task iface.ReadPlanTask) (iface.ConnectorDataIntegrityCheckResult, error) {
	return iface.ConnectorDataIntegrityCheckResult{}, errors.New("Not implemented")
}

// SetParameters implements iface.Connector.
func (c *connector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
}

// Setup implements iface.Connector.
func (c *connector) Setup(ctx context.Context, t iface.Transport) error {
	c.flowCtx, c.flowCancelFunc = context.WithCancel(ctx)
	c.t = t
	coord, err := t.GetCoordinatorEndpoint("local")
	if err != nil {
		return err
	}
	c.coord = coord

	id, err := c.coord.RegisterConnector(iface.ConnectorDetails{
		Id:   "testconn",
		Desc: c.desc,
		Type: iface.ConnectorType{
			DbType:  "testconn",
			Version: "1",
			Spec:    "testconn spec",
		},
		Cap: iface.ConnectorCapabilities{
			Source:         true,
			Sink:           true,
			IntegrityCheck: false,
			Resumability:   false,
		},
	}, c)
	if err != nil {
		return nil
	}

	c.id = id

	return nil
}

// StartReadToChannel implements iface.Connector.
func (c *connector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannel iface.DataChannelID) error {
	f, err := os.Open(filepath.Join(c.path, "bootstrap.json"))
	if err != nil {
		return err
	}
	defer f.Close()

	channel, err := c.t.GetDataChannelEndpoint(dataChannel)
	if err != nil {
		return err
	}

	var batch [][]byte
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var res interface{}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		err = bson.UnmarshalExtJSON([]byte(line), true, &res)
		if err != nil {
			return err
		}
		bsonBytes, err := bson.Marshal(res)
		if err != nil {
			return err
		}
		batch = append(batch, bsonBytes)
	}
	err = f.Close()
	if err != nil {
		return err
	}

	f2, err := os.Open(filepath.Join(c.path, "updates.json"))
	if err != nil {
		return err
	}
	defer f2.Close()

	scanner2 := bufio.NewScanner(f2)
	var allSplitted [][]string
	for scanner2.Scan() {
		line := strings.TrimSpace(scanner2.Text())
		if line == "" {
			continue
		}
		splitted := strings.SplitN(line, "\t", 3)
		allSplitted = append(allSplitted, splitted)
	}
	err = f2.Close()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, task := range readPlan.Tasks {
		id := task.Id
		db := task.Def.Db
		col := task.Def.Col
		wg.Add(1)
		go func() {
			defer wg.Done()

			channel <- iface.DataMessage{
				DataBatch:    batch,
				MutationType: iface.MutationType_InsertBatch,
				Loc: iface.Location{
					Database:   db,
					Collection: col,
				},
			}

			channel <- iface.DataMessage{
				MutationType: iface.MutationType_Barrier,
				BarrierType:  iface.BarrierType_Block,
			}
		Loop:
			for {
				for _, splitted := range allSplitted {
					var idRes interface{}
					// Weird, but wrapping the id this lets us parse the type properly
					err := bson.UnmarshalExtJSON([]byte("{\"id\": "+splitted[1]+"}"), true, &idRes)
					if err != nil {
						slog.Error(err.Error())
						break Loop
					}
					idType, idVal, err := bson.MarshalValue(idRes.(bson.D)[0].Value)

					if err != nil {
						slog.Error(err.Error())
						break Loop
					}

					var data *[]byte
					var mutationType uint

					if splitted[0] == "delete" {
						mutationType = iface.MutationType_Delete
					} else if splitted[0] == "insert" {
						mutationType = iface.MutationType_Insert
						var res interface{}
						err = bson.UnmarshalExtJSON([]byte(splitted[2]), true, &res)
						if err != nil {
							slog.Error(err.Error())
							break Loop
						}
						doc, err := bson.Marshal(res)
						if err != nil {
							slog.Error(err.Error())
							break Loop
						}
						data = &doc
					} else if splitted[0] == "update" {
						mutationType = iface.MutationType_Update
						var res interface{}
						err = bson.UnmarshalExtJSON([]byte(splitted[2]), true, &res)
						if err != nil {
							slog.Error(err.Error())
							break Loop
						}
						doc, err := bson.Marshal(res)
						if err != nil {
							slog.Error(err.Error())
							break Loop
						}
						data = &doc
					}
					select {
					case <-c.flowCtx.Done():
						return
					case channel <- iface.DataMessage{
						Data:         data,
						MutationType: mutationType,
						Loc: iface.Location{
							Database:   db,
							Collection: col,
						},
						Id:     &idVal,
						IdType: byte(idType),
					}:
					}
				}
				if !c.loop {
					break
				}
			}

			err = c.coord.NotifyTaskDone(flowId, c.id, id, nil)
			if err != nil {
				slog.Error(err.Error())
			}
		}()
	}

	go func() {
		wg.Wait()
		close(channel)
		c.coord.NotifyDone(flowId, c.id)
	}()

	return nil
}

// StartWriteFromChannel implements iface.Connector.
func (c *connector) StartWriteFromChannel(flowId iface.FlowID, dataChannel iface.DataChannelID) error {
	channel, err := c.t.GetDataChannelEndpoint(dataChannel)
	if err != nil {
		return err
	}
	bootstrapFile, err := os.Create(filepath.Join(c.path, "bootstrap.json"))
	if err != nil {
		return err
	}
	updatesFile, err := os.Create(filepath.Join(c.path, "updates.json"))
	if err != nil {
		return err
	}

	go func() {
		defer bootstrapFile.Close()
		defer updatesFile.Close()
		defer c.coord.NotifyDone(flowId, c.id)
		for dataMsg := range channel {
			switch dataMsg.MutationType {
			case iface.MutationType_InsertBatch:
				for _, bsonData := range dataMsg.DataBatch {
					_, err = bootstrapFile.WriteString(bson.Raw(bsonData).String() + "\n")
					if err != nil {
						slog.Error(err.Error())
						return
					}
				}
			case iface.MutationType_Insert:
				idBson := bson.RawValue{Type: bsontype.Type(dataMsg.IdType), Value: *dataMsg.Id}
				_, err = updatesFile.WriteString("insert\t" + idBson.String() + "\t" + bson.Raw(*dataMsg.Data).String() + "\n")
				if err != nil {
					slog.Error(err.Error())
					return
				}
			case iface.MutationType_Update:
				idBson := bson.RawValue{Type: bsontype.Type(dataMsg.IdType), Value: *dataMsg.Id}
				_, err = updatesFile.WriteString("update\t" + idBson.String() + "\t" + bson.Raw(*dataMsg.Data).String() + "\n")
				if err != nil {
					slog.Error(err.Error())
					return
				}
			case iface.MutationType_Delete:
				idBson := bson.RawValue{Type: bsontype.Type(dataMsg.IdType), Value: *dataMsg.Id}
				_, err = updatesFile.WriteString("delete\t" + idBson.String() + "\n")
				if err != nil {
					slog.Error(err.Error())
					return
				}
			default:
				slog.Warn("unsupported mutation type", "mutation_type", dataMsg.MutationType)
			}
		}
	}()

	return nil
}

// Teardown implements iface.Connector.
func (c *connector) Teardown() {
	if c.flowCancelFunc != nil {
		c.flowCancelFunc()
	}
}
