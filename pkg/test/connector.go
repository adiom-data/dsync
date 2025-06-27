package test

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.akshayshah.org/memhttp"
	"go.mongodb.org/mongo-driver/bson"
)

type ConnectorTestSuite struct {
	suite.Suite
	namespace            string
	connectorFactoryFunc func() adiomv1connect.ConnectorServiceClient

	Bootstrap         func(context.Context) error
	InsertUpdates     func(context.Context) error
	AssertExists      func(context.Context, *assert.Assertions, []*adiomv1.BsonValue, bool) error
	NumPages          int
	NumItems          int
	SkipDuplicateTest bool
}

func ClientFromHandler(h adiomv1connect.ConnectorServiceHandler) adiomv1connect.ConnectorServiceClient {
	_, handler := adiomv1connect.NewConnectorServiceHandler(h)
	srv, err := memhttp.New(handler)
	if err != nil {
		panic(err)
	}
	return adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())
}

func NewConnectorTestSuite(namespace string, connectorFactoryFunc func() adiomv1connect.ConnectorServiceClient, bootstrap func(context.Context) error, insertUpdates func(context.Context) error, numPages int, numItems int) *ConnectorTestSuite {
	return &ConnectorTestSuite{namespace: namespace, connectorFactoryFunc: connectorFactoryFunc, Bootstrap: bootstrap, InsertUpdates: insertUpdates, NumPages: numPages, NumItems: numItems}
}

func (suite *ConnectorTestSuite) TestAll() {
	c := suite.connectorFactoryFunc()
	if suite.AssertExists == nil {
		suite.AssertExists = func(context.Context, *assert.Assertions, []*adiomv1.BsonValue, bool) error {
			return nil
		}
	}
	ctx := context.Background()

	if suite.Bootstrap != nil {
		suite.Assert().NoError(suite.Bootstrap(ctx))
	}

	res, err := c.GetInfo(ctx, connect.NewRequest(&adiomv1.GetInfoRequest{}))
	suite.Run("TestGetInfo", func() {
		suite.Assert().NoError(err)
		suite.Assert().NotNil(res.Msg.GetCapabilities(), "capabilities must be defined")
		suite.Assert().NotEmpty(res.Msg.GetDbType(), "db type should be specified")
		suite.Assert().True(res.Msg.GetCapabilities().GetSink() != nil || res.Msg.GetCapabilities().GetSource() != nil, "must be at least a sink or a source")
	})

	capabilities := res.Msg.GetCapabilities()

	if capabilities.GetSource() != nil {
		suite.Run("TestSource", func() {

			suite.Assert().NotEmpty(capabilities.GetSource().GetSupportedDataTypes())

			planRes, err := c.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{Namespaces: []string{suite.namespace}}))

			suite.Run("TestGeneratePlan", func() {
				suite.Assert().NoError(err)
				suite.Assert().NotEmpty(planRes.Msg.GetPartitions())
				for _, p := range planRes.Msg.GetPartitions() {
					suite.Assert().NotEmpty(p.GetNamespace())
				}
			})
			suite.Run("TestGetNamespaceMetadata", func() {
				res, err := c.GetNamespaceMetadata(ctx, connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{
					Namespace: suite.namespace,
				}))
				suite.Assert().NoError(err)
				suite.Assert().NotNil(res.Msg)
			})

			suite.Run("TestListData", func() {
				for _, t := range capabilities.GetSource().GetSupportedDataTypes() {
					var pageCount int
					for _, p := range planRes.Msg.GetPartitions() {
						var cursor []byte
						var itemCount int
						for {
							res1, err := c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
								Partition: p,
								Type:      t,
								Cursor:    cursor,
							}))
							suite.Assert().NoError(err)
							pageCount++
							itemCount += len(res1.Msg.GetData())

							res2, err := c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
								Partition: p,
								Type:      t,
								Cursor:    cursor,
							}))
							suite.Assert().NoError(err)
							if !suite.SkipDuplicateTest {
								suite.Assert().Equal(res1.Msg.GetData(), res2.Msg.GetData(), "Repeated calls with the same cursor should return identical data")
							}

							cursor = res1.Msg.GetNextCursor()
							if cursor == nil {
								break
							}

							res3, err := c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
								Partition: p,
								Type:      t,
								Cursor:    cursor,
							}))
							suite.Assert().NoError(err)
							suite.Assert().NotEqual(cursor, res3.Msg.GetNextCursor(), "Cursor should advance for the next page")
						}
						suite.Assert().Equal(suite.NumItems, itemCount, "Should process at least %d items", suite.NumItems)
					}
					suite.Assert().Equal(suite.NumPages, pageCount, "Should process at least %d pages of data", suite.NumPages)
				}
			})

			if suite.InsertUpdates != nil {
				suite.Assert().NoError(suite.InsertUpdates(ctx))
			}

			suite.Run("TestStreamUpdates", func() {
				for _, t := range capabilities.GetSource().GetSupportedDataTypes() {
					for _, p := range planRes.Msg.GetUpdatesPartitions() {
						namespaces := p.GetNamespaces()
						ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*5))
						defer cancel()
						res, err := c.StreamUpdates(ctx, connect.NewRequest(&adiomv1.StreamUpdatesRequest{
							Namespaces: namespaces,
							Type:       t,
							Cursor:     p.GetCursor(),
						}))
						suite.Assert().NoError(err)
						gotSomething := false
						for res.Receive() {
							suite.Assert().NotNil(res.Msg())
							if len(res.Msg().GetUpdates()) > 0 {
								gotSomething = true
								cancel()
							}
						}
						if res.Err() != nil && !errors.Is(res.Err(), context.Canceled) {
							suite.Assert().Fail("Not expecting an error")
						}
						suite.Assert().True(gotSomething, "Expecting at least 1 update")
					}
				}
			})

			suite.Run("TestStreamLSN", func() {
				if capabilities.GetSource().GetLsnStream() {
					for _, p := range planRes.Msg.GetUpdatesPartitions() {
						namespaces := p.GetNamespaces()
						ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*5))
						defer cancel()
						res, err := c.StreamLSN(ctx, connect.NewRequest(&adiomv1.StreamLSNRequest{
							Namespaces: namespaces,
							Cursor:     p.GetCursor(),
						}))
						suite.Assert().NoError(err)
						gotSomething := false
						for res.Receive() {
							suite.Assert().NotNil(res.Msg())
							gotSomething = true
							cancel()
						}
						if res.Err() != nil && !errors.Is(res.Err(), context.Canceled) {
							suite.Assert().Fail("Not expecting an error")
						}
						suite.Assert().True(gotSomething, "Expecting at least 1 update")
					}
				}
			})
		})
	}

	if capabilities.GetSink() != nil {

		supported := capabilities.GetSink().GetSupportedDataTypes()
		sampleBson, _ := bson.Marshal(bson.D{{"_id", "someid"}, {"k", "v"}})
		sampleBsonUpdate, _ := bson.Marshal(bson.D{{"_id", "someid"}, {"k", "v2"}})
		sampleBsonID := bson.Raw(sampleBson).Lookup("_id")
		sampleBsonIDProto := []*adiomv1.BsonValue{{
			Data: sampleBsonID.Value,
			Type: uint32(sampleBsonID.Type),
		}}

		suite.Run("TestSink", func() {
			suite.Run("TestWriteData", func() {
				for _, t := range supported {
					if t == adiomv1.DataType_DATA_TYPE_MONGO_BSON {
						_ = suite.AssertExists(ctx, suite.Assert(), sampleBsonIDProto, false)
						_, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
							Namespace: suite.namespace,
							Data:      [][]byte{sampleBson},
							Type:      t,
						}))
						suite.Assert().NoError(err)
						_ = suite.AssertExists(ctx, suite.Assert(), sampleBsonIDProto, true)
					}
				}
			})

			suite.Run("TestWriteUpdates", func() {
				for _, t := range supported {
					if t == adiomv1.DataType_DATA_TYPE_MONGO_BSON {
						_ = suite.AssertExists(ctx, suite.Assert(), sampleBsonIDProto, true)
						_, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
							Namespace: suite.namespace,
							Updates: []*adiomv1.Update{{
								Id:   sampleBsonIDProto,
								Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
								Data: sampleBsonUpdate,
							}},
							Type: t,
						}))
						suite.Assert().NoError(err)
						_ = suite.AssertExists(ctx, suite.Assert(), sampleBsonIDProto, true)

						_, err = c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
							Namespace: suite.namespace,
							Updates: []*adiomv1.Update{{
								Id:   sampleBsonIDProto,
								Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
							}},
							Type: t,
						}))
						suite.Assert().NoError(err)
						_ = suite.AssertExists(ctx, suite.Assert(), sampleBsonIDProto, false)

						_, err = c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
							Namespace: suite.namespace,
							Updates: []*adiomv1.Update{{
								Id:   sampleBsonIDProto,
								Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
								Data: sampleBson,
							}},
							Type: t,
						}))
						suite.Assert().NoError(err)
						_ = suite.AssertExists(ctx, suite.Assert(), sampleBsonIDProto, true)
					}
				}
			})
		})
	}
}
