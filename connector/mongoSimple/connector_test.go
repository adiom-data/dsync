package main

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

// MockMongoClient is a mock of mongo.Client
type MockMongoClient struct {
	*gomock.Controller
}

func (m *MockMongoClient) Database(name string) *mongo.Database {
	return &mongo.Database{}
}

func TestNewMongoConnector(t *testing.T) {
	ctx := context.Background()
	connector, err := NewMongoConnector(ctx, "mongodb://source:27017", "mongodb://dest:27017", "sourceDB", "destDB")
	assert.NoError(t, err)
	assert.NotNil(t, connector)
	assert.Equal(t, "sourceDB", connector.sourceDB)
	assert.Equal(t, "destDB", connector.destDB)
}

func TestCalculateChecksum(t *testing.T) {
	mc := &MongoConnector{}
	data := []byte("test data")
	checksum := mc.calculateChecksum(data)
	assert.Equal(t, "eb733a00c0c9d336e65691a37ab54293", checksum)
}

func TestUpdateResumeToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockMongoClient(ctrl)
	mockColl := NewMockCollection(ctrl)

	mc := &MongoConnector{
		destClient: mockClient,
		destDB:     "testDB",
	}

	ctx := context.Background()
	token := bson.Raw{0x01, 0x02, 0x03}

	mockClient.EXPECT().
		Database("testDB").
		Return(NewMockDatabase(ctrl))

	mockClient.Database("testDB").(*MockDatabase).EXPECT().
		Collection(resumeTokenCollection).
		Return(mockColl)

	mockColl.EXPECT().
		UpdateOne(
			ctx,
			bson.M{"_id": resumeTokenID},
			bson.M{"$set": bson.M{"token": token}},
			options.Update().SetUpsert(true),
		).
		Return(&mongo.UpdateResult{}, nil)

	err := mc.updateResumeToken(ctx, token)
	assert.NoError(t, err)
}

func TestGetLatestResumeToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockMongoClient(ctrl)
	mockColl := NewMockCollection(ctrl)

	mc := &MongoConnector{
		destClient: mockClient,
		destDB:     "testDB",
	}

	ctx := context.Background()
	expectedToken := bson.Raw{0x01, 0x02, 0x03}

	mockClient.EXPECT().
		Database("testDB").
		Return(NewMockDatabase(ctrl))

	mockClient.Database("testDB").(*MockDatabase).EXPECT().
		Collection(resumeTokenCollection).
		Return(mockColl)

	mockColl.EXPECT().
		FindOne(ctx, bson.M{"_id": resumeTokenID}).
		Return(NewMockSingleResult(ctrl))

	mockColl.FindOne(ctx, bson.M{"_id": resumeTokenID}).(*MockSingleResult).EXPECT().
		Decode(gomock.Any()).
		SetArg(0, struct{ Token bson.Raw }{Token: expectedToken}).
		Return(nil)

	token, err := mc.getLatestResumeToken(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expectedToken, token)
}

func TestProcessChangeEvent(t *testing.T) {
	mc := &MongoConnector{
		sourceDB: "sourceDB",
		stats:    TransferStats{},
	}

	changeEvent := bson.M{
		"operationType": "insert",
		"fullDocument":  bson.Raw{0x01, 0x02, 0x03},
		"ns": bson.M{
			"db":   "sourceDB",
			"coll": "testCollection",
		},
	}

	mc.processChangeEvent(changeEvent)

	assert.Equal(t, int64(1), mc.stats.changeEvents.Load())
	assert.Equal(t, int64(3), mc.stats.bytesProcessed.Load())
}

// Mock implementations for MongoDB interfaces
type MockMongoClient struct {
	gomock.Controller
}

func NewMockMongoClient(ctrl *gomock.Controller) *MockMongoClient {
	return &MockMongoClient{Controller: *ctrl}
}

func (m *MockMongoClient) Database(name string) mongo.Database {
	return NewMockDatabase(m.Controller)
}

type MockDatabase struct {
	gomock.Controller
}

func NewMockDatabase(ctrl gomock.Controller) *MockDatabase {
	return &MockDatabase{Controller: ctrl}
}

func (m *MockDatabase) Collection(name string) mongo.Collection {
	return NewMockCollection(m.Controller)
}

type MockCollection struct {
	gomock.Controller
}

func NewMockCollection(ctrl gomock.Controller) *MockCollection {
	return &MockCollection{Controller: ctrl}
}

func (m *MockCollection) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return &mongo.UpdateResult{}, nil
}

func (m *MockCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) mongo.SingleResult {
	return NewMockSingleResult(m.Controller)
}

type MockSingleResult struct {
	gomock.Controller
}

func NewMockSingleResult(ctrl gomock.Controller) *MockSingleResult {
	return &MockSingleResult{Controller: ctrl}
}

func (m *MockSingleResult) Decode(v interface{}) error {
	return nil
}
