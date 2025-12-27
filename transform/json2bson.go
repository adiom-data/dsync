package transform

import (
	"context"
	"encoding/json"
	"fmt"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
)

type json2BsonTransform struct {
}

func (i *json2BsonTransform) mapData(d []byte) []byte {
	var m map[string]any
	err := json.Unmarshal(d, &m)
	if err != nil {
		panic(err)
	}
	m["_id"] = m["id"]
	delete(m, "id")
	out, err := bson.Marshal(m)
	if err != nil {
		panic(err)
	}

	return out
}

func (i *json2BsonTransform) GetTransform(_ context.Context, r *connect.Request[adiomv1.GetTransformRequest]) (*connect.Response[adiomv1.GetTransformResponse], error) {
	if r.Msg.GetRequestType() != adiomv1.DataType_DATA_TYPE_JSON_ID && r.Msg.GetResponseType() != adiomv1.DataType_DATA_TYPE_MONGO_BSON {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unsupported request and response type"))
	}

	var newUpdates []*adiomv1.Update
	var newData [][]byte

	for _, update := range r.Msg.GetUpdates() {
		newUpdates = append(newUpdates, &adiomv1.Update{
			Id:   update.GetId(),
			Type: update.GetType(),
			Data: i.mapData(update.GetData()),
		})
	}

	for _, data := range r.Msg.GetData() {
		newData = append(newData, i.mapData(data))
	}

	return connect.NewResponse(&adiomv1.GetTransformResponse{
		Namespace: r.Msg.Namespace,
		Updates:   newUpdates,
		Data:      newData,
	}), nil
}

func (i *json2BsonTransform) GetTransformInfo(context.Context, *connect.Request[adiomv1.GetTransformInfoRequest]) (*connect.Response[adiomv1.GetTransformInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetTransformInfoResponse{
		Transforms: []*adiomv1.GetTransformInfoResponse_TransformInfo{
			{
				RequestType:   adiomv1.DataType_DATA_TYPE_JSON_ID,
				ResponseTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
			},
		},
	}), nil
}

func NewJson2BsonTransform() adiomv1connect.TransformServiceHandler {
	return &json2BsonTransform{}
}
