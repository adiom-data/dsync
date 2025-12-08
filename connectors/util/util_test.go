package util_test

import (
	"testing"

	"github.com/adiom-data/dsync/connectors/util"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestKeepLastUpdate(t *testing.T) {
	testData := []struct {
		Name     string
		Updates  []*adiomv1.Update
		Expected []*adiomv1.Update
	}{
		{
			Name:     "nil",
			Updates:  nil,
			Expected: nil,
		},
		{
			Name:     "empty",
			Updates:  []*adiomv1.Update{},
			Expected: []*adiomv1.Update{},
		},
		{
			Name:     "1 item",
			Updates:  []*adiomv1.Update{{}},
			Expected: []*adiomv1.Update{{}},
		},
		{
			Name:     "2 items",
			Updates:  []*adiomv1.Update{{}, {}},
			Expected: []*adiomv1.Update{{}},
		},
		{
			Name: "3 items with 1 dupe",
			Updates: []*adiomv1.Update{
				{
					Id: []*adiomv1.BsonValue{{
						Data: []byte{1},
					}, {
						Data: []byte{2},
					}},
					Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
				},
				{
					Id: []*adiomv1.BsonValue{{
						Data: []byte{1},
					}, {
						Data: []byte{1},
					}},
					Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
				},
				{
					Id: []*adiomv1.BsonValue{{
						Data: []byte{1},
					}, {
						Data: []byte{2},
					}},
					Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
				},
			},
			Expected: []*adiomv1.Update{
				{
					Id: []*adiomv1.BsonValue{{
						Data: []byte{1},
					}, {
						Data: []byte{2},
					}},
					Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
				},
				{
					Id: []*adiomv1.BsonValue{{
						Data: []byte{1},
					}, {
						Data: []byte{1},
					}},
					Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
				},
			},
		},
	}
	for _, testCase := range testData {
		t.Run(testCase.Name, func(t *testing.T) {
			result := util.KeepLastUpdate(testCase.Updates)
			if assert.Len(t, result, len(testCase.Expected)) {
				for i := range result {
					assert.True(t, proto.Equal(testCase.Expected[i], result[i]))
				}
			}
		})
	}
}
