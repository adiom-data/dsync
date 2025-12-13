package util

import (
	"slices"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

	"github.com/cespare/xxhash"
)

type dataIdIndex struct {
	dataId []*adiomv1.BsonValue
	index  int
}

// returns the new item or existing item, and whether or not a new item was added
func addToIdIndexMap(m map[uint64][]*dataIdIndex, update *adiomv1.Update) (*dataIdIndex, bool) {
	hasher := xxhash.New()
	id := update.GetId()
	for _, idPart := range id {
		_, _ = hasher.Write(idPart.GetData())
	}
	h := hasher.Sum64()
	items, found := m[h]
	if found {
		for _, item := range items {
			match := true
			if len(item.dataId) == len(id) {
				for i, idPart := range item.dataId {
					if !slices.Equal(idPart.GetData(), id[i].GetData()) {
						match = false
						break
					}
				}
			} else {
				match = false
			}
			if match {
				return item, false
			}
		}
	}
	item := &dataIdIndex{update.GetId(), -1}
	m[h] = append(items, item)
	return item, true
}

func KeepLastUpdate(updates []*adiomv1.Update) []*adiomv1.Update {
	hashToDataIdIndex := map[uint64][]*dataIdIndex{}
	var res []*adiomv1.Update
	for _, update := range updates {
		dii, isNew := addToIdIndexMap(hashToDataIdIndex, update)
		if isNew {
			dii.index = len(res)
			res = append(res, update)
		} else {
			res[dii.index] = update
		}
	}
	return res
}
