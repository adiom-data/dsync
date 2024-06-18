package connectorRandom

import (
	"log/slog"

	"github.com/brianvoe/gofakeit/v7"
)

type IndexMap struct {
	IDSet     map[uint32]bool
	indexToID []uint32
}

func NewIndexMap() *IndexMap {
	return &IndexMap{IDSet: make(map[uint32]bool), indexToID: make([]uint32, 0)}
}

func (im *IndexMap) AddRandomID() uint32 {
	id := gofakeit.Uint32()
	_, exists := im.IDSet[id]

	for exists {
		id = gofakeit.Uint32()
		_, exists = im.IDSet[id]
	}
	im.IDSet[id] = true
	im.indexToID = append(im.indexToID, id)
	return id
}

func (im *IndexMap) GetRandomIndex() int {
	len := len(im.indexToID)
	if len == 0 {
		slog.Debug("IndexMap is empty")
		return -1
	}
	idx := gofakeit.Number(0, len-1)
	return idx
}

func (im *IndexMap) DeleteRandomKey() uint32 {
	idx := im.GetRandomIndex()
	key := im.indexToID[idx]
	delete(im.IDSet, key)
	//delete from indexToID slice
	len := len(im.indexToID)
	im.indexToID[idx], im.indexToID[len-1] = im.indexToID[len-1], im.indexToID[idx]
	im.indexToID = im.indexToID[:len-1]
	return key
}
