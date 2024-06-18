package connectorRandom

import "github.com/brianvoe/gofakeit/v7"

type IndexMap struct {
	IDSet   map[uint32]bool
	numDocs int
}

func NewIndexMap() *IndexMap {
	return &IndexMap{IDSet: make(map[uint32]bool)}
}

func (im *IndexMap) AddRandomIndex() uint32 {
	index := gofakeit.Uint32()
	_, exists := im.IDSet[index]

	for exists {
		index = gofakeit.Uint32()
		_, exists = im.IDSet[index]
	}
	im.IDSet[index] = true
	im.numDocs++
	return index
}

func (im *IndexMap) GetRandomKey() uint32 {
	var key uint32
	for k := range im.IDSet {
		key = k
		break
	}
	return key
}

func (im *IndexMap) GetNumDocs() int {
	return im.numDocs
}

func (im *IndexMap) Delete() uint32 {
	key := im.GetRandomKey()
	delete(im.IDSet, key)
	return key
}
