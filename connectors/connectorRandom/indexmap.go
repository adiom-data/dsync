/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorRandom

import (
	"log/slog"
	"sync"

	"github.com/brianvoe/gofakeit/v7"
)

type IndexMap struct {
	IDSet   map[uint32]bool
	IDArray []uint32
	mutex   sync.RWMutex
}

func NewIndexMap() *IndexMap {
	return &IndexMap{IDSet: make(map[uint32]bool), IDArray: make([]uint32, 0)}
}

func (im *IndexMap) AddRandomID() uint32 {
	//Atomic
	im.mutex.Lock()
	defer im.mutex.Unlock()

	id := gofakeit.Uint32()
	_, exists := im.IDSet[id]

	for exists {
		id = gofakeit.Uint32()
		_, exists = im.IDSet[id]
	}

	im.IDSet[id] = true
	im.IDArray = append(im.IDArray, id)
	return id
}

func (im *IndexMap) getRandomIndex_unsafe() int {
	length := len(im.IDArray)
	if length == 0 {
		slog.Debug("IndexMap is empty")
		return -1
	}
	idx := gofakeit.Number(0, length-1)
	return idx
}

func (im *IndexMap) GetRandomKey() uint32 {
	//Atomic
	im.mutex.RLock()
	defer im.mutex.RUnlock()
	idx := im.getRandomIndex_unsafe()
	return im.IDArray[idx]
}

func (im *IndexMap) DeleteRandomKey() uint32 {
	//Atomic
	im.mutex.Lock()
	defer im.mutex.Unlock()

	idx := im.getRandomIndex_unsafe()
	key := im.IDArray[idx]
	delete(im.IDSet, key)
	//delete from IDArray slice
	length := len(im.IDArray)
	im.IDArray[idx], im.IDArray[length-1] = im.IDArray[length-1], im.IDArray[idx] //swap with last element to make delete O(1)
	im.IDArray = im.IDArray[:length-1]
	return key
}

func (im *IndexMap) GetNumDocs() int {
	//Atomic
	im.mutex.RLock()
	defer im.mutex.RUnlock()
	return len(im.IDArray)
}
