/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorCosmos

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
)

type TokenMap struct {
	Map   map[iface.Location]bson.Raw
	mutex sync.RWMutex
}

func NewTokenMap() *TokenMap {
	return &TokenMap{Map: make(map[iface.Location]bson.Raw)}
}

// atomic add key value pair to the map
func (tm *TokenMap) AddToken(key iface.Location, value bson.Raw) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.Map[key] = value
}

// atomic get value from the map by key
func (tm *TokenMap) GetToken(key iface.Location) (bson.Raw, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	token, exists := tm.Map[key]
	if !exists {
		return nil, fmt.Errorf("token not found for key: %v", key)
	}
	return token, nil
}

// atomic serialization to byte array
func (tm *TokenMap) encodeMap() ([]byte, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	//sort the list of keys for deterministic encoding
	var keys []iface.Location
	for k := range tm.Map {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		loc1 := keys[i].Database + keys[i].Collection
		loc2 := keys[j].Database + keys[j].Collection
		return loc1 < loc2 //sort by string comparison of "database.collection"
	})

	//encode key value pairs one by one
	for _, k := range keys {
		err := enc.Encode(k)
		if err != nil {
			return nil, fmt.Errorf("failed to encode key: %v", err)
		}
		err = enc.Encode(tm.Map[k])
		if err != nil {
			return nil, fmt.Errorf("failed to encode value: %v", err)
		}
	}

	return buf.Bytes(), nil
}

// atomic deserialization from byte array, updates Map attribute
func (tm *TokenMap) decodeMap(b []byte) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	//decode the key value pairs one by one, and update the map, break if end of stream
	for {
		//decode the key
		var k iface.Location
		err := dec.Decode(&k)
		if err != nil {
			if err == io.EOF {
				// End of the encoded stream, break out of the loop
				break
			}
			return fmt.Errorf("failed to decode key: %v", err)
		}
		//decode the value
		var v bson.Raw
		err = dec.Decode(&v)
		if err != nil {
			return fmt.Errorf("failed to decode value: %v", err)
		}
		tm.Map[k] = v
	}
	return nil
}
