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

func (tm *TokenMap) AddToken(key iface.Location, value bson.Raw) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.Map[key] = value
}

func (tm *TokenMap) GetToken(key iface.Location) (bson.Raw, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	token, exists := tm.Map[key]
	if !exists {
		return nil, fmt.Errorf("token not found for key: %v", key)
	}
	return token, nil
}

func (tm *TokenMap) encodeMap() ([]byte, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	var keys []iface.Location
	for k := range tm.Map {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		loc1 := keys[i].Database + keys[i].Collection
		loc2 := keys[j].Database + keys[j].Collection
		return loc1 < loc2
	})

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

func (tm *TokenMap) decodeMap(b []byte) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	//var keys []iface.Location
	for {
		var k iface.Location
		err := dec.Decode(&k)
		if err != nil {
			if err == io.EOF {
				// End of the encoded stream, break out of the loop
				break
			}
			return fmt.Errorf("failed to decode key: %v", err)
		}
		var v bson.Raw
		err = dec.Decode(&v)
		if err != nil {
			return fmt.Errorf("failed to decode value: %v", err)
		}
		tm.Map[k] = v
	}
	return nil
	/*
	   err := dec.Decode(&tm.Map)

	   	if err != nil {
	   		return fmt.Errorf("failed to decode map: %v", err)
	   	}

	   return nil
	*/
}
