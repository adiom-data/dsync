package connectorCosmos

import (
	"bytes"
	"encoding/gob"
	"fmt"
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
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(tm.Map)
	if err != nil {
		return nil, fmt.Errorf("failed to encode map: %v", err)
	}
	return buf.Bytes(), nil
}

func (tm *TokenMap) decodeMap(b []byte) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&tm.Map)
	if err != nil {
		return fmt.Errorf("failed to decode map: %v", err)
	}
	return nil
}
