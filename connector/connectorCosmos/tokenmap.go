package connectorCosmos

import (
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
