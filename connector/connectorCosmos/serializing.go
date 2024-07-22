package connectorCosmos

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
)

func encodeMap(m map[iface.Location]bson.Raw) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("failed to encode map: %v", err)
	}
	return buf.Bytes(), nil
}

func decodeMap(b []byte) (map[iface.Location]bson.Raw, error) {
	var m map[iface.Location]bson.Raw
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&m)
	if err != nil {
		return nil, fmt.Errorf("failed to decode map: %v", err)
	}
	return m, nil
}
