package gobregister

import (
	"encoding/gob"
	"net/netip"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

var cursorTypesOnce sync.Once

// RegisterCursorTypes registers concrete database types stored inside
// interface-valued gob cursors and composite IDs. gob's registry is global, so
// all connectors must share this once guard.
func RegisterCursorTypes() {
	cursorTypesOnce.Do(func() {
		gob.Register(time.Time{})
		gob.Register([16]byte{})
		gob.Register(decimal.Decimal{})
		gob.Register(netip.Addr{})
		gob.Register(netip.Prefix{})
	})
}
