package iface

import "context"

type Statestore interface {
	// General
	Setup(ctx context.Context) error
	Teardown()
}
