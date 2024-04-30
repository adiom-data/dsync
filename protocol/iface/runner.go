package iface

import "context"

type Runner interface {
	// General
	Setup(ctx context.Context) error
	Run()
	Teardown()
}
