package iface

import "context"

type Runner interface {
	// General
	Setup()
	Run(ctx context.Context)
	Teardown()
}
