package options

import (
	"github.com/urfave/cli/v2"
)

type Options struct {
	Verbosity string

	SrcConnString        string
	DstConnString        string
	StateStoreConnString string
}

func NewFromCLIContext(c *cli.Context) Options {
	o := Options{}

	o.Verbosity = c.String("verbosity")
	o.SrcConnString = c.String("source")
	o.DstConnString = c.String("destination")
	o.StateStoreConnString = c.String("metadata")

	return o
}
