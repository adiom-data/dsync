package options

import (
	"github.com/urfave/cli/v2"
)

type Options struct {
	Verbosity string
}

func NewFromCLIContext(c *cli.Context) Options {
	o := Options{}

	o.Verbosity = c.String("verbosity")

	return o
}
