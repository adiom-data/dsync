package options

import (
	"github.com/urfave/cli/v2"
)

type Options struct {
	Verbosity string
	LogFile   string
}

func NewFromCLIContext(c *cli.Context) Options {
	o := Options{}

	o.Verbosity = c.String("verbosity")

	if c.IsSet("logFile") {
		o.LogFile = c.String("logFile")
	}

	return o
}
