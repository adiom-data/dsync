package dsync

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/app/options"
	"github.com/adiom-data/dsync/build"
	"github.com/adiom-data/dsync/logger"
	"github.com/urfave/cli/v2"
)

// NewApp starts the dsync container process.
func NewApp() *cli.App {
	flags, before := options.GetFlagsAndBeforeFunc()

	app := &cli.App{
		Before:    before,
		Flags:     flags,
		Name:      "dsync",
		Usage:     "Copies data from the source to the destination",
		UsageText: "dsync [options]",
		Version:   build.VersionStr,
		Copyright: build.CopyrightStr,
		Action:    runDsync,
	}

	return app
}

func runDsync(c *cli.Context) error {
	o := options.NewFromCLIContext(c)
	lo := logger.Options{Verbosity: o.Verbosity}

	logger.Setup(lo)
	slog.Debug(fmt.Sprintf("%+v", o))

	return nil
}
