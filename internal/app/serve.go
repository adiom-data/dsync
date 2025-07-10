package dsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/logger"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
)

var serveCommand *cli.Command = &cli.Command{
	Name:   "serve",
	Action: runServe,
	Usage:  "dsync serve --address localhost:8088 [other-options] connector [connector-options]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "verbosity",
			Usage: "DEBUG|INFO|WARN|ERROR",
			Value: "INFO",
		},
		&cli.StringFlag{
			Name:     "address",
			Usage:    "host:port e.g. localhost:8080",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "cert-file",
			Usage: "Cerificate file",
		},
		&cli.StringFlag{
			Name:  "key-file",
			Usage: "Key file",
		},
		&cli.BoolFlag{
			Name:  "no-gzip",
			Usage: "Disable gzip compression",
		},
	},
}

func runServe(c *cli.Context) error {
	logger.Setup(logger.Options{Verbosity: c.String("verbosity")})

	address := c.String("address")
	certFile := c.String("cert-file")
	keyFile := c.String("key-file")
	noGzip := c.Bool("no-gzip")
	clearText := false
	if certFile == "" && keyFile == "" {
		clearText = true
	}

	args := c.Args().Slice()
	var connector adiomv1connect.ConnectorServiceHandler
	registeredConnectors := options.GetRegisteredConnectors()
	if len(args) < 1 {
		return fmt.Errorf("missing target connector")
	}
	for _, registeredConnector := range registeredConnectors {
		if registeredConnector.IsConnector(args[0]) {
			if registeredConnector.Create != nil {
				var err error
				connector, _, err = registeredConnector.Create(args, options.AdditionalSettings{})
				if err != nil {
					return fmt.Errorf("error creating connector")
				}
			}
		}
	}
	if connector == nil {
		return fmt.Errorf("unable to create target connector")
	}

	reflector := grpcreflect.NewStaticReflector(adiomv1connect.ConnectorServiceName)
	mux := http.NewServeMux()
	var opts []connect.HandlerOption
	if noGzip {
		opts = append(opts, connect.WithCompression("gzip", nil, nil))
	}
	path, serviceHandler := adiomv1connect.NewConnectorServiceHandler(connector, opts...)
	mux.Handle(path, serviceHandler)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))
	var handler http.Handler

	if clearText {
		handler = h2c.NewHandler(mux, &http2.Server{})
	} else {
		handler = mux
	}

	srv := http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: time.Second * 10,
	}

	var eg errgroup.Group
	eg.Go(func() error {
		if clearText {
			slog.Info("Cleartext server.")
			return srv.ListenAndServe()
		} else {
			return srv.ListenAndServeTLS(certFile, keyFile)
		}
	})

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	eg.Go(func() error {
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()
		return srv.Shutdown(ctx)
	})

	if err := eg.Wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Warn("Shutdown was not clean", "err", err)
		return err
	}

	return nil
}
