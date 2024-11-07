package options

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

var ErrMissingTransform = errors.New("transform must start with grpc://")

func ConfigureTransformer(args []string) (adiomv1connect.TransformServiceClient, []string, error) {
	if len(args) == 0 || !strings.HasPrefix(args[0], "grpc://") {
		return nil, nil, ErrMissingTransform
	}
	var conn adiomv1connect.TransformServiceClient
	var restArgs []string
	app := &cli.App{
		HelpName:  "grpc",
		Usage:     "Transform",
		UsageText: "grpc://address:port [options]",
		Flags: []cli.Flag{
			altsrc.NewBoolFlag(&cli.BoolFlag{
				Name:  "insecure",
				Usage: "Connect without TLS",
			}),
			altsrc.NewBoolFlag(&cli.BoolFlag{
				Name:  "gzip",
				Usage: "Use gzip on requests",
			}),
		},
		Action: func(c *cli.Context) error {
			restArgs = c.Args().Slice()
			_, endpoint, ok := strings.Cut(args[0], "://")
			if !ok {
				return fmt.Errorf("invalid connection string %v", args[0])
			}
			options := []connect.ClientOption{connect.WithGRPC()}
			if c.Bool("gzip") {
				options = append(options, connect.WithSendGzip())
			}
			finalEndpoint := "https://" + endpoint
			httpClient := http.DefaultClient
			if c.Bool("insecure") {
				httpClient = insecureClient()
				finalEndpoint = "http://" + endpoint
			}
			if _, _, ok := strings.Cut(endpoint, "://"); ok {
				finalEndpoint = endpoint
			}
			conn = adiomv1connect.NewTransformServiceClient(httpClient, finalEndpoint, options...)
			return nil
		},
	}
	if err := app.Run(args); err != nil {
		return nil, nil, err
	}
	if conn != nil {
		return conn, restArgs, nil
	}
	return nil, nil, ErrHelp
}
