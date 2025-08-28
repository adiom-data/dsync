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

var GRPCConnector = ConfigureGRPCFactory("Connector", nil, func(c connect.HTTPClient, s string, opts ...connect.ClientOption) interface{} {
	return adiomv1connect.NewConnectorServiceClient(c, s, opts...)
})

var ErrMissingEmbedder = fmt.Errorf("embedder is missing")
var ErrInvalidEmbedder = fmt.Errorf("embedder must start with grpc://")
var Embedder = ConfigureGRPCFactory("Embedder", ErrInvalidEmbedder, func(c connect.HTTPClient, s string, opts ...connect.ClientOption) interface{} {
	return adiomv1connect.NewEmbeddingServiceClient(c, s, opts...)
})

var ErrMissingChunker = fmt.Errorf("chunker is missing")
var ErrInvalidChunker = fmt.Errorf("chunker must start with grpc://")
var Chunker = ConfigureGRPCFactory("Chunker", ErrInvalidChunker, func(c connect.HTTPClient, s string, opts ...connect.ClientOption) interface{} {
	return adiomv1connect.NewChunkingServiceClient(c, s, opts...)
})

var ErrMissingTransform = errors.New("transform is missing")
var ErrInvalidTransform = errors.New("transform must start with grpc://")
var Transformer = ConfigureGRPCFactory("Transform", ErrInvalidTransform, func(c connect.HTTPClient, s string, opts ...connect.ClientOption) interface{} {
	return adiomv1connect.NewTransformServiceClient(c, s, opts...)
})

func ConfigureEmbedder(args []string) (adiomv1connect.EmbeddingServiceClient, []string, error) {
	c, restArgs, err := Embedder(args)
	if err != nil {
		return nil, nil, err
	}
	return c.(adiomv1connect.EmbeddingServiceClient), restArgs, err
}

func ConfigureChunker(args []string) (adiomv1connect.ChunkingServiceClient, []string, error) {
	c, restArgs, err := Chunker(args)
	if err != nil {
		return nil, nil, err
	}
	return c.(adiomv1connect.ChunkingServiceClient), restArgs, err
}

func ConfigureTransformer(args []string) (adiomv1connect.TransformServiceClient, []string, error) {
	c, restArgs, err := Transformer(args)
	if err != nil {
		return nil, nil, err
	}
	return c.(adiomv1connect.TransformServiceClient), restArgs, err
}

func ConfigureGRPCFactory(usage string, missingErr error, f func(connect.HTTPClient, string, ...connect.ClientOption) interface{}) func([]string) (interface{}, []string, error) {
	return func(args []string) (interface{}, []string, error) {
		if missingErr != nil && (len(args) == 0 || !strings.HasPrefix(args[0], "grpc://")) {
			return nil, nil, missingErr
		}
		var conn any
		var restArgs []string
		app := &cli.App{
			HelpName:  "grpc",
			Usage:     usage,
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
				conn = f(httpClient, finalEndpoint, options...)
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
}
