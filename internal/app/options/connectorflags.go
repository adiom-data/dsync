package options

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/airbyte"
	"github.com/adiom-data/dsync/connectors/cosmos"
	"github.com/adiom-data/dsync/connectors/dynamodb"
	"github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/connectors/null"
	"github.com/adiom-data/dsync/connectors/random"
	"github.com/adiom-data/dsync/connectors/testconn"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
	"golang.org/x/net/http2"
)

var ErrMissingConnector = errors.New("missing or unsupported connector")
var ErrHelp = errors.New("connector help used")

type AdditionalSettings struct {
	BaseThreadCount int
}

type RegisteredConnector struct {
	Name        string
	IsConnector func(string) bool

	// One of Create or CreateRemote should be not nil
	// Create is for optimizing a local implementation
	Create       func([]string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error)
	CreateRemote func([]string, AdditionalSettings) (adiomv1connect.ConnectorServiceClient, []string, error)
}

type ConfiguredConnector struct {
	Local  adiomv1connect.ConnectorServiceHandler
	Remote adiomv1connect.ConnectorServiceClient
}

func ConfigureConnectors(args []string, additionalSettings AdditionalSettings) (ConfiguredConnector, ConfiguredConnector, []string, error) {
	var src ConfiguredConnector
	var dst ConfiguredConnector
	var err error
	srcArgs := args
	registeredConnectors := GetRegisteredConnectors()

	if len(srcArgs) < 1 {
		return src, dst, nil, fmt.Errorf("missing source: %w", ErrMissingConnector)
	}
	var dstArgs []string
	for _, registeredConnector := range registeredConnectors {
		if registeredConnector.IsConnector(srcArgs[0]) {
			if registeredConnector.Create != nil {
				if src.Local, dstArgs, err = registeredConnector.Create(srcArgs, additionalSettings); err != nil {
					return src, dst, nil, err
				}
			} else {
				if src.Remote, dstArgs, err = registeredConnector.CreateRemote(srcArgs, additionalSettings); err != nil {
					return src, dst, nil, err
				}
			}
			break
		} else if strings.EqualFold(srcArgs[0], registeredConnector.Name) {
			if registeredConnector.Create != nil {
				_, _, err = registeredConnector.Create([]string{srcArgs[0], "--help"}, additionalSettings)
			} else {
				_, _, err = registeredConnector.CreateRemote([]string{srcArgs[0], "--help"}, additionalSettings)
			}
			return src, dst, nil, err
		}
	}
	if src.Local == nil && src.Remote == nil {
		return src, dst, nil, fmt.Errorf("unsupported source: %w", ErrMissingConnector)
	}

	if len(dstArgs) < 1 {
		return src, dst, nil, fmt.Errorf("missing destination: %w", ErrMissingConnector)
	}
	var restArgs []string
	for _, registeredConnector := range registeredConnectors {
		if registeredConnector.IsConnector(dstArgs[0]) {
			if registeredConnector.Create != nil {
				if dst.Local, restArgs, err = registeredConnector.Create(dstArgs, additionalSettings); err != nil {
					return src, dst, nil, err
				}
			} else {
				if dst.Remote, restArgs, err = registeredConnector.CreateRemote(dstArgs, additionalSettings); err != nil {
					return src, dst, nil, err
				}
			}
			break
		} else if strings.EqualFold(dstArgs[0], registeredConnector.Name) {
			if registeredConnector.Create != nil {
				_, _, err = registeredConnector.Create([]string{dstArgs[0], "help"}, additionalSettings)
			} else {
				_, _, err = registeredConnector.CreateRemote([]string{dstArgs[0], "help"}, additionalSettings)
			}
			return src, dst, nil, err
		}
	}
	if dst.Local == nil && dst.Remote == nil {
		return src, dst, nil, fmt.Errorf("unsupported destination: %w", ErrMissingConnector)
	}
	return src, dst, restArgs, nil
}

func CreateHelper(name string, usage string, flags []cli.Flag, action func(*cli.Context, []string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error)) func([]string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
	return func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
		var conn adiomv1connect.ConnectorServiceHandler
		var restArgs []string
		app := &cli.App{
			HelpName:  name,
			Usage:     "Connector",
			UsageText: usage,
			Flags:     flags,
			Action: func(c *cli.Context) error {
				var err error
				restArgs = c.Args().Slice()
				conn, err = action(c, args, as)
				return err
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

// This could later be set up such that each connector registers itself instead of being centralized here
func GetRegisteredConnectors() []RegisteredConnector {
	return []RegisteredConnector{
		{
			Name: "/dev/random",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "/dev/random")
			},
			Create: CreateHelper("/dev/random", "/dev/random", nil, func(*cli.Context, []string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				return random.NewConn(random.ConnectorSettings{}), nil
			}),
		},
		{
			Name: "/dev/null",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "/dev/null")
			},
			Create: CreateHelper("/dev/null", "/dev/null", nil, func(*cli.Context, []string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				return null.NewConn(), nil
			}),
		},
		{
			Name: "testconn",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(s, "testconn://")
			},
			Create: CreateHelper("testconn", "testconn://./fixture", nil, func(_ *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				_, connString, ok := strings.Cut(args[0], "://")
				if !ok {
					return nil, fmt.Errorf("invalid connection string %v", args[0])
				}
				return testconn.NewConn(connString), nil
			}),
		},
		{
			Name: "DynamoDB",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "dynamodb") || strings.EqualFold(s, "dynamodb://localstack")
			},
			Create: CreateHelper("DynamoDB", "dynamodb OR dynamodb://localstack", nil, func(_ *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				if strings.EqualFold(args[0], "dynamodb://localstack") {
					_, connString, ok := strings.Cut(args[0], "://")
					if !ok {
						return nil, fmt.Errorf("invalid connection string %v", args[0])
					}
					return dynamodb.NewConn(connString), nil
				} else {
					return dynamodb.NewConn(""), nil
				}
			}),
		},
		{
			Name: "CosmosDB",
			IsConnector: func(s string) bool {
				if strings.HasPrefix(s, "mongodb://") || strings.HasPrefix(s, "mongodb+srv://") {
					return mongo.GetMongoFlavor(s) == mongo.FlavorCosmosDB
				}
				return false
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				settings := cosmos.ConnectorSettings{}
				settings.ConnectionString = args[0]
				if as.BaseThreadCount != 0 {
					settings.NumParallelPartitionWorkers = as.BaseThreadCount / 2
				}
				return CreateHelper("CosmosDB", "mongodb://cosmos-connection-string [options]", CosmosFlags(&settings), func(_ *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					return cosmos.NewConn(settings), nil
				})(args, as)
			},
		},
		{
			Name: "MongoDB",
			IsConnector: func(s string) bool {
				if strings.HasPrefix(s, "mongodb://") || strings.HasPrefix(s, "mongodb+srv://") {
					return mongo.GetMongoFlavor(s) == mongo.FlavorMongoDB
				}
				return false
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				settings := mongo.ConnectorSettings{ConnectionString: args[0]}
				return CreateHelper("MongoDB", "mongodb://connection-string [options]", MongoFlags(&settings), func(_ *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					return mongo.NewConn(settings), nil
				})(args, as)
			},
		},
		{
			Name: "AirbyteSource",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(s, "airbyte://")
			},
			Create: CreateHelper("AirbyteSource", "airbyte://docker-image", []cli.Flag{
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:     "config",
					Required: true,
				}),
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:  "save-catalog",
					Usage: "Allows the sink to use the source generated catalog by matching the name.",
				}),
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:  "sync-mode",
					Value: "full_refresh",
				}),
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:  "destination-sync-mode",
					Value: "append",
				}),
				altsrc.NewIntFlag(&cli.IntFlag{
					Name:  "generation-id",
					Value: -1,
				}),
				altsrc.NewIntFlag(&cli.IntFlag{
					Name:  "minimum-generation-id",
					Value: -1,
				}),
				altsrc.NewIntFlag(&cli.IntFlag{
					Name:  "sync-id",
					Value: -1,
				}),
				altsrc.NewStringSliceFlag(&cli.StringSliceFlag{
					Name: "cursor-field",
				}),
				altsrc.NewStringSliceFlag(&cli.StringSliceFlag{
					Name:  "primary-key",
					Usage: "Separator for nested field is a dot: '.'",
				}),
			}, func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				_, dockerImage, ok := strings.Cut(args[0], "://")
				if !ok {
					return nil, fmt.Errorf("invalid connection string %v", args[0])
				}
				pkRaw := c.StringSlice("primary-key")
				var pkFinal [][]string
				for _, pk := range pkRaw {
					pkFinal = append(pkFinal, strings.Split(pk, "."))
				}
				return airbyte.NewSource(dockerImage, c.String("config"), c.String("save-catalog"), c.String("sync-mode"), c.String("destination-sync-mode"), c.Int("sync-id"), c.Int("generation-id"), c.Int("minimum-generation-id"), c.StringSlice("cursor-field"), pkFinal), nil
			}),
		},
		{
			Name: "AirbyteSink",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(s, "airbyte-sink://")
			},
			Create: CreateHelper("AirbyteSink", "airbyte-sink://docker-image", []cli.Flag{
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:     "config",
					Required: true,
				}),
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:     "catalog",
					Required: true,
				}),
			}, func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				_, dockerImage, ok := strings.Cut(args[0], "://")
				if !ok {
					return nil, fmt.Errorf("invalid connection string %v", args[0])
				}
				return airbyte.NewSink(dockerImage, c.String("config"), c.String("catalog")), nil
			}),
		},
		{
			Name: "grpc",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(s, "grpc://")
			},
			CreateRemote: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceClient, []string, error) {
				var conn adiomv1connect.ConnectorServiceClient
				var restArgs []string
				app := &cli.App{
					HelpName:  "grpc",
					Usage:     "Connector",
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
						options := []connect.ClientOption{connect.WithGRPC(), connect.WithSendMaxBytes(100000000), connect.WithReadMaxBytes(100000000)}
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
						conn = adiomv1connect.NewConnectorServiceClient(httpClient, finalEndpoint, options...)
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
			},
		},
	}
}

func CosmosFlags(settings *cosmos.ConnectorSettings) []cli.Flag {
	return append(MongoFlags(&settings.ConnectorSettings), []cli.Flag{
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:        "cosmos-reader-max-namespaces",
			Usage:       "maximum number of namespaces that can be copied from the CosmosDB connector. Recommended to keep this number under 15 to avoid performance issues.",
			Value:       cosmosDefaultMaxNumNamespaces,
			Required:    false,
			Destination: &settings.MaxNumNamespaces,
			Category:    "Cosmos DB-specific Options",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "cosmos-deletes-cdc",
			Usage:    "witness connection string to generate CDC events for CosmosDB deletes",
			Category: "Cosmos DB-specific Options",
			Action: func(_ *cli.Context, v string) error {
				settings.EmulateDeletes = true
				settings.WitnessMongoConnString = v
				return nil
			},
		}),
		altsrc.NewDurationFlag(&cli.DurationFlag{
			Name:        "cosmos-delete-interval",
			Required:    false,
			Destination: &settings.DeletesCheckInterval,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:        "cosmos-parallel-partition-workers",
			Required:    false,
			Destination: &settings.NumParallelPartitionWorkers,
		}),
	}...)
}

func MongoFlags(settings *mongo.ConnectorSettings) []cli.Flag {
	return []cli.Flag{
		altsrc.NewDurationFlag(&cli.DurationFlag{
			Name:        "server-timeout",
			Required:    false,
			Destination: &settings.ServerConnectTimeout,
		}),
		altsrc.NewDurationFlag(&cli.DurationFlag{
			Name:        "ping-timeout",
			Required:    false,
			Destination: &settings.PingTimeout,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:        "writer-batch-size",
			Required:    false,
			Destination: &settings.WriterMaxBatchSize,
		}),
		altsrc.NewInt64Flag(&cli.Int64Flag{
			Name:        "doc-partition",
			Required:    false,
			Destination: &settings.TargetDocCountPerPartition,
			Value:       50 * 1000,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "initial-sync-query",
			Usage:       "query filter for the initial data copy (v2 Extended JSON)",
			Required:    false,
			Destination: &settings.Query,
		}),
	}
}

func insecureClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
}
