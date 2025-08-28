package options

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/adiom-data/dsync/connectors/airbyte"
	"github.com/adiom-data/dsync/connectors/cosmos"
	"github.com/adiom-data/dsync/connectors/dynamodb"
	"github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/connectors/null"
	"github.com/adiom-data/dsync/connectors/postgres"
	"github.com/adiom-data/dsync/connectors/random"
	"github.com/adiom-data/dsync/connectors/testconn"
	"github.com/adiom-data/dsync/connectors/vector"
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

func CreateHelperWithRestArgs(name string, usage string, flags []cli.Flag, action func(*cli.Context, []string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error)) func([]string, AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
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
				conn, restArgs, err = action(c, args, as)
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
			Create: CreateHelper("/dev/null", "/dev/null", []cli.Flag{
				&cli.DurationFlag{
					Name:  "sleep",
					Usage: "Sleep time between requests",
				},
				&cli.DurationFlag{
					Name:  "sleep-jitter",
					Usage: "If sleep is set, additional jitter",
				},
				&cli.BoolFlag{
					Name:  "log-json",
					Usage: "Convert data to json and log INFO",
				},
			}, func(c *cli.Context, args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				return null.NewConn(c.Bool("log-json"), c.Duration("sleep"), c.Duration("sleep-jitter")), nil
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
					return mongo.NewConn(settings)
				})(args, as)
			},
		},
		{
			Name: "Postgres",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				settings := postgresSettingsDefault
				return CreateHelper("Postgres", postgresUsage, PostgresFlags(&settings), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					if c.Bool("manual") {
						settings.Force = false
					}
					settings.URL = args[0]
					return postgres.NewConn(c.Context, settings)
				})(args, as)
			},
		},
		{
			Name: "weaviate",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "weaviate")
			},
			Create: CreateHelperWithRestArgs("weaviate", "weaviate --url http://weaviate-host:port --has-chunker --has-embedder [grpc://chunker-host:port] [grpc://embedder-host:port]", WeaviateFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				url := c.String("url")
				groupID := c.String("group-id-field")
				apiKey := c.String("api-key")
				chunker := c.Bool("has-chunker")
				embedder := c.Bool("has-embedder")
				useIdentityMapper := c.Bool("use-identity-mapper")
				restArgs := c.Args().Slice()
				var chunkerClient adiomv1connect.ChunkingServiceClient = vector.NewSimple()
				var embedderClient adiomv1connect.EmbeddingServiceClient = vector.NewSimple()
				var err error
				if chunker {
					if len(restArgs) == 0 {
						return nil, nil, ErrMissingChunker
					}
					chunkerClient, restArgs, err = ConfigureChunker(restArgs)
					if err != nil {
						return nil, nil, err
					}
				}
				if embedder {
					if len(restArgs) == 0 {
						return nil, nil, ErrMissingEmbedder
					}
					embedderClient, restArgs, err = ConfigureEmbedder(restArgs)
					if err != nil {
						return nil, nil, err
					}
				}

				conn, err := vector.NewWeaviateConn(chunkerClient, embedderClient, url, groupID, apiKey, useIdentityMapper)
				if err != nil {
					return nil, nil, err
				}
				return conn, restArgs, err
			}),
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
				conn, restArgs, err := GRPCConnector(args)
				if err != nil {
					return nil, nil, err
				}
				return conn.(adiomv1connect.ConnectorServiceClient), restArgs, err
			},
		},
	}
}

func WeaviateFlags() []cli.Flag {
	return []cli.Flag{
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:     "url",
			Usage:    "With scheme e.g. http://localhost:8080",
			Required: true,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:  "group-id-field",
			Usage: "Chunks from the same document share the same group id",
			Value: "g_id",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name: "api-key",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "has-chunker",
			Usage: "Specifies that there will be grpc chunker specified (grpc://chunker-host:port). If embedder also specified, first arg is chunker.",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "has-embedder",
			Usage: "Specifies that there will be grpc embedder specified (grpc://embedder-host:port). If chunker also specified, last arg is embedder.",
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:   "use-identity-mapper",
			Hidden: true,
		}),
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
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:        "cosmos-stream-deletes-enabled",
			Usage:       "If this cosmos instance supports deletes in the change stream (currently a preview feature)",
			Destination: &settings.WithDelete,
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
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:        "max-page-size",
			Destination: &settings.MaxPageSize,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "initial-sync-query",
			Usage:       "query filter for the initial data copy (v2 Extended JSON)",
			Aliases:     []string{"q"},
			Required:    false,
			Destination: &settings.Query,
		}),
	}
}

var postgresUsage = `postgresql://user:pass@host:port [options]

Source:
   Tables must have a primary key and you must specify at least one namespace.

   Initial sync is split into tasks by Bernoulli sampling based on 1 / doc-partition.

   Change streams are powered by logical replication. Ensure that the postgres instance has been configured with 'wal_level=logical'. It should have the permissions to create replication slots and publications unless that is being managed externally via the --manual flag. Note that even though this connector may drop and create the replication slot and publication, you must still manually remove drop resources when you are done with dsync or postgres storage may grow. Also note that if you are running multiple instances for the same source, you will need to configure a different replication slot and/or publication for each one.

   Currently, TOAST fields are not properly supported. Set the replica identity to full as a workaround.

Destination:
   Not currently supported
`

var postgresSettingsDefault = postgres.PostgresSettings{
	Force:                      true,
	SlotName:                   "dsync_slot",
	PublicationName:            "dsync_pub",
	Limit:                      1000,
	StreamMaxBatchWait:         time.Second * 5,
	StreamMaxBatchSize:         100,
	StreamFlushDelay:           time.Minute * 3,
	EstimatedCountThreshold:    1000000,
	TargetDocCountPerPartition: 100000,
}

func PostgresFlags(settings *postgres.PostgresSettings) []cli.Flag {
	return []cli.Flag{
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:        "page-size",
			Usage:       "Specify pagination limit when fetching for initial sync",
			Value:       postgresSettingsDefault.Limit,
			Destination: &settings.Limit,
		}),
		altsrc.NewDurationFlag(&cli.DurationFlag{
			Name:        "stream-max-batch-wait",
			Usage:       "Force flush a stream batch after this interval",
			Value:       postgresSettingsDefault.StreamMaxBatchWait,
			Destination: &settings.StreamMaxBatchWait,
		}),
		altsrc.NewIntFlag(&cli.IntFlag{
			Name:        "stream-max-batch-size",
			Usage:       "Force flush a stream batch at this limit",
			Value:       postgresSettingsDefault.StreamMaxBatchSize,
			Destination: &settings.StreamMaxBatchSize,
		}),
		altsrc.NewDurationFlag(&cli.DurationFlag{
			Name:        "stream-flush-delay",
			Usage:       "Delay before notifying postgres backend of stream progress. This should be comfortably larger than saving streaming cursor updates.",
			Value:       postgresSettingsDefault.StreamFlushDelay,
			Destination: &settings.StreamFlushDelay,
		}),
		altsrc.NewBoolFlag(&cli.BoolFlag{
			Name:  "manual",
			Usage: "Use to not recreate replication slot and publication (e.g. if you are managing these outside). You will still need to clean up the slot later even if you don't use this.",
			Value: !postgresSettingsDefault.Force,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "replication-slot",
			Value:       postgresSettingsDefault.SlotName,
			Destination: &postgresSettingsDefault.SlotName,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "publication-name",
			Value:       postgresSettingsDefault.PublicationName,
			Destination: &postgresSettingsDefault.PublicationName,
		}),
		altsrc.NewInt64Flag(&cli.Int64Flag{
			Name:        "estimated-count-threshold",
			Usage:       "If estimated count is less than this, try a full count.",
			Value:       postgresSettingsDefault.EstimatedCountThreshold,
			Destination: &settings.EstimatedCountThreshold,
		}),
		altsrc.NewInt64Flag(&cli.Int64Flag{
			Name:        "doc-partition",
			Value:       postgresSettingsDefault.TargetDocCountPerPartition,
			Destination: &settings.TargetDocCountPerPartition,
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
