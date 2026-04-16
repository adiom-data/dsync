package options

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/adiom-data/dsync/connectors/airbyte"
	"github.com/adiom-data/dsync/connectors/cosmos"
	"github.com/adiom-data/dsync/connectors/dynamodb"
	fileconnector "github.com/adiom-data/dsync/connectors/file"
	"github.com/adiom-data/dsync/connectors/kafka"
	"github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/connectors/null"
	"github.com/adiom-data/dsync/connectors/postgres"
	"github.com/adiom-data/dsync/connectors/random"
	s3connector "github.com/adiom-data/dsync/connectors/s3"
	"github.com/adiom-data/dsync/connectors/s3vector"
	"github.com/adiom-data/dsync/connectors/sqlbatch"
	"github.com/adiom-data/dsync/connectors/testconn"
	"github.com/adiom-data/dsync/connectors/vector"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	connectorsv1 "github.com/adiom-data/dsync/gen/adiom/commands/connectors/v1"
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
			Create: CreateHelper("/dev/random", "/dev/random", connectorsv1.DevRandomFlagsCommandFlags(), func(_ *cli.Context, _ []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				return random.NewConn(random.ConnectorSettings{}), nil
			}),
		},
		{
			Name: "/dev/fakesource",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "/dev/fakesource")
			},
			Create: CreateHelper("/dev/fakesource", "/dev/fakesource", connectorsv1.FakeSourceFlagsCommandFlags(), func(c *cli.Context, _ []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseFakeSourceFlags(c)
				if err != nil {
					return nil, err
				}
				return fakeSourceFromFlags(flags)
			}),
		},
		{
			Name: "/dev/null",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "/dev/null")
			},
			Create: CreateHelper("/dev/null", "/dev/null", connectorsv1.DevNullFlagsCommandFlags(), func(c *cli.Context, _ []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseDevNullFlags(c)
				if err != nil {
					return nil, err
				}
				return null.NewConn(flags.Id, flags.LogJson, flags.Sleep.AsDuration(), flags.SleepJitter.AsDuration()), nil
			}),
		},
		{
			Name: "S3",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(strings.ToLower(s), "s3://")
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				if len(args) == 0 {
					return nil, nil, fmt.Errorf("missing s3 connection string: %w", ErrMissingConnector)
				}
				return CreateHelper("s3", "s3://bucket[/prefix] [options]", connectorsv1.S3FlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					flags, err := connectorsv1.ParseS3Flags(c)
					if err != nil {
						return nil, err
					}
					return s3connector.NewConn(s3SettingsFromFlags(flags, args[0]))
				})(args, as)
			},
		},
		{
			Name: "File",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(strings.ToLower(s), "file://")
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				if len(args) == 0 {
					return nil, nil, fmt.Errorf("missing file connection string: %w", ErrMissingConnector)
				}
				return CreateHelper("file", "file:///path/to/dir OR file:///path/to/file.csv [options]", connectorsv1.FileFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					flags, err := connectorsv1.ParseFileFlags(c)
					if err != nil {
						return nil, err
					}
					settings, err := fileSettingsFromFlags(flags, args[0])
					if err != nil {
						return nil, err
					}
					return fileconnector.NewConn(settings)
				})(args, as)
			},
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
			Name: "sqlbatch",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "sqlbatch")
			},
			Create: CreateHelper("sqlbatch", "sqlbatch", connectorsv1.SqlbatchFlagsCommandFlags(), func(c *cli.Context, _ []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseSqlbatchFlags(c)
				if err != nil {
					return nil, err
				}
				return sqlbatch.NewConn(flags.Config)
			}),
		},
		{
			Name: "DynamoDB",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "dynamodb") || strings.EqualFold(s, "dynamodb://localstack")
			},
			Create: CreateHelper("DynamoDB", "dynamodb OR dynamodb://localstack", connectorsv1.DynamoDBFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseDynamoDBFlags(c)
				if err != nil {
					return nil, err
				}
				connString := ""
				if strings.EqualFold(args[0], "dynamodb://localstack") {
					_, cs, ok := strings.Cut(args[0], "://")
					if !ok {
						return nil, fmt.Errorf("invalid connection string %v", args[0])
					}
					connString = cs
				}
				return dynamodb.NewConn(connString,
					dynamodb.WithDocsPerSegment(int(flags.DocPartition)),
					dynamodb.WithPlanParallelism(int(flags.PlanParallelism)),
					dynamodb.WithID(flags.Id),
				), nil
			}),
		},
		{
			Name: "s3vectors",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "s3vector") || strings.EqualFold(s, "s3vectors")
			},
			Create: CreateHelper("s3vectors", "s3vector or s3vectors", connectorsv1.S3VectorsFlagsCommandFlags(), func(c *cli.Context, _ []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseS3VectorsFlags(c)
				if err != nil {
					return nil, err
				}
				return s3vector.NewConn(flags.Bucket, flags.VectorKey, int(flags.MaxParallelism), int(flags.BatchSize), int(flags.RateLimit))
			}),
		},
		{
			Name: "CosmosDB",
			IsConnector: func(s string) bool {
				if strings.HasPrefix(s, "mongodb://") || strings.HasPrefix(s, "mongodb+srv://") {
					return mongo.GetMongoFlavor(s) == mongo.FlavorCosmosDB_RU
				}
				return false
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				return CreateHelper("CosmosDB", "mongodb://cosmos-connection-string [options]", connectorsv1.CosmosFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					flags, err := connectorsv1.ParseCosmosFlags(c)
					if err != nil {
						return nil, err
					}
					return cosmos.NewConn(cosmosSettingsFromFlags(flags, args[0], as)), nil
				})(args, as)
			},
		},
		{
			Name: "MongoDB",
			IsConnector: func(s string) bool {
				if strings.HasPrefix(s, "mongodb://") || strings.HasPrefix(s, "mongodb+srv://") {
					flavor := mongo.GetMongoFlavor(s)
					return flavor == mongo.FlavorMongoDB || flavor == mongo.FlavorCosmosDB_VCORE || flavor == mongo.FlavorDocumentDB
				}
				return false
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				return CreateHelper("MongoDB", "mongodb://connection-string [options]", connectorsv1.MongoFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					flags, err := connectorsv1.ParseMongoFlags(c)
					if err != nil {
						return nil, err
					}
					return mongo.NewConn(mongoSettingsFromFlags(flags, args[0]))
				})(args, as)
			},
		},
		{
			Name: "Postgres",
			IsConnector: func(s string) bool {
				return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
			},
			Create: func(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
				return CreateHelper("Postgres", postgresUsage, connectorsv1.PostgresFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
					flags, err := connectorsv1.ParsePostgresFlags(c)
					if err != nil {
						return nil, err
					}
					return postgres.NewConn(c.Context, postgresSettingsFromFlags(flags, args[0]))
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
			Name: "kafka-dst",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "kafka-dst")
			},
			Create: CreateHelper("kafka-dst", "kafka-dst", connectorsv1.KafkaDstFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseKafkaDstFlags(c)
				if err != nil {
					return nil, err
				}
				return kafkaDstFromFlags(flags)
			}),
		},
		{
			Name: "kafka-src",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "kafka-src")
			},
			Create: CreateHelper("kafka-src", "kafka-src", connectorsv1.KafkaSrcFlagsCommandFlags(), func(c *cli.Context, args []string, _ AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, error) {
				flags, err := connectorsv1.ParseKafkaSrcFlags(c)
				if err != nil {
					return nil, err
				}
				return kafkaSrcFromFlags(flags)
			}),
		},
		{
			Name: "kafka-wrap",
			IsConnector: func(s string) bool {
				return strings.EqualFold(s, "kafka-wrap")
			},
			Create: KafkaWrap,
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

func KafkaWrap(args []string, as AdditionalSettings) (adiomv1connect.ConnectorServiceHandler, []string, error) {
	var conn adiomv1connect.ConnectorServiceHandler
	var restArgs []string
	app := &cli.App{
		HelpName:  "kafka-wrap",
		Usage:     "Connector",
		UsageText: "kafka-wrap [options] [source-connector] [connector-options]",
		Flags:     connectorsv1.KafkaSrcFlagsCommandFlags(),
		Action: func(c *cli.Context) error {
			restArgs = c.Args().Slice()
			flags, err := connectorsv1.ParseKafkaSrcFlags(c)
			if err != nil {
				return err
			}
			conn, err = kafkaSrcFromFlags(flags)
			if err != nil {
				return err
			}
			registeredConnectors := GetRegisteredConnectors()
			if len(restArgs) < 1 {
				return fmt.Errorf("missing connector for kafka-wrap")
			}
			for _, c := range registeredConnectors {
				if c.IsConnector(restArgs[0]) {
					if c.Create != nil {
						underlying, newRestArgs, err := c.Create(restArgs, as)
						if err != nil {
							return fmt.Errorf("err creating wrapped connector: %w", err)
						}
						restArgs = newRestArgs
						conn = kafka.NewKafkaWrapConn(conn, underlying)
						return nil
					} else if c.CreateRemote != nil {
						underlying, newRestArgs, err := c.CreateRemote(restArgs, as)
						if err != nil {
							return fmt.Errorf("err creating wrapped connector: %w", err)
						}
						restArgs = newRestArgs
						conn = kafka.NewKafkaWrapConn(conn, underlying)
						return nil
					} else {
						return fmt.Errorf("unable to wrap connector")
					}
				}
			}

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

func kafkaSrcFromFlags(f *connectorsv1.KafkaSrcFlags) (adiomv1connect.ConnectorServiceHandler, error) {
	dataType := adiomv1.DataType(adiomv1.DataType_value[f.DataType])
	tm := map[string][]string{}
	for _, topic := range f.Topics {
		tm[topic] = nil
	}
	for _, m := range f.TopicMappings {
		topic, ns, ok := strings.Cut(m, ":")
		if !ok {
			return nil, fmt.Errorf("invalid topic mapping %v", m)
		}
		tm[topic] = append(tm[topic], ns)
	}
	return kafka.NewKafkaConn(f.Brokers, tm, kafka.DsyncMessageToUpdate, kafka.DsyncMessageToNamespace, f.SaslUser, f.SaslPassword, f.Offset, dataType), nil
}

func kafkaDstFromFlags(f *connectorsv1.KafkaDstFlags) (adiomv1connect.ConnectorServiceHandler, error) {
	dataType := adiomv1.DataType(adiomv1.DataType_value[f.DataType])
	tm := map[string]string{}
	for _, m := range f.NamespaceTopic {
		ns, topic, ok := strings.Cut(m, ":")
		if !ok {
			return nil, fmt.Errorf("invalid topic mapping %v", m)
		}
		tm[ns] = topic
	}
	return kafka.NewDestKafka(f.Brokers, f.DefaultTopic, tm, f.SaslUser, f.SaslPassword, dataType)
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

func fileSettingsFromFlags(f *connectorsv1.FileFlags, uri string) (fileconnector.ConnectorSettings, error) {
	if f.Delimiter != "" && len(f.Delimiter) != 1 {
		return fileconnector.ConnectorSettings{}, fileconnector.ErrInvalidDelimiter
	}
	s := fileconnector.ConnectorSettings{
		Uri:       uri,
		Format:    f.Format,
		BatchSize: int(f.BatchSize),
	}
	if len(f.Delimiter) == 1 {
		s.Delimiter = rune(f.Delimiter[0])
	}
	return s, nil
}

func s3SettingsFromFlags(f *connectorsv1.S3Flags, uri string) s3connector.ConnectorSettings {
	return s3connector.ConnectorSettings{
		Uri:            uri,
		PrettyJSON:     f.PrettyJson,
		Region:         f.Region,
		Prefix:         f.Prefix,
		OutputFormat:   f.OutputFormat,
		Profile:        f.Profile,
		Endpoint:       f.Endpoint,
		AccessKeyID:    f.AccessKeyId,
		SecretAccessKey: f.SecretAccessKey,
		SessionToken:   f.SessionToken,
		UsePathStyle:   f.UsePathStyle,
		MaxFileSizeMB:  f.MaxFileSize,
		MaxTotalMemoryMB: f.MaxTotalMemory,
	}
}

func mongoBaseSettingsFromFlags(f *connectorsv1.MongoBaseFlags, connString string) mongo.ConnectorSettings {
	s := mongo.ConnectorSettings{ConnectionString: connString}
	if f == nil {
		return s
	}
	s.SkipBatchOverwrite = f.SkipBatchOverwrite
	s.FullDocumentKey = f.FullDocumentKey
	s.ServerConnectTimeout = f.ServerTimeout.AsDuration()
	s.PingTimeout = f.PingTimeout.AsDuration()
	s.WriterMaxBatchSize = int(f.WriterBatchSize)
	s.TargetDocCountPerPartition = f.DocPartition
	s.MaxPageSize = int(f.MaxPageSize)
	s.Query = f.InitialSyncQuery
	return s
}

func mongoSettingsFromFlags(f *connectorsv1.MongoFlags, connString string) mongo.ConnectorSettings {
	s := mongoBaseSettingsFromFlags(f.Base, connString)
	s.SampleFactor = int(f.SampleFactor)
	s.PerNamespaceStreams = f.PerNamespaceStreams
	return s
}

func cosmosSettingsFromFlags(f *connectorsv1.CosmosFlags, connString string, as AdditionalSettings) cosmos.ConnectorSettings {
	s := cosmos.ConnectorSettings{
		ConnectorSettings:           mongoBaseSettingsFromFlags(f.Mongo, connString),
		MaxNumNamespaces:            int(f.CosmosReaderMaxNamespaces),
		NumParallelPartitionWorkers: int(f.CosmosParallelPartitionWorkers),
		WithDelete:                  f.CosmosStreamDeletesEnabled,
	}
	if as.BaseThreadCount != 0 && s.NumParallelPartitionWorkers == 0 {
		s.NumParallelPartitionWorkers = as.BaseThreadCount / 2
	}
	if f.CosmosDeleteInterval != nil {
		s.DeletesCheckInterval = f.CosmosDeleteInterval.AsDuration()
	}
	if f.CosmosDeletesCdc != "" {
		s.EmulateDeletes = true
		s.WitnessMongoConnString = f.CosmosDeletesCdc
	}
	return s
}

func fakeSourceFromFlags(f *connectorsv1.FakeSourceFlags) (adiomv1connect.ConnectorServiceHandler, error) {
	var m map[string]any
	if len(f.Payload) > 0 {
		m = map[string]any{}
	}
	for _, p := range f.Payload {
		k, v, _ := strings.Cut(p, ":")
		m[k] = v
	}
	if f.PayloadJson != "" {
		if err := json.Unmarshal([]byte(f.PayloadJson), &m); err != nil {
			return nil, fmt.Errorf("err unmarshalling payload-json: %w", err)
		}
	}
	return random.NewConnV2(random.ConnV2Input{
		NamespacePrefix:                 f.NamespacePrefix,
		NumNamespaces:                   int(f.NumNamespaces),
		NumPartitionsPerNamespace:       int(f.NumPartitionsPerNamespace),
		NumUpdatePartitionsPerNamespace: int(f.NumUpdatePartitionsPerNamespace),
		BatchSize:                       int(f.BatchSize),
		UpdateBatchSize:                 int(f.UpdateBatchSize),
		MaxUpdatesPerTick:               int(f.MaxUpdatesPerTick),
		NumDocsPerPartition:             f.NumDocsPerPartition,
		Payload:                         m,
		Sleep:                           f.Sleep.AsDuration(),
		Jitter:                          f.Jitter.AsDuration(),
		UpdateDuration:                  f.UpdateDuration.AsDuration(),
		StreamTick:                      f.StreamTick.AsDuration(),
	}), nil
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

func postgresSettingsFromFlags(f *connectorsv1.PostgresFlags, url string) postgres.PostgresSettings {
	s := postgres.PostgresSettings{
		URL:                        url,
		Force:                      !f.Manual,
		SlotName:                   f.ReplicationSlot,
		PublicationName:            f.PublicationName,
		Limit:                      int(f.PageSize),
		StreamMaxBatchSize:         int(f.StreamMaxBatchSize),
		EstimatedCountThreshold:    f.EstimatedCountThreshold,
		TargetDocCountPerPartition: f.DocPartition,
		EnableReplicaMode:          f.EnableReplicaMode,
	}
	if f.StreamMaxBatchWait != nil {
		s.StreamMaxBatchWait = f.StreamMaxBatchWait.AsDuration()
	}
	if f.StreamFlushDelay != nil {
		s.StreamFlushDelay = f.StreamFlushDelay.AsDuration()
	}
	return s
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
