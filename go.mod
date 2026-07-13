module github.com/adiom-data/dsync

go 1.26

require (
	connectrpc.com/connect v1.18.1
	connectrpc.com/grpcreflect v1.3.0
	github.com/IBM/sarama v1.46.3
	github.com/adiom-data/commandargs v0.0.0-20260415173909-157c91bb5601
	github.com/aws/aws-sdk-go-v2 v1.41.6
	github.com/aws/aws-sdk-go-v2/config v1.32.14
	github.com/aws/aws-sdk-go-v2/credentials v1.19.14
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.19.3
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.54.0
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.25.6
	github.com/aws/aws-sdk-go-v2/service/s3 v1.100.0
	github.com/aws/aws-sdk-go-v2/service/s3vectors v1.6.1
	github.com/aws/smithy-go v1.25.0
	github.com/benbjohnson/clock v1.3.5
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash v1.1.0
	github.com/gdamore/tcell/v2 v2.8.1
	github.com/jackc/pglogrepl v0.0.0-20250509230407-a9884f6bd75a
	github.com/jackc/pgx-shopspring-decimal v0.0.0-20220624020537-1d36b5a1853e
	github.com/jackc/pgx/v5 v5.9.2
	github.com/lmittmann/tint v1.1.2
	github.com/microsoft/go-mssqldb v1.9.2
	github.com/mitchellh/hashstructure v1.1.0
	github.com/pgvector/pgvector-go v0.3.0
	github.com/rivo/tview v0.0.0-20250625164341-a4a78f1e05cb
	github.com/samber/slog-multi v1.4.1
	github.com/shopspring/decimal v1.3.1
	github.com/sijms/go-ora/v2 v2.9.0
	github.com/stretchr/testify v1.11.1
	github.com/tryvium-travels/memongo v0.12.0
	github.com/urfave/cli/v2 v2.27.7
	github.com/weaviate/weaviate v1.37.1
	github.com/weaviate/weaviate-go-client/v5 v5.2.1
	go.akshayshah.org/memhttp v0.1.0
	go.mongodb.org/mongo-driver/v2 v2.5.0
	golang.org/x/exp v0.0.0-20251113190631-e25ba8c21ef6
	golang.org/x/net v0.52.0
	golang.org/x/time v0.14.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/acobaugh/osrelease v0.0.0-20181218015638-a93a0a55a249 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.9 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.10 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/gdamore/encoding v1.0.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/analysis v0.24.1 // indirect
	github.com/go-openapi/errors v0.22.4 // indirect
	github.com/go-openapi/jsonpointer v0.22.4 // indirect
	github.com/go-openapi/jsonreference v0.21.4 // indirect
	github.com/go-openapi/loads v0.23.2 // indirect
	github.com/go-openapi/runtime v0.29.2 // indirect
	github.com/go-openapi/spec v0.22.3 // indirect
	github.com/go-openapi/strfmt v0.25.0 // indirect
	github.com/go-openapi/swag v0.23.1 // indirect
	github.com/go-openapi/swag/conv v0.25.4 // indirect
	github.com/go-openapi/swag/fileutils v0.25.1 // indirect
	github.com/go-openapi/swag/jsonname v0.25.4 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.4 // indirect
	github.com/go-openapi/swag/loading v0.25.4 // indirect
	github.com/go-openapi/swag/mangling v0.25.1 // indirect
	github.com/go-openapi/swag/stringutils v0.25.4 // indirect
	github.com/go-openapi/swag/typeutils v0.25.4 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.4 // indirect
	github.com/go-openapi/validate v0.25.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/samber/lo v1.51.0 // indirect
	github.com/samber/slog-common v0.19.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	go.mongodb.org/mongo-driver v1.17.6 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/oauth2 v0.35.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/term v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260401024825-9d38bb4040a9 // indirect
)

require (
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/brianvoe/gofakeit/v7 v7.8.1
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/sync v0.20.0
	golang.org/x/text v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1
)
