module github.com/blackbirdworks/gopherstack

go 1.26.1

require (
	github.com/alecthomas/kong v1.14.0
	github.com/alicebob/miniredis/v2 v2.37.0
	github.com/aws/aws-sdk-go-v2 v1.41.3
	github.com/aws/aws-sdk-go-v2/config v1.32.11
	github.com/aws/aws-sdk-go-v2/credentials v1.19.11
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.34
	github.com/aws/aws-sdk-go-v2/service/acm v1.37.21
	github.com/aws/aws-sdk-go-v2/service/acmpca v1.46.10
	github.com/aws/aws-sdk-go-v2/service/amplify v1.38.12
	github.com/aws/aws-sdk-go-v2/service/apigateway v1.38.6
	github.com/aws/aws-sdk-go-v2/service/apigatewayv2 v1.33.7
	github.com/aws/aws-sdk-go-v2/service/appconfig v1.43.11
	github.com/aws/aws-sdk-go-v2/service/appconfigdata v1.23.20
	github.com/aws/aws-sdk-go-v2/service/applicationautoscaling v1.41.12
	github.com/aws/aws-sdk-go-v2/service/appsync v1.53.3
	github.com/aws/aws-sdk-go-v2/service/athena v1.57.2
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.64.2
	github.com/aws/aws-sdk-go-v2/service/backup v1.54.8
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.71.7
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.55.1
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.64.0
	github.com/aws/aws-sdk-go-v2/service/cognitoidentity v1.33.20
	github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider v1.59.1
	github.com/aws/aws-sdk-go-v2/service/configservice v1.61.2
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.56.1
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.12
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.294.0
	github.com/aws/aws-sdk-go-v2/service/ecr v1.55.4
	github.com/aws/aws-sdk-go-v2/service/ecs v1.73.1
	github.com/aws/aws-sdk-go-v2/service/efs v1.41.12
	github.com/aws/aws-sdk-go-v2/service/elasticache v1.51.11
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.45.21
	github.com/aws/aws-sdk-go-v2/service/firehose v1.42.11
	github.com/aws/aws-sdk-go-v2/service/iam v1.53.4
	github.com/aws/aws-sdk-go-v2/service/iot v1.72.3
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.43.2
	github.com/aws/aws-sdk-go-v2/service/kms v1.50.2
	github.com/aws/aws-sdk-go-v2/service/lambda v1.88.2
	github.com/aws/aws-sdk-go-v2/service/opensearch v1.59.0
	github.com/aws/aws-sdk-go-v2/service/rds v1.116.2
	github.com/aws/aws-sdk-go-v2/service/redshift v1.62.3
	github.com/aws/aws-sdk-go-v2/service/resourcegroups v1.33.22
	github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi v1.31.8
	github.com/aws/aws-sdk-go-v2/service/route53 v1.62.3
	github.com/aws/aws-sdk-go-v2/service/route53resolver v1.42.3
	github.com/aws/aws-sdk-go-v2/service/s3 v1.96.4
	github.com/aws/aws-sdk-go-v2/service/s3control v1.68.2
	github.com/aws/aws-sdk-go-v2/service/scheduler v1.17.20
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.41.3
	github.com/aws/aws-sdk-go-v2/service/ses v1.34.20
	github.com/aws/aws-sdk-go-v2/service/sfn v1.40.8
	github.com/aws/aws-sdk-go-v2/service/sns v1.39.13
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.23
	github.com/aws/aws-sdk-go-v2/service/ssm v1.68.2
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.8
	github.com/aws/aws-sdk-go-v2/service/support v1.31.19
	github.com/aws/aws-sdk-go-v2/service/swf v1.33.14
	github.com/aws/smithy-go v1.24.2
	github.com/distribution/distribution/v3 v3.0.0
	github.com/docker/docker v28.5.2+incompatible
	github.com/docker/go-connections v0.6.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/labstack/echo/v5 v5.0.4
	github.com/miekg/dns v1.1.72
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/playwright-community/playwright-go v0.5700.1
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go v0.40.0
	github.com/vektah/gqlparser/v2 v2.5.32
	golang.org/x/crypto v0.48.0
	gopkg.in/yaml.v3 v3.0.1
)

require github.com/aws/aws-sdk-go-v2/service/batch v1.61.1

require github.com/aws/aws-sdk-go-v2/service/bedrock v1.56.0

require github.com/aws/aws-sdk-go-v2/service/bedrockruntime v1.50.1

require (
	github.com/aws/aws-sdk-go-v2/service/cloudcontrol v1.29.11
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.60.2
	github.com/aws/aws-sdk-go-v2/service/cloudtrail v1.55.7
	github.com/aws/aws-sdk-go-v2/service/codeartifact v1.38.19
	github.com/aws/aws-sdk-go-v2/service/codebuild v1.68.11
	github.com/aws/aws-sdk-go-v2/service/codecommit v1.33.10
	github.com/aws/aws-sdk-go-v2/service/codeconnections v1.10.18
	github.com/aws/aws-sdk-go-v2/service/codedeploy v1.35.11
	github.com/aws/aws-sdk-go-v2/service/codepipeline v1.46.19
	github.com/aws/aws-sdk-go-v2/service/codestarconnections v1.35.11
	github.com/aws/aws-sdk-go-v2/service/costexplorer v1.63.4
	github.com/aws/aws-sdk-go-v2/service/databasemigrationservice v1.61.8
	github.com/aws/aws-sdk-go-v2/service/docdb v1.48.11
	github.com/aws/aws-sdk-go-v2/service/eks v1.80.2
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing v1.33.21
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2 v1.54.8
	github.com/aws/aws-sdk-go-v2/service/elastictranscoder v1.33.0
	github.com/aws/aws-sdk-go-v2/service/emrserverless v1.39.4
	github.com/aws/aws-sdk-go-v2/service/glue v1.137.2
	github.com/aws/aws-sdk-go-v2/service/kafka v1.49.0
	github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2 v1.36.22
)

require (
	github.com/aws/aws-sdk-go-v2/service/glacier v1.32.4
	github.com/aws/aws-sdk-go-v2/service/identitystore v1.36.3
	github.com/aws/aws-sdk-go-v2/service/iotanalytics v1.32.0
	github.com/aws/aws-sdk-go-v2/service/iotwireless v1.54.7
	github.com/aws/aws-sdk-go-v2/service/kinesisanalytics v1.30.21
	github.com/aws/aws-sdk-go-v2/service/lakeformation v1.47.3
)

require github.com/aws/aws-sdk-go-v2/service/managedblockchain v1.31.19

require github.com/aws/aws-sdk-go-v2/service/mediaconvert v1.87.3

require (
	github.com/aws/aws-sdk-go-v2/service/mediastore v1.29.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/memorydb v1.33.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/mq v1.34.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/mwaa v1.39.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/neptune v1.44.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/organizations v1.50.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/pinpoint v1.39.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/pipes v1.23.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/qldb v1.32.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/qldbsession v1.32.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/ram v1.36.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/rdsdata v1.32.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/redshiftdata v1.38.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sagemaker v1.236.0 // indirect
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk v1.34.0
	github.com/aws/aws-sdk-go-v2/service/emr v1.57.7
	github.com/aws/aws-sdk-go-v2/service/fis v1.37.18
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.16 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitfield/gotestdox v0.2.2 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/deckarep/golang-set/v2 v2.8.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/docker/docker-credential-helpers v0.9.5 // indirect
	github.com/docker/go-events v0.0.0-20250808211157-605354379745 // indirect
	github.com/docker/go-metrics v0.0.1 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/ebitengine/purego v0.10.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-jose/go-jose/v3 v3.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/gorilla/handlers v1.5.2 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/lufia/plan9stats v0.0.0-20260216142805-b3301c5f2a88 // indirect
	github.com/magiconair/properties v1.8.10 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.2.0 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/morikuni/aec v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.18.0 // indirect
	github.com/redis/go-redis/extra/redisotel/v9 v9.18.0 // indirect
	github.com/redis/go-redis/v9 v9.18.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/shirou/gopsutil/v4 v4.26.2 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/bridges/prometheus v0.67.0 // indirect
	go.opentelemetry.io/contrib/exporters/autoexport v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.18.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.18.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.64.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.18.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.42.0 // indirect
	go.opentelemetry.io/otel/log v0.18.0 // indirect
	go.opentelemetry.io/otel/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.18.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/trace v1.42.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/telemetry v0.0.0-20260209163413-e7419c687ee4 // indirect
	golang.org/x/term v0.40.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	golang.org/x/tools v0.42.0 // indirect
	golang.org/x/tools/go/packages/packagestest v0.1.1-deprecated // indirect
	golang.org/x/vuln v1.1.4 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.79.2 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gotest.tools/gotestsum v1.13.0 // indirect
)

tool (
	golang.org/x/vuln/cmd/govulncheck
	gotest.tools/gotestsum
)
