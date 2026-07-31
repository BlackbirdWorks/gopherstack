module github.com/blackbirdworks/gopherstack

go 1.26.5

require (
	github.com/alecthomas/kong v1.16.0
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/aws/aws-sdk-go-v2 v1.43.2
	github.com/aws/aws-sdk-go-v2/config v1.32.31
	github.com/aws/aws-sdk-go-v2/credentials v1.19.30
	github.com/aws/aws-sdk-go-v2/service/acm v1.43.0
	github.com/aws/aws-sdk-go-v2/service/acmpca v1.49.0
	github.com/aws/aws-sdk-go-v2/service/amplify v1.41.0
	github.com/aws/aws-sdk-go-v2/service/apigateway v1.42.0
	github.com/aws/aws-sdk-go-v2/service/apigatewayv2 v1.37.0
	github.com/aws/aws-sdk-go-v2/service/appconfig v1.48.0
	github.com/aws/aws-sdk-go-v2/service/appconfigdata v1.26.0
	github.com/aws/aws-sdk-go-v2/service/applicationautoscaling v1.45.0
	github.com/aws/aws-sdk-go-v2/service/appsync v1.56.0
	github.com/aws/aws-sdk-go-v2/service/athena v1.60.0
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.70.0
	github.com/aws/aws-sdk-go-v2/service/backup v1.59.0
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.75.0
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.65.0
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.80.0
	github.com/aws/aws-sdk-go-v2/service/cognitoidentity v1.36.0
	github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider v1.67.0
	github.com/aws/aws-sdk-go-v2/service/configservice v1.68.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.62.0
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.36.0
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.317.0
	github.com/aws/aws-sdk-go-v2/service/ecr v1.60.0
	github.com/aws/aws-sdk-go-v2/service/ecs v1.89.0
	github.com/aws/aws-sdk-go-v2/service/efs v1.44.0
	github.com/aws/aws-sdk-go-v2/service/elasticache v1.56.0
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.48.0
	github.com/aws/aws-sdk-go-v2/service/firehose v1.46.0
	github.com/aws/aws-sdk-go-v2/service/iam v1.56.0
	github.com/aws/aws-sdk-go-v2/service/iot v1.77.0
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.46.0
	github.com/aws/aws-sdk-go-v2/service/kms v1.55.0
	github.com/aws/aws-sdk-go-v2/service/lambda v1.100.0
	github.com/aws/aws-sdk-go-v2/service/opensearch v1.75.0
	github.com/aws/aws-sdk-go-v2/service/rds v1.123.0
	github.com/aws/aws-sdk-go-v2/service/redshift v1.65.0
	github.com/aws/aws-sdk-go-v2/service/resourcegroups v1.36.0
	github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi v1.35.0
	github.com/aws/aws-sdk-go-v2/service/route53 v1.65.2
	github.com/aws/aws-sdk-go-v2/service/route53resolver v1.48.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.106.0
	github.com/aws/aws-sdk-go-v2/service/s3control v1.73.0
	github.com/aws/aws-sdk-go-v2/service/scheduler v1.20.0
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.44.0
	github.com/aws/aws-sdk-go-v2/service/ses v1.37.0
	github.com/aws/aws-sdk-go-v2/service/sfn v1.45.0
	github.com/aws/aws-sdk-go-v2/service/sns v1.42.0
	github.com/aws/aws-sdk-go-v2/service/sqs v1.46.0
	github.com/aws/aws-sdk-go-v2/service/ssm v1.73.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.45.0
	github.com/aws/aws-sdk-go-v2/service/support v1.34.0
	github.com/aws/aws-sdk-go-v2/service/swf v1.37.0
	github.com/aws/smithy-go v1.27.5
	github.com/distribution/distribution/v3 v3.1.1
	github.com/docker/go-connections v0.7.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/labstack/echo/v5 v5.3.1
	github.com/miekg/dns v1.1.72
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/client_model v0.6.2
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go v0.43.0
	github.com/vektah/gqlparser/v2 v2.5.36
	golang.org/x/crypto v0.54.0
	gopkg.in/yaml.v3 v3.0.1
)

require github.com/aws/aws-sdk-go-v2/service/batch v1.68.0

require github.com/aws/aws-sdk-go-v2/service/bedrock v1.66.0

require github.com/aws/aws-sdk-go-v2/service/bedrockruntime v1.56.0

require (
	github.com/aws/aws-sdk-go-v2/service/cloudcontrol v1.32.0
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.67.0
	github.com/aws/aws-sdk-go-v2/service/cloudtrail v1.58.0
	github.com/aws/aws-sdk-go-v2/service/codeartifact v1.41.0
	github.com/aws/aws-sdk-go-v2/service/codebuild v1.72.0
	github.com/aws/aws-sdk-go-v2/service/codecommit v1.36.0
	github.com/aws/aws-sdk-go-v2/service/codeconnections v1.13.0
	github.com/aws/aws-sdk-go-v2/service/codedeploy v1.38.0
	github.com/aws/aws-sdk-go-v2/service/codepipeline v1.49.0
	github.com/aws/aws-sdk-go-v2/service/codestarconnections v1.38.0
	github.com/aws/aws-sdk-go-v2/service/costexplorer v1.67.0
	github.com/aws/aws-sdk-go-v2/service/databasemigrationservice v1.66.0
	github.com/aws/aws-sdk-go-v2/service/docdb v1.51.0
	github.com/aws/aws-sdk-go-v2/service/eks v1.90.0
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing v1.36.0
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2 v1.58.1
	github.com/aws/aws-sdk-go-v2/service/emrserverless v1.44.0
	github.com/aws/aws-sdk-go-v2/service/glue v1.149.0
	github.com/aws/aws-sdk-go-v2/service/kafka v1.56.0
	github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2 v1.41.0
)

require (
	github.com/aws/aws-sdk-go-v2/service/glacier v1.35.0
	github.com/aws/aws-sdk-go-v2/service/identitystore v1.39.0
	github.com/aws/aws-sdk-go-v2/service/iotanalytics v1.32.0
	github.com/aws/aws-sdk-go-v2/service/iotwireless v1.59.0
	github.com/aws/aws-sdk-go-v2/service/kinesisanalytics v1.33.0
	github.com/aws/aws-sdk-go-v2/service/lakeformation v1.50.0
)

require github.com/aws/aws-sdk-go-v2/service/managedblockchain v1.34.0

require github.com/aws/aws-sdk-go-v2/service/mediaconvert v1.96.0

require (
	github.com/aws/aws-sdk-go-v2/service/mediastore v1.32.0
	github.com/aws/aws-sdk-go-v2/service/mediastoredata v1.32.0
)

require (
	connectrpc.com/connect v1.20.0
	github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi v1.32.0
	github.com/aws/aws-sdk-go-v2/service/elasticsearchservice v1.45.0
	github.com/aws/aws-sdk-go-v2/service/iotdataplane v1.35.0
	github.com/aws/aws-sdk-go-v2/service/memorydb v1.36.0
	github.com/aws/aws-sdk-go-v2/service/mq v1.39.0
	github.com/aws/aws-sdk-go-v2/service/mwaa v1.43.0
	github.com/aws/aws-sdk-go-v2/service/neptune v1.48.0
	github.com/aws/aws-sdk-go-v2/service/organizations v1.53.0
	github.com/aws/aws-sdk-go-v2/service/pinpoint v1.42.0
	github.com/aws/aws-sdk-go-v2/service/pipes v1.26.0
	github.com/aws/aws-sdk-go-v2/service/ram v1.39.0
	github.com/aws/aws-sdk-go-v2/service/rdsdata v1.35.0
	github.com/aws/aws-sdk-go-v2/service/redshiftdata v1.43.0
	github.com/aws/aws-sdk-go-v2/service/s3tables v1.18.0
	github.com/aws/aws-sdk-go-v2/service/sagemaker v1.261.0
	github.com/aws/aws-sdk-go-v2/service/sagemakerruntime v1.43.0
	github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository v1.33.0
	github.com/aws/aws-sdk-go-v2/service/servicediscovery v1.43.0
	github.com/aws/aws-sdk-go-v2/service/sesv2 v1.66.0
	github.com/aws/aws-sdk-go-v2/service/shield v1.37.0
	github.com/aws/aws-sdk-go-v2/service/ssoadmin v1.42.0
	github.com/aws/aws-sdk-go-v2/service/textract v1.43.0
	github.com/aws/aws-sdk-go-v2/service/timestreamquery v1.39.0
	github.com/aws/aws-sdk-go-v2/service/timestreamwrite v1.38.0
	github.com/aws/aws-sdk-go-v2/service/transcribe v1.58.0
	github.com/aws/aws-sdk-go-v2/service/transfer v1.75.0
	github.com/aws/aws-sdk-go-v2/service/verifiedpermissions v1.36.0
	github.com/aws/aws-sdk-go-v2/service/wafv2 v1.76.0
	github.com/aws/aws-sdk-go-v2/service/xray v1.39.0
	github.com/moby/moby/api v1.55.0
	github.com/moby/moby/client v0.5.0
)

require github.com/aws/aws-sdk-go-v2/service/bedrockagent v1.58.0

require (
	github.com/aws/aws-sdk-go-v2/service/accessanalyzer v1.51.0
	github.com/aws/aws-sdk-go-v2/service/appmesh v1.38.0
	github.com/aws/aws-sdk-go-v2/service/apprunner v1.42.0
	github.com/aws/aws-sdk-go-v2/service/comprehend v1.43.0
	github.com/aws/aws-sdk-go-v2/service/databrew v1.42.0
	github.com/aws/aws-sdk-go-v2/service/datasync v1.61.0
	github.com/aws/aws-sdk-go-v2/service/dax v1.32.0
	github.com/aws/aws-sdk-go-v2/service/detective v1.41.0
	github.com/aws/aws-sdk-go-v2/service/directoryservice v1.41.0
	github.com/aws/aws-sdk-go-v2/service/dlm v1.39.0
	github.com/aws/aws-sdk-go-v2/service/forecast v1.44.0
	github.com/aws/aws-sdk-go-v2/service/fsx v1.68.0
	github.com/aws/aws-sdk-go-v2/service/guardduty v1.85.0
	github.com/aws/aws-sdk-go-v2/service/inspector2 v1.53.0
	github.com/aws/aws-sdk-go-v2/service/macie2 v1.54.0
	github.com/aws/aws-sdk-go-v2/service/medialive v1.101.0
	github.com/aws/aws-sdk-go-v2/service/mediapackage v1.42.0
	github.com/aws/aws-sdk-go-v2/service/mediatailor v1.63.0
	github.com/aws/aws-sdk-go-v2/service/personalize v1.50.0
	github.com/aws/aws-sdk-go-v2/service/polly v1.60.0
	github.com/aws/aws-sdk-go-v2/service/quicksight v1.121.0
	github.com/aws/aws-sdk-go-v2/service/rekognition v1.54.0
	github.com/aws/aws-sdk-go-v2/service/rolesanywhere v1.25.1
	github.com/aws/aws-sdk-go-v2/service/securityhub v1.75.0
	github.com/aws/aws-sdk-go-v2/service/translate v1.36.0
	github.com/aws/aws-sdk-go-v2/service/waf v1.33.0
	github.com/aws/aws-sdk-go-v2/service/workmail v1.39.0
	github.com/aws/aws-sdk-go-v2/service/workspaces v1.72.0
)

require (
	github.com/aws/aws-sdk-go v1.55.8
	github.com/aws/aws-sdk-go-v2/service/appstream v1.64.0
)

require github.com/aws/aws-sdk-go-v2/service/vpclattice v1.25.0

require github.com/aws/aws-sdk-go-v2/service/networkmonitor v1.16.0

require github.com/aws/aws-sdk-go-v2/service/omics v1.49.1

require github.com/aws/aws-sdk-go-v2/service/cleanrooms v1.48.0

require (
	github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore v1.15.2
	github.com/aws/aws-sdk-go-v2/service/opensearchserverless v1.34.2
	github.com/aws/aws-sdk-go-v2/service/personalizeruntime v1.36.2
	github.com/aws/aws-sdk-go-v2/service/schemas v1.37.2
	github.com/mxschmitt/playwright-go v0.6100.0
	go.uber.org/goleak v1.3.0
	modernc.org/sqlite v1.54.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	modernc.org/libc v1.74.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

require (
	github.com/antlr/antlr4 v0.0.0-20181218183524-be58ebffde8e // indirect
	github.com/aws/aws-dax-go v1.2.15
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
)

require (
	github.com/cedar-policy/cedar-go v1.8.0
	golang.org/x/exp v0.0.0-20260718201538-764159d718ef // indirect
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.14
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk v1.37.0
	github.com/aws/aws-sdk-go-v2/service/emr v1.64.0
	github.com/aws/aws-sdk-go-v2/service/fis v1.40.0
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.12.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.31 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.32 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.33.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.38.0 // indirect
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
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/deckarep/golang-set/v2 v2.8.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/docker/docker-credential-helpers v0.9.8 // indirect
	github.com/docker/go-events v0.0.0-20260713150650-1ee7122bc07b // indirect
	github.com/docker/go-metrics v0.0.1 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/ebitengine/purego v0.10.2 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-jose/go-jose/v3 v3.0.5 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/gorilla/handlers v1.5.2 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/gorilla/websocket v1.5.3
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20260627054121-477a66015f15 // indirect
	github.com/magiconair/properties v1.18.11 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.2.1 // indirect
	github.com/moby/patternmatcher v0.6.1 // indirect
	github.com/moby/sys/sequential v0.7.0 // indirect
	github.com/moby/sys/user v0.4.1 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.21.0 // indirect
	github.com/redis/go-redis/extra/redisotel/v9 v9.21.0 // indirect
	github.com/redis/go-redis/v9 v9.21.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/shirou/gopsutil/v4 v4.26.6 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/bridges/prometheus v0.69.0 // indirect
	go.opentelemetry.io/contrib/exporters/autoexport v0.69.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.66.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.44.0 // indirect
	go.opentelemetry.io/otel/log v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/telemetry v0.0.0-20260708182218-49f421fb7959 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	golang.org/x/vuln v1.1.4 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260724162435-b2f20204f0df // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260724162435-b2f20204f0df // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gotest.tools/gotestsum v1.13.0 // indirect
)

tool (
	golang.org/x/vuln/cmd/govulncheck
	gotest.tools/gotestsum
)
