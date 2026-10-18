package cliconfig

import (
	"time"

	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
	xraybackend "github.com/blackbirdworks/gopherstack/services/xray"
)

// PersistableConfig holds settings that can be saved to disk.
type PersistableConfig struct {
	ElasticsearchEngine string                    `json:"elasticsearch_engine"`
	ElastiCacheEngine   string                    `json:"elasticache_engine"`
	Region              string                    `json:"region"`
	LogLevel            string                    `json:"log_level"`
	Port                string                    `json:"port"`
	OpenSearchEngine    string                    `json:"opensearch_engine"`
	AccountID           string                    `json:"account_id"`
	DataDir             string                    `json:"data_dir"`
	DNSListenAddr       string                    `json:"dns_listen_addr"`
	DNSResolveIP        string                    `json:"dns_resolve_ip"`
	S3                  s3backend.Settings        `json:"s3"`
	Lambda              lambdabackend.Settings    `json:"lambda"`
	DynamoDB            ddbbackend.Settings       `json:"dynamodb"`
	Batch               batchbackend.Settings     `json:"batch"`
	EC2                 ec2backend.Settings       `json:"ec2"`
	CodeBuild           codebuildbackend.Settings `json:"codebuild"`
	FIS                 fisbackend.Settings       `json:"fis"`
	Athena              athenabackend.Settings    `json:"athena"`
	EMR                 emrbackend.Settings       `json:"emr"`
	SES                 sesbackend.Settings       `json:"ses"`
	SSM                 ssmbackend.Settings       `json:"ssm"`
	XRay                xraybackend.Settings      `json:"xray"`
	Backup              backupbackend.Settings    `json:"backup"`
	PortRangeEnd        int                       `json:"port_range_end"`
	InitScriptTimeout   time.Duration             `json:"init_script_timeout"`
	LatencyMs           int                       `json:"latency_ms"`
	PortRangeStart      int                       `json:"port_range_start"`
	JanitorTimeout      time.Duration             `json:"janitor_timeout"`
	CloudWatchLogs      cwlogsbackend.Settings    `json:"cloudwatchlogs"`
	STS                 stsbackend.Settings       `json:"sts"`
	StepFunctions       sfnbackend.Settings       `json:"stepfunctions"`
	AutoPurgeTTL        time.Duration             `json:"auto_purge_ttl"`
	Kinesis             kinesisbackend.Settings   `json:"kinesis"`
	KMS                 kmsbackend.Settings       `json:"kms"`
	EnforceIAM          bool                      `json:"enforce_iam"`
	Demo                bool                      `json:"demo"`
	Persist             bool                      `json:"persist"`
}
