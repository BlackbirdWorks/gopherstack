package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"

	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
	cfnbackend "github.com/blackbirdworks/gopherstack/cloudformation"
	cwbackend "github.com/blackbirdworks/gopherstack/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/demo"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	elasticachebackend "github.com/blackbirdworks/gopherstack/elasticache"
	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	kinesisbackend "github.com/blackbirdworks/gopherstack/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	snsevents "github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"
	sfnasl "github.com/blackbirdworks/gopherstack/stepfunctions/asl"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

const (
	defaultPort     = "8000"
	defaultRegion   = "us-east-1"
	defaultTimeout  = 30 * time.Second
	shutdownTimeout = 5 * time.Second
)

// CLI holds all command-line / environment-variable configuration for Gopherstack.
type CLI struct {
	SSM                   struct{}            `embed:"" prefix:"ssm-"`
	SecretsManager        struct{}            `embed:"" prefix:"secretsmanager-"`
	KMS                   struct{}            `embed:"" prefix:"kms-"`
	SQS                   sqsbackend.Settings `embed:"" prefix:"sqs-"`
	SNS                   struct{}            `embed:"" prefix:"sns-"`
	STS                   struct{}            `embed:"" prefix:"sts-"`
	IAM                   struct{}            `embed:"" prefix:"iam-"`
	kinesisHandler        service.Registerable
	elasticacheHandler    service.Registerable
	secretsManagerHandler service.Registerable
	ddbHandler            service.Registerable
	s3Handler             service.Registerable
	ssmHandler            service.Registerable
	iamHandler            service.Registerable
	stsHandler            service.Registerable
	snsHandler            service.Registerable
	sqsHandler            service.Registerable
	lambdaHandler         service.Registerable
	eventBridgeHandler    service.Registerable
	apiGatewayHandler     service.Registerable
	cloudWatchLogsHandler service.Registerable
	stepFunctionsHandler  service.Registerable
	cloudWatchHandler     service.Registerable
	cloudFormationHandler service.Registerable
	kmsHandler            service.Registerable
	snsClient             *sns.Client
	kmsClient             *kms.Client
	iamClient             *iam.Client
	s3Client              *s3.Client
	ssmClient             *ssmsdk.Client
	ddbClient             *dynamodb.Client
	stsClient             *stssdk.Client
	sqsClient             *sqssdk.Client
	secretsManagerClient  *secretsmanager.Client
	AccountID             string                 `                                  name:"account-id"         env:"ACCOUNT_ID"         default:"000000000000" help:"Mock AWS account ID used in ARNs."`                                //nolint:lll // config struct tags are intentionally verbose
	Port                  string                 `                                  name:"port"               env:"PORT"               default:"8000"         help:"HTTP server port."`                                                //nolint:lll // config struct tags are intentionally verbose
	ElastiCacheEngine     string                 `                                  name:"elasticache-engine" env:"ELASTICACHE_ENGINE" default:"embedded"     help:"ElastiCache engine mode: embedded (miniredis), stub, or docker."`  //nolint:lll // config struct tags are intentionally verbose
	Region                string                 `                                  name:"region"             env:"REGION"             default:"us-east-1"    help:"AWS region."`                                                      //nolint:lll // config struct tags are intentionally verbose
	LogLevel              string                 `                                  name:"log-level"          env:"LOG_LEVEL"          default:"info"         help:"Log level (debug|info|warn|error)."`                               //nolint:lll // config struct tags are intentionally verbose
	DNSListenAddr         string                 `                                  name:"dns-addr"           env:"DNS_ADDR"           default:""             help:"Address for embedded DNS server (e.g. :10053). Empty = disabled."` //nolint:lll // config struct tags are intentionally verbose
	DNSResolveIP          string                 `                                  name:"dns-resolve-ip"     env:"DNS_RESOLVE_IP"     default:"127.0.0.1"    help:"IP address synthetic hostnames resolve to."`                       //nolint:lll // config struct tags are intentionally verbose
	S3                    s3backend.Settings     `embed:"" prefix:"s3-"`
	InitScripts           []string               `                                  name:"init-script"        env:"INIT_SCRIPTS"                              help:"Shell scripts to run on startup (may be specified multiple times)."` //nolint:lll // config struct tags are intentionally verbose
	DynamoDB              ddbbackend.Settings    `embed:"" prefix:"dynamodb-"`
	Lambda                lambdabackend.Settings `embed:"" prefix:"lambda-"`
	PortRangeStart        int                    `                                  name:"port-range-start"   env:"PORT_RANGE_START"   default:"10000"        help:"Start of the port range for resource endpoints."`           //nolint:lll // config struct tags are intentionally verbose
	PortRangeEnd          int                    `                                  name:"port-range-end"     env:"PORT_RANGE_END"     default:"10100"        help:"End (exclusive) of the port range for resource endpoints."` //nolint:lll // config struct tags are intentionally verbose
	InitScriptTimeout     time.Duration          `                                  name:"init-timeout"       env:"INIT_TIMEOUT"       default:"30s"          help:"Per-script timeout for init hooks."`                        //nolint:lll // config struct tags are intentionally verbose
	Demo                  bool                   `                                  name:"demo"               env:"DEMO"               default:"false"        help:"Load demo data on startup."`                                //nolint:lll // config struct tags are intentionally verbose
}

// GetGlobalConfig returns the centralised account ID and region (config.Provider).
func (c *CLI) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{
		AccountID: c.AccountID,
		Region:    c.Region,
	}
}

// GetDynamoDBSettings returns DynamoDB settings (dynamodb.ConfigProvider).
func (c *CLI) GetDynamoDBSettings() ddbbackend.Settings {
	return c.DynamoDB
}

// GetS3Settings returns S3 settings (s3.ConfigProvider).
func (c *CLI) GetS3Settings() s3backend.Settings {
	return c.S3
}

// GetS3Endpoint returns the configured S3 endpoint (s3.ConfigProvider).
func (c *CLI) GetS3Endpoint() string {
	s3Port := strings.TrimPrefix(c.Port, ":")

	return "localhost:" + s3Port
}

// GetLambdaSettings returns Lambda settings (lambda.SettingsProvider).
func (c *CLI) GetLambdaSettings() lambdabackend.Settings {
	return c.Lambda
}

// GetDynamoDBClient returns the SDK client for DynamoDB (dashboard.AWSSDKProvider).
func (c *CLI) GetDynamoDBClient() *dynamodb.Client { return c.ddbClient }

// GetS3Client returns the SDK client for S3 (dashboard.AWSSDKProvider).
func (c *CLI) GetS3Client() *s3.Client { return c.s3Client }

// GetSSMClient returns the SDK client for SSM (dashboard.AWSSDKProvider).
func (c *CLI) GetSSMClient() *ssmsdk.Client { return c.ssmClient }

// GetSTSClient returns the SDK client for STS (dashboard.AWSSDKProvider).
func (c *CLI) GetSTSClient() *stssdk.Client { return c.stsClient }

// GetSQSClient returns the SDK client for SQS (dashboard.AWSSDKProvider).
func (c *CLI) GetSQSClient() *sqssdk.Client { return c.sqsClient }

// GetDynamoDBHandler returns the DynamoDB handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetDynamoDBHandler() service.Registerable { return c.ddbHandler }

// GetS3Handler returns the S3 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetS3Handler() service.Registerable { return c.s3Handler }

// GetSSMHandler returns the SSM handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSSMHandler() service.Registerable { return c.ssmHandler }

// GetIAMHandler returns the IAM handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIAMHandler() service.Registerable { return c.iamHandler }

// GetSTSHandler returns the STS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSTSHandler() service.Registerable { return c.stsHandler }

// GetSNSHandler returns the SNS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSNSHandler() service.Registerable { return c.snsHandler }

// GetSQSHandler returns the SQS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSQSHandler() service.Registerable { return c.sqsHandler }

// GetKMSHandler returns the KMS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKMSHandler() service.Registerable { return c.kmsHandler }

// GetSecretsManagerHandler returns the Secrets Manager handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSecretsManagerHandler() service.Registerable { return c.secretsManagerHandler }

// GetLambdaHandler returns the Lambda handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetLambdaHandler() service.Registerable { return c.lambdaHandler }

// GetEventBridgeHandler returns the EventBridge handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEventBridgeHandler() service.Registerable { return c.eventBridgeHandler }

// GetAPIGatewayHandler returns the API Gateway handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAPIGatewayHandler() service.Registerable { return c.apiGatewayHandler }

// GetCloudWatchLogsHandler returns the CloudWatch Logs handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudWatchLogsHandler() service.Registerable { return c.cloudWatchLogsHandler }

// GetStepFunctionsHandler returns the Step Functions handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetStepFunctionsHandler() service.Registerable { return c.stepFunctionsHandler }

// GetCloudWatchHandler returns the CloudWatch handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudWatchHandler() service.Registerable { return c.cloudWatchHandler }

// GetCloudFormationHandler returns the CloudFormation handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudFormationHandler() service.Registerable { return c.cloudFormationHandler }

// GetKinesisHandler returns the Kinesis handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKinesisHandler() service.Registerable { return c.kinesisHandler }

// GetElastiCacheHandler returns the ElastiCache handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetElastiCacheHandler() service.Registerable { return c.elasticacheHandler }

// GetElastiCacheEngine returns the ElastiCache engine mode (elasticache.EngineConfig).
func (c *CLI) GetElastiCacheEngine() string { return c.ElastiCacheEngine }

// Run parses CLI / environment-variable configuration and starts Gopherstack.
// It is called from main() and exits on error.
func Run() {
	var cli CLI

	kong.Parse(
		&cli,
		kong.Name("gopherstack"),
		kong.Description("In-memory AWS DynamoDB + S3 compatible server."),
	)

	if err := run(context.Background(), cli); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// run starts the server with the given CLI configuration.
// It is separated from Run so it can be exercised in tests without [os.Exit].
func run(ctx context.Context, cli CLI) error {
	log := buildLogger(cli.LogLevel)

	// --- Port allocator ---
	portAlloc, err := portalloc.New(cli.PortRangeStart, cli.PortRangeEnd)
	if err != nil {
		log.WarnContext(ctx, "Port allocator disabled (invalid range)", "error", err)
	} else {
		log.InfoContext(ctx, "Port allocator ready",
			"start", cli.PortRangeStart,
			"end", cli.PortRangeEnd,
			"available", portAlloc.Available(),
		)
	}

	// --- Embedded DNS server ---
	var dnsSrv *gopherDNS.Server
	if cli.DNSListenAddr != "" {
		dnsSrv = startEmbeddedDNS(ctx, log, cli.DNSListenAddr, cli.DNSResolveIP)
	}

	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	awsCfgVal, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion(cli.Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		awscfg.WithHTTPClient(inMemClient),
	)
	if err != nil {
		log.ErrorContext(ctx, "Failed to load AWS config", "error", err)

		return err
	}

	initializeClients(&cli, awsCfgVal)

	janitorCtx, janitorCancel := context.WithCancel(ctx)
	defer janitorCancel()

	appCtx := &service.AppContext{
		Logger:     log,
		Config:     &cli,
		JanitorCtx: janitorCtx,
		PortAlloc:  portAlloc,
	}

	services, err := initializeServices(appCtx)
	if err != nil {
		return err
	}

	// Wire DNS registrar to Lambda backend for function URL hostname registration.
	if dnsSrv != nil {
		wireLambdaDNS(cli.lambdaHandler, dnsSrv)
	}

	e := echo.New()
	e.Use(httputil.RequestIDMiddleware())
	e.Pre(logger.EchoMiddleware(log))
	e.GET("/_gopherstack/health", healthHandler)

	if setupErr := setupRegistry(e, log, services); setupErr != nil {
		return setupErr
	}

	startBackgroundWorkers(janitorCtx, log, services)
	inMemMux.Handle("/", e)

	if cli.Demo {
		log.InfoContext(ctx, "Loading demo data...")
		err = demo.LoadData(ctx, log, &demo.Clients{
			DynamoDB:       cli.ddbClient,
			S3:             cli.s3Client,
			SQS:            cli.sqsClient,
			SNS:            cli.snsClient,
			IAM:            cli.iamClient,
			STS:            cli.stsClient,
			SSM:            cli.ssmClient,
			KMS:            cli.kmsClient,
			SecretsManager: cli.secretsManagerClient,
		})
		if err != nil {
			log.ErrorContext(ctx, "Failed to load demo data", "error", err)
		}
	}

	// --- Init hooks ---
	if len(cli.InitScripts) > 0 {
		runner := inithooks.New(cli.InitScripts, cli.InitScriptTimeout, log)
		runner.Run(ctx)
	}

	return startServer(ctx, log, cli.Port, e)
}

// initializeClients configures the AWS SDK clients for DynamoDB, S3, SSM, and STS.
func initializeClients(cli *CLI, awsCfg aws.Config) {
	cli.ddbClient = dynamodb.NewFromConfig(
		awsCfg,
		func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.s3Client = s3.NewFromConfig(
		awsCfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.ssmClient = ssmsdk.NewFromConfig(
		awsCfg,
		func(o *ssmsdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.stsClient = stssdk.NewFromConfig(
		awsCfg,
		func(o *stssdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.sqsClient = sqssdk.NewFromConfig(
		awsCfg,
		func(o *sqssdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.snsClient = sns.NewFromConfig(
		awsCfg,
		func(o *sns.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.iamClient = iam.NewFromConfig(
		awsCfg,
		func(o *iam.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.kmsClient = kms.NewFromConfig(
		awsCfg,
		func(o *kms.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.secretsManagerClient = secretsmanager.NewFromConfig(
		awsCfg,
		func(o *secretsmanager.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
}

// initializeServices initializes all service providers.
func initializeServices(appCtx *service.AppContext) ([]service.Registerable, error) {
	var services []service.Registerable
	serviceProviders := []service.Provider{
		&ddbbackend.Provider{},
		&s3backend.Provider{},
		&ssmbackend.Provider{},
		&iambackend.Provider{},
		&stsbackend.Provider{},
		&snsbackend.Provider{},
		&sqsbackend.Provider{},
		&kmsbackend.Provider{},
		&secretsmanagerbackend.Provider{},
		&lambdabackend.Provider{},
		&ebbackend.Provider{},
		&apigwbackend.Provider{},
		&cwlogsbackend.Provider{},
		&sfnbackend.Provider{},
		&cwbackend.Provider{},
		&kinesisbackend.Provider{},
		&elasticachebackend.Provider{},
	}

	for _, provider := range serviceProviders {
		svc, err := provider.Init(appCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to init %s: %w", provider.Name(), err)
		}

		services = append(services, svc)
	}

	// Store handlers in CLI so dashboard and CloudFormation can access them.
	if cli, ok := appCtx.Config.(*CLI); ok {
		cli.ddbHandler = services[0]
		cli.s3Handler = services[1]
		cli.ssmHandler = services[2]
		cli.iamHandler = services[3]
		cli.stsHandler = services[4]
		cli.snsHandler = services[5]
		cli.sqsHandler = services[6]
		cli.kmsHandler = services[7]
		cli.secretsManagerHandler = services[8]
		cli.lambdaHandler = services[9]
		cli.eventBridgeHandler = services[10]
		cli.apiGatewayHandler = services[11]
		cli.cloudWatchLogsHandler = services[12]
		cli.stepFunctionsHandler = services[13]
		cli.cloudWatchHandler = services[14]
		cli.kinesisHandler = services[15]
		cli.elasticacheHandler = services[16]
	}

	// Wire SNS→SQS delivery: when SNS publishes a message, deliver it to SQS queues.
	wireSNSToSQS(services[5], services[6])

	// Wire EventBridge target fan-out: deliver events to Lambda, SQS, SNS targets.
	wireEventBridgeDelivery(services[10], services[9], services[6], services[5])

	// Wire S3 bucket notification delivery to SQS/SNS targets.
	wireS3Notifications(services[1], services[6], services[5])

	// Wire Step Functions → Lambda Task integration.
	wireStepFunctionsLambda(services[13], services[9])

	// Wire API Gateway → Lambda proxy integration.
	wireAPIGatewayLambda(services[11], services[9])

	// Wire Kinesis → Lambda event source mapping poller.
	wireKinesisLambda(services[15], services[9])

	// Wire CloudWatch Logs → Lambda log delivery.
	wireLambdaCWLogs(services[9], services[12])

	// Wire Lambda invoker → SecretsManager rotation.
	wireSecretsManagerLambda(services[8], services[9])

	// Init CloudFormation after core handlers are stored so it can access their backends.
	cfnSvc, err := (&cfnbackend.Provider{}).Init(appCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init CloudFormation: %w", err)
	}

	services = append(services, cfnSvc)

	if cli, ok := appCtx.Config.(*CLI); ok {
		cli.cloudFormationHandler = cfnSvc
	}

	// Init dashboard last so it can access all service handlers.
	dashSvc, err := (&dashboard.Provider{}).Init(appCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init Dashboard: %w", err)
	}
	services = append(services, dashSvc)

	// The router sorts services by MatchPriority() at startup, so registration order
	// does not affect routing correctness.
	return services, nil
}

// startBackgroundWorkers starts all background workers from services.
func startBackgroundWorkers(ctx context.Context, log *slog.Logger, services []service.Registerable) {
	for _, svc := range services {
		if worker, ok := svc.(service.BackgroundWorker); ok {
			if workerErr := worker.StartWorker(ctx); workerErr != nil {
				log.ErrorContext(ctx, "failed to start background worker", "error", workerErr)
			}
		}
	}
}

// wireSNSToSQS connects the SNS publish emitter to the SQS delivery handler so
// that messages published to SNS topics are delivered to subscribed SQS queues.
// snsReg and sqsReg must be the service.Registerable values returned by their
// respective providers (indices 5 and 6 in the services slice).
func wireSNSToSQS(snsReg, sqsReg service.Registerable) {
	snsH, ok1 := snsReg.(*snsbackend.Handler)
	sqsH, ok2 := sqsReg.(*sqsbackend.Handler)

	if !ok1 || !ok2 {
		return
	}

	snsBk, ok3 := snsH.Backend.(*snsbackend.InMemoryBackend)
	sqsBk, ok4 := sqsH.Backend.(*sqsbackend.InMemoryBackend)

	if !ok3 || !ok4 {
		return
	}

	emitter := snsevents.NewInMemoryEmitter[*snsevents.SNSPublishedEvent]()
	snsBk.SetPublishEmitter(emitter)
	sqsBk.SubscribeToSNS(emitter)
}

// wireEventBridgeDelivery connects EventBridge fan-out to Lambda, SQS, and SNS backends.
// ebReg, lambdaReg, sqsReg, snsReg must be the service.Registerable values returned
// by their respective providers (indices 10, 9, 6, 5 in the services slice).
func wireEventBridgeDelivery(ebReg, lambdaReg, sqsReg, snsReg service.Registerable) {
	ebH, ok := ebReg.(*ebbackend.Handler)
	if !ok {
		return
	}

	ebBk, bkOk := ebH.Backend.(*ebbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	dt := &ebbackend.DeliveryTargets{}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			dt.Lambda = &lambdaInvokerAdapter{backend: lambdaBk}
		}
	}

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		if sqsBk, bk2Ok := sqsH.Backend.(*sqsbackend.InMemoryBackend); bk2Ok {
			dt.SQS = &sqsSenderAdapter{backend: sqsBk}
		}
	}

	if snsH, snsOk := snsReg.(*snsbackend.Handler); snsOk {
		if snsBk, bk2Ok := snsH.Backend.(*snsbackend.InMemoryBackend); bk2Ok {
			dt.SNS = &snsPublisherAdapter{backend: snsBk}
		}
	}

	ebBk.SetDeliveryTargets(dt)
}

// lambdaInvokerAdapter adapts the Lambda backend to the eventbridge.LambdaInvoker interface.
type lambdaInvokerAdapter struct {
	backend *lambdabackend.InMemoryBackend
}

func (a *lambdaInvokerAdapter) InvokeFunction(
	ctx context.Context, name, invocationType string, payload []byte,
) ([]byte, int, error) {
	it := lambdabackend.InvocationType(invocationType)

	return a.backend.InvokeFunction(ctx, name, it, payload)
}

// sqsSenderAdapter adapts the SQS backend to the eventbridge.SQSSender interface.
type sqsSenderAdapter struct {
	backend *sqsbackend.InMemoryBackend
}

func (a *sqsSenderAdapter) SendMessageToQueue(_ context.Context, queueARN, messageBody string) error {
	// Convert SQS ARN to queue name (last segment after ':').
	queueURL := arnToSQSQueueURL(queueARN)
	_, err := a.backend.SendMessage(&sqsbackend.SendMessageInput{
		QueueURL:    queueURL,
		MessageBody: messageBody,
	})

	return err
}

// snsPublisherAdapter adapts the SNS backend to the eventbridge.SNSPublisher interface.
type snsPublisherAdapter struct {
	backend *snsbackend.InMemoryBackend
}

func (a *snsPublisherAdapter) PublishToTopic(_ context.Context, topicARN, message string) error {
	_, err := a.backend.Publish(topicARN, message, "", "", nil)

	return err
}

// wireS3Notifications connects the S3 handler to SQS and SNS backends so that
// bucket notification configurations are honoured on PutObject and DeleteObject.
// s3Reg, sqsReg, snsReg must be services[1], services[6], services[5].
func wireS3Notifications(s3Reg, sqsReg, snsReg service.Registerable) {
	s3H, ok := s3Reg.(*s3backend.S3Handler)
	if !ok {
		return
	}

	targets := &s3backend.NotificationTargets{}

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		if sqsBk, bkOk := sqsH.Backend.(*sqsbackend.InMemoryBackend); bkOk {
			targets.SQSSender = &sqsSenderAdapter{backend: sqsBk}
		}
	}

	if snsH, snsOk := snsReg.(*snsbackend.Handler); snsOk {
		if snsBk, bkOk := snsH.Backend.(*snsbackend.InMemoryBackend); bkOk {
			targets.SNSPublisher = &s3SNSPublisherAdapter{backend: snsBk}
		}
	}

	s3H.SetNotificationDispatcher(s3backend.NewNotificationDispatcher(targets, "us-east-1"))
}

// s3SNSPublisherAdapter adapts the SNS backend to the s3.SNSPublisher interface.
type s3SNSPublisherAdapter struct {
	backend *snsbackend.InMemoryBackend
}

func (a *s3SNSPublisherAdapter) PublishToTopic(_ context.Context, topicARN, message, _ string) error {
	_, err := a.backend.Publish(topicARN, message, "", "", nil)

	return err
}

// wireAPIGatewayLambda connects the API Gateway handler to the Lambda backend
// for AWS_PROXY integrations.
func wireAPIGatewayLambda(apigwReg, lambdaReg service.Registerable) {
	apigwH, ok := apigwReg.(*apigwbackend.Handler)
	if !ok {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			apigwH.SetLambdaInvoker(&apigwLambdaInvokerAdapter{backend: lambdaBk})
		}
	}
}

// apigwLambdaInvokerAdapter adapts the Lambda backend to the apigateway.LambdaInvoker interface.
type apigwLambdaInvokerAdapter struct {
	backend *lambdabackend.InMemoryBackend
}

func (a *apigwLambdaInvokerAdapter) InvokeFunction(
	ctx context.Context, name, invocationType string, payload []byte,
) ([]byte, int, error) {
	it := lambdabackend.InvocationType(invocationType)

	return a.backend.InvokeFunction(ctx, name, it, payload)
}

// so that Task states with Lambda resources can invoke functions.
func wireStepFunctionsLambda(sfnReg, lambdaReg service.Registerable) {
	sfnH, ok := sfnReg.(*sfnbackend.Handler)
	if !ok {
		return
	}

	sfnBk, bkOk := sfnH.Backend.(*sfnbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			sfnBk.SetLambdaInvoker(&sfnLambdaInvokerAdapter{backend: lambdaBk})
		}
	}
}

// sfnLambdaInvokerAdapter adapts the Lambda backend to the asl.LambdaInvoker interface.
type sfnLambdaInvokerAdapter struct {
	backend *lambdabackend.InMemoryBackend
}

func (a *sfnLambdaInvokerAdapter) InvokeFunction(
	ctx context.Context, name, invocationType string, payload []byte,
) ([]byte, int, error) {
	it := lambdabackend.InvocationType(invocationType)

	return a.backend.InvokeFunction(ctx, name, it, payload)
}

// Ensure sfnLambdaInvokerAdapter implements sfnasl.LambdaInvoker.
var _ sfnasl.LambdaInvoker = (*sfnLambdaInvokerAdapter)(nil)

// wireKinesisLambda connects the Kinesis backend to the Lambda event source poller
// so that records written to Kinesis streams trigger Lambda functions with active
// event source mappings.
func wireKinesisLambda(kinesisReg, lambdaReg service.Registerable) {
	kinesisH, ok := kinesisReg.(*kinesisbackend.Handler)
	if !ok {
		return
	}

	kinesisBk, bkOk := kinesisH.Backend.(*kinesisbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			adapter := &kinesisReaderAdapter{backend: kinesisBk}
			lambdaBk.SetKinesisPoller(lambdabackend.NewEventSourcePoller(lambdaBk, adapter, lambdaH.Logger))
		}
	}
}

// kinesisReaderAdapter adapts the Kinesis backend to the lambda.KinesisReader interface.
type kinesisReaderAdapter struct {
	backend *kinesisbackend.InMemoryBackend
}

func (a *kinesisReaderAdapter) GetShardIDs(streamName string) ([]string, error) {
	out, err := a.backend.DescribeStream(&kinesisbackend.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(out.Shards))
	for i, s := range out.Shards {
		ids[i] = s.ShardID
	}

	return ids, nil
}

func (a *kinesisReaderAdapter) GetShardIterator(
	streamName, shardID, iteratorType, startingSeqNum string,
) (string, error) {
	out, err := a.backend.GetShardIterator(&kinesisbackend.GetShardIteratorInput{
		StreamName:             streamName,
		ShardID:                shardID,
		ShardIteratorType:      iteratorType,
		StartingSequenceNumber: startingSeqNum,
	})
	if err != nil {
		return "", err
	}

	return out.ShardIterator, nil
}

func (a *kinesisReaderAdapter) GetRecords(
	iteratorToken string,
	limit int,
) ([]lambdabackend.KinesisRecord, string, error) {
	out, err := a.backend.GetRecords(&kinesisbackend.GetRecordsInput{
		ShardIterator: iteratorToken,
		Limit:         limit,
	})
	if err != nil {
		return nil, "", err
	}

	records := make([]lambdabackend.KinesisRecord, len(out.Records))
	for i, r := range out.Records {
		records[i] = lambdabackend.KinesisRecord{
			PartitionKey:   r.PartitionKey,
			SequenceNumber: r.SequenceNumber,
			Data:           r.Data,
			ArrivalTime:    r.ApproximateArrivalTimestamp,
		}
	}

	return records, out.NextShardIterator, nil
}

// ARN format: arn:aws:sqs:region:accountId:queueName
// URL format expected by SQS backend: http://endpoint/accountId/queueName
func arnToSQSQueueURL(arn string) string {
	parts := strings.Split(arn, ":")
	// Minimum parts for a valid SQS ARN: arn, aws, sqs, region, accountId, queueName
	const minARNParts = 6
	if len(parts) < minARNParts {
		return arn
	}

	accountID := parts[4]
	queueName := parts[5]

	return "http://local/" + accountID + "/" + queueName
}

// wireLambdaCWLogs connects the Lambda backend to CloudWatch Logs so that
// function invocations produce log entries in /aws/lambda/{function-name}.
func wireLambdaCWLogs(lambdaReg, cwlogsReg service.Registerable) {
	lambdaH, ok := lambdaReg.(*lambdabackend.Handler)
	if !ok {
		return
	}

	lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if cwlogsH, cwlogsOk := cwlogsReg.(*cwlogsbackend.Handler); cwlogsOk {
		if cwlogsBk, cwBkOk := cwlogsH.Backend.(*cwlogsbackend.InMemoryBackend); cwBkOk {
			lambdaBk.SetCWLogsBackend(&cwLogsAdapter{backend: cwlogsBk})
		}
	}
}

// cwLogsAdapter adapts the CloudWatch Logs InMemoryBackend to the lambda.CWLogsBackend interface.
type cwLogsAdapter struct {
	backend *cwlogsbackend.InMemoryBackend
}

func (a *cwLogsAdapter) EnsureLogGroupAndStream(groupName, streamName string) error {
	if _, err := a.backend.CreateLogGroup(groupName); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogGroupAlreadyExists) {
		return err
	}

	if _, err := a.backend.CreateLogStream(groupName, streamName); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogStreamAlreadyExist) {
		return err
	}

	return nil
}

func (a *cwLogsAdapter) PutLogLines(groupName, streamName string, messages []string) error {
	events := make([]cwlogsbackend.InputLogEvent, len(messages))
	now := time.Now().UnixMilli()

	for i, msg := range messages {
		events[i] = cwlogsbackend.InputLogEvent{Message: msg, Timestamp: now}
	}

	_, err := a.backend.PutLogEvents(groupName, streamName, events)

	return err
}

// wireSecretsManagerLambda wires the Lambda invoker into the SecretsManager handler
// so that RotateSecret with a RotationLambdaARN invokes the Lambda function.
func wireSecretsManagerLambda(smReg, lambdaReg service.Registerable) {
	smH, ok := smReg.(*secretsmanagerbackend.Handler)
	if !ok {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			smH.SetLambdaInvoker(&lambdaInvokerAdapter{backend: lambdaBk})
		}
	}
}

func startServer(ctx context.Context, log *slog.Logger, port string, e *echo.Echo) error {
	if port[0] != ':' {
		port = ":" + port
	}

	log.InfoContext(ctx, "Starting Gopherstack (DynamoDB + S3)", "port", port)
	log.InfoContext(ctx, "  DynamoDB endpoint", "url", "http://localhost"+port)
	log.InfoContext(ctx, "  S3 endpoint      ", "url", "http://localhost"+port+" (path-style)")
	log.InfoContext(ctx, "  Dashboard        ", "url", "http://localhost"+port+"/dashboard")

	server := &http.Server{
		Addr:         port,
		Handler:      e,
		ReadTimeout:  defaultTimeout,
		WriteTimeout: defaultTimeout,
		IdleTimeout:  defaultTimeout,
	}

	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.InfoContext(ctx, "Shutting down server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.ErrorContext(ctx, "Server shutdown failed", "error", err)

			return err
		}

		return nil
	case err := <-errChan:
		log.ErrorContext(ctx, "Failed to start server", "error", err)

		return err
	}
}

// buildLogger converts the CLI log-level string to a [slog.Logger].
func buildLogger(level string) *slog.Logger {
	var slogLevel slog.Level

	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	return logger.NewLogger(slogLevel)
}

// healthResponse is the JSON body returned by the health endpoint.
type healthResponse struct {
	// Status is always "ok" when the server is running.
	Status string `json:"status"`
	// Services lists all registered mock AWS services.
	Services []string `json:"services"`
}

// healthHandler returns a JSON status response for all mock services.
func healthHandler(c *echo.Context) error {
	return c.JSON(http.StatusOK, healthResponse{
		Status: "ok",
		Services: []string{
			"DynamoDB", "S3", "SSM", "IAM", "STS", "SNS", "SQS", "KMS", "SecretsManager", "Lambda",
			"EventBridge", "APIGateway", "CloudWatchLogs", "StepFunctions", "CloudWatch", "CloudFormation",
			"Kinesis",
		},
	})
}

func setupRegistry(
	e *echo.Echo,
	log *slog.Logger,
	services []service.Registerable,
) error {
	registry := service.NewRegistry(log)

	for _, svc := range services {
		if err := registry.Register(svc); err != nil {
			log.Error("Failed to register service", "service", svc.Name(), "error", err)

			return err
		}
	}

	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	return nil
}

// startEmbeddedDNS creates and starts the embedded DNS server.
// Configuration errors and startup failures are logged as warnings; the server
// continues to run without DNS in those cases.
func startEmbeddedDNS(ctx context.Context, log *slog.Logger, addr, resolveIP string) *gopherDNS.Server {
	dnsSrv, err := gopherDNS.New(gopherDNS.Config{
		ListenAddr: addr,
		ResolveIP:  resolveIP,
		Logger:     log,
	})
	if err != nil {
		log.WarnContext(ctx, "DNS server disabled (config error)", "error", err)

		return nil
	}

	if startErr := dnsSrv.Start(ctx); startErr != nil {
		log.WarnContext(ctx, "DNS server failed to start", "error", startErr)

		return nil
	}

	log.InfoContext(ctx, "DNS server started", "addr", addr)

	return dnsSrv
}

// wireLambdaDNS sets the DNS registrar on the Lambda backend so function URL
// hostnames are automatically registered when CreateFunctionUrlConfig is called.
func wireLambdaDNS(lambdaReg service.Registerable, dns lambdabackend.DNSRegistrar) {
	if lambdaReg == nil || dns == nil {
		return
	}

	lambdaH, ok := lambdaReg.(*lambdabackend.Handler)
	if !ok {
		return
	}

	if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
		lambdaBk.SetDNSRegistrar(dns)
	}
}
