package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// clients bundles one SDK client per service exercised by pgoload, all
// pointed at the same Gopherstack endpoint.
type clients struct {
	ddb            *dynamodb.Client
	s3             *s3.Client
	sqs            *sqs.Client
	sns            *sns.Client
	kinesis        *kinesis.Client
	iam            *iam.Client
	sts            *sts.Client
	ssm            *ssm.Client
	secretsmanager *secretsmanager.Client
	cloudwatch     *cloudwatch.Client
	logs           *cloudwatchlogs.Client
	ec2            *ec2.Client
	lambda         *lambda.Client
	kms            *kms.Client
	eventbridge    *eventbridge.Client
	stepfunctions  *sfn.Client
}

// buildClients constructs every service client used by pgoload, using
// static test credentials against cfg.endpoint.
func buildClients(ctx context.Context, cfg config) (*clients, error) {
	awsCfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(awsRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	ep := aws.String(cfg.endpoint)

	return &clients{
		ddb: dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) { o.BaseEndpoint = ep }),
		s3: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = ep
			o.UsePathStyle = true
		}),
		sqs:            sqs.NewFromConfig(awsCfg, func(o *sqs.Options) { o.BaseEndpoint = ep }),
		sns:            sns.NewFromConfig(awsCfg, func(o *sns.Options) { o.BaseEndpoint = ep }),
		kinesis:        kinesis.NewFromConfig(awsCfg, func(o *kinesis.Options) { o.BaseEndpoint = ep }),
		iam:            iam.NewFromConfig(awsCfg, func(o *iam.Options) { o.BaseEndpoint = ep }),
		sts:            sts.NewFromConfig(awsCfg, func(o *sts.Options) { o.BaseEndpoint = ep }),
		ssm:            ssm.NewFromConfig(awsCfg, func(o *ssm.Options) { o.BaseEndpoint = ep }),
		secretsmanager: secretsmanager.NewFromConfig(awsCfg, func(o *secretsmanager.Options) { o.BaseEndpoint = ep }),
		cloudwatch:     cloudwatch.NewFromConfig(awsCfg, func(o *cloudwatch.Options) { o.BaseEndpoint = ep }),
		logs:           cloudwatchlogs.NewFromConfig(awsCfg, func(o *cloudwatchlogs.Options) { o.BaseEndpoint = ep }),
		ec2:            ec2.NewFromConfig(awsCfg, func(o *ec2.Options) { o.BaseEndpoint = ep }),
		lambda:         lambda.NewFromConfig(awsCfg, func(o *lambda.Options) { o.BaseEndpoint = ep }),
		kms:            kms.NewFromConfig(awsCfg, func(o *kms.Options) { o.BaseEndpoint = ep }),
		eventbridge:    eventbridge.NewFromConfig(awsCfg, func(o *eventbridge.Options) { o.BaseEndpoint = ep }),
		stepfunctions:  sfn.NewFromConfig(awsCfg, func(o *sfn.Options) { o.BaseEndpoint = ep }),
	}, nil
}

// resources holds the identifiers of everything provisioned once during
// setup and then reused by every breadth-scenario worker.
type resources struct {
	roleArn         string
	queueURL        string
	queueArn        string
	topicArn        string
	kinesisShardID  string
	kmsKeyID        string
	functionName    string
	stateMachineArn string
	s3Buckets       []string
}

// setupResources provisions the DynamoDB table, S3 buckets, and every
// breadth-scenario's resources, bounded by setupTimeout so a short
// -duration still leaves time for the load phase itself. Each service's
// setup is independent; a failure in one is fatal (unlike op-level errors
// during the load phase, which are only counted) because a missing
// resource would make that whole scenario error-spin instead of exercising
// its real hot paths.
func setupResources(ctx context.Context, cls *clients, log *slog.Logger) (*resources, error) {
	setupCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	if err := ensureDDBTable(setupCtx, cls.ddb, log); err != nil {
		return nil, fmt.Errorf("ddb table setup: %w", err)
	}

	buckets, err := ensureS3Buckets(setupCtx, cls.s3, log)
	if err != nil {
		return nil, fmt.Errorf("s3 bucket setup: %w", err)
	}

	res := &resources{s3Buckets: buckets}

	if res.roleArn, err = ensureIAMRole(setupCtx, cls.iam, log); err != nil {
		return nil, fmt.Errorf("iam role setup: %w", err)
	}

	if res.queueURL, res.queueArn, err = ensureSQSQueue(setupCtx, cls.sqs, log); err != nil {
		return nil, fmt.Errorf("sqs queue setup: %w", err)
	}

	if res.topicArn, err = ensureSNSTopic(setupCtx, cls.sns, res.queueArn, log); err != nil {
		return nil, fmt.Errorf("sns topic setup: %w", err)
	}

	if err = ensureSecrets(setupCtx, cls.secretsmanager, log); err != nil {
		return nil, fmt.Errorf("secretsmanager setup: %w", err)
	}

	if res.kinesisShardID, err = ensureKinesisStream(setupCtx, cls.kinesis, log); err != nil {
		return nil, fmt.Errorf("kinesis stream setup: %w", err)
	}

	if err = ensureLogGroup(setupCtx, cls.logs, log); err != nil {
		return nil, fmt.Errorf("logs group setup: %w", err)
	}

	if res.kmsKeyID, err = ensureKMSKey(setupCtx, cls.kms, log); err != nil {
		return nil, fmt.Errorf("kms key setup: %w", err)
	}

	if res.functionName, err = ensureLambdaFunction(setupCtx, cls.lambda, res.roleArn, log); err != nil {
		return nil, fmt.Errorf("lambda function setup: %w", err)
	}

	if err = ensureEventBridgeRule(setupCtx, cls.eventbridge, res.queueArn, log); err != nil {
		return nil, fmt.Errorf("eventbridge rule setup: %w", err)
	}

	if res.stateMachineArn, err = ensureStateMachine(setupCtx, cls.stepfunctions, res.roleArn, log); err != nil {
		return nil, fmt.Errorf("step functions state machine setup: %w", err)
	}

	return res, nil
}
