package replicator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"github.com/blackbirdworks/gopherstack/internal/awswire/iam"
	"github.com/blackbirdworks/gopherstack/internal/awswire/kinesis"
	"github.com/blackbirdworks/gopherstack/internal/awswire/kms"
	"github.com/blackbirdworks/gopherstack/internal/awswire/lambda"
	"github.com/blackbirdworks/gopherstack/internal/awswire/secretsmanager"
	"github.com/blackbirdworks/gopherstack/internal/awswire/sns"
	"github.com/blackbirdworks/gopherstack/internal/awswire/sqs"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	clientTimeout   = 5 * time.Minute
	defaultRegion   = "us-east-1"
	targetStaticKey = "test"
)

type sdkClients struct {
	http   *httpDoer
	s3     *s3.Client
	sqs    *sqs.Client
	sns    *sns.Client
	ddb    *dynamodb.Client
	iam    *iam.Client
	ec2    *ec2Wire
	lambda *lambda.Client
	ssm    *ssm.Client
	sm     *secretsmanager.Client
	kms    *kms.Client
	eb     *eventbridge.Client
	kin    *kinesis.Client
	logs   *cloudwatchlogs.Client
	sts    *sts.Client
	region string
}

func newClients(ctx context.Context, ep Endpoint, isTarget bool) (*sdkClients, error) {
	hc := httpclient.New(clientTimeout)
	opts := []func(*config.LoadOptions) error{config.WithHTTPClient(hc)}

	if ep.Region != "" {
		opts = append(opts, config.WithRegion(ep.Region))
	}

	if ep.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(ep.Profile))
	}

	if ep.URL != "" {
		opts = append(opts, config.WithBaseEndpoint(ep.URL))
	}

	switch {
	case ep.AccessKeyID != "":
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(ep.AccessKeyID, ep.SecretAccessKey, ep.SessionToken)))
	case isTarget:
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(targetStaticKey, targetStaticKey, "")))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	if cfg.Region == "" {
		cfg.Region = defaultRegion
	}

	return &sdkClients{
		region: cfg.Region,
		http:   &httpDoer{c: hc},
		s3:     s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = ep.URL != "" }),
		sqs:    sqs.NewFromConfig(cfg),
		sns:    sns.NewFromConfig(cfg),
		ddb:    dynamodb.NewFromConfig(cfg),
		iam:    iam.NewFromConfig(cfg),
		ec2:    newEC2Wire(cfg, hc),
		lambda: lambda.NewFromConfig(cfg),
		ssm:    ssm.NewFromConfig(cfg),
		sm:     secretsmanager.NewFromConfig(cfg),
		kms:    kms.NewFromConfig(cfg),
		eb:     eventbridge.NewFromConfig(cfg),
		kin:    kinesis.NewFromConfig(cfg),
		logs:   cloudwatchlogs.NewFromConfig(cfg),
		sts:    sts.NewFromConfig(cfg),
	}, nil
}

// accountID returns the caller's account, or "" when STS is unavailable.
func (c *sdkClients) accountID(ctx context.Context) string {
	out, err := c.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return ""
	}

	return aws.ToString(out.Account)
}

func errCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

// isNotFound matches the not-found error codes used across AWS services.
func isNotFound(err error) bool {
	code := strings.ToLower(errCode(err))
	if code == "" {
		return false
	}

	for _, m := range []string{"notfound", "nosuch", "doesnotexist", "nonexistent", "notexist"} {
		if strings.Contains(code, m) {
			return true
		}
	}

	return false
}

// existsErr maps a describe error to (exists, hardError).
func existsErr(err error) (bool, error) {
	switch {
	case err == nil:
		return true, nil
	case isNotFound(err):
		return false, nil
	default:
		return false, err
	}
}
