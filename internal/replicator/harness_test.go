package replicator_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/blackbirdworks/gopherstack/services/ssm"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	srcAccount = "111111111111"
	dstAccount = "222222222222"
	region     = "us-east-1"
)

func newInstance(t *testing.T, account string) string {
	t.Helper()

	regs := []service.Registerable{
		sts.NewHandler(sts.NewInMemoryBackendWithConfig(account)),
		s3.NewHandler(s3.NewInMemoryBackend(nil)),
		sqs.NewHandler(sqs.NewInMemoryBackendWithConfig(account, region)),
		sns.NewHandler(sns.NewInMemoryBackendWithConfig(account, region)),
		dynamodb.NewHandler(dynamodb.NewInMemoryDB()),
		ec2.NewHandler(ec2.NewInMemoryBackend(account, region)),
		iam.NewHandler(iam.NewInMemoryBackendWithConfig(account)),
		ssm.NewHandler(ssm.NewInMemoryBackend()),
		secretsmanager.NewHandler(secretsmanager.NewInMemoryBackendWithConfig(account, region)),
		kms.NewHandler(kms.NewInMemoryBackendWithConfig(account, region)),
		eventbridge.NewHandler(eventbridge.NewInMemoryBackendWithConfig(account, region)),
		lambda.NewHandler(lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), account, region)),
		kinesis.NewHandler(kinesis.NewInMemoryBackend()),
		cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackendWithConfig(account, region)),
	}

	return sdktest.Serve(t, regs...)
}

type pair struct {
	src, dst string
	srcCfg   aws.Config
	dstCfg   aws.Config
}

func newPair(t *testing.T) pair {
	t.Helper()

	p := pair{src: newInstance(t, srcAccount), dst: newInstance(t, dstAccount)}
	p.srcCfg = sdkConfig(t, p.src, srcAccount)
	p.dstCfg = sdkConfig(t, p.dst, dstAccount)

	return p
}

func sdkConfig(t *testing.T, url, key string) aws.Config {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion(region),
		config.WithBaseEndpoint(url),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, "secret", "")),
	)
	require.NoError(t, err)

	return cfg
}

func (p pair) config(types ...string) replicator.Config {
	return replicator.Config{
		Source: replicator.Endpoint{
			URL: p.src, Region: region, AccessKeyID: srcAccount, SecretAccessKey: "secret",
		},
		Target: replicator.Endpoint{URL: p.dst, Region: region, AccessKeyID: dstAccount, SecretAccessKey: "secret"},
		Types:  types,
	}
}

func actions(rep *replicator.Report) map[string]string {
	m := map[string]string{}
	for _, r := range rep.Results {
		m[r.Type+"/"+r.ID] = r.Action
	}

	return m
}
