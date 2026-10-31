package app

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdkiam "github.com/aws/aws-sdk-go-v2/service/iam"
	sdklambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdksqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

//nolint:gochecknoglobals // one shared full-server instance across the partition tests
var (
	partitionServerOnce sync.Once
	partitionEcho       *echo.Echo
)

func partitionServer(t *testing.T) *echo.Echo {
	t.Helper()

	partitionServerOnce.Do(func() {
		log := buildLogger("")
		ctx := context.Background()

		cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
		cli.portAlloc = setupPortAllocatorWithReservations(ctx, log, cli)
		cli.faultStore = chaos.NewFaultStore()

		services, err := initializeServicesT(t, &service.AppContext{
			Logger: log, Config: &cli, JanitorCtx: ctx, PortAlloc: cli.portAlloc,
		})
		if err != nil {
			panic(err)
		}

		e := buildEchoServer(ctx, log, nil, services, cli)
		if setupErr := setupChaosAndRegistry(e, log, &cli, services); setupErr != nil {
			panic(setupErr)
		}

		partitionEcho = e
	})

	return partitionEcho
}

func partitionConfig(t *testing.T, region string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(&http.Client{Transport: benchTransport{e: partitionServer(t)}}),
	)
	require.NoError(t, err)

	return cfg
}

func emptyZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)
	f, err := w.Create("index.py")
	require.NoError(t, err)
	_, err = f.Write([]byte("def handler(e, c):\n    return e\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestPartitionStrict_RegionDerivedARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, cfg aws.Config, name string) string
		name   string
		region string
		prefix string
	}{
		{createRoleARN, "iam_aws", "us-east-1", "arn:aws:iam::"},
		{createQueueARN, "sqs_cn", "cn-north-1", "arn:aws-cn:sqs:cn-north-1:"},
		{createQueueARN, "sqs_gov", "us-gov-west-1", "arn:aws-us-gov:sqs:us-gov-west-1:"},
		{createFunctionARN, "lambda_cn", "cn-north-1", "arn:aws-cn:lambda:cn-north-1:"},
		{createQueueARN, "sqs_aws", "us-east-1", "arn:aws:sqs:us-east-1:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.create(t, partitionConfig(t, tt.region), "part-"+tt.name)
			assert.Contains(t, got, tt.prefix)
		})
	}
}

func createRoleARN(t *testing.T, cfg aws.Config, name string) string {
	t.Helper()

	out, err := sdkiam.NewFromConfig(cfg, func(o *sdkiam.Options) { o.BaseEndpoint = aws.String(benchEndpoint) }).
		CreateRole(context.Background(), &sdkiam.CreateRoleInput{
			RoleName:                 aws.String(name),
			AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
	require.NoError(t, err)

	return aws.ToString(out.Role.Arn)
}

func createQueueARN(t *testing.T, cfg aws.Config, name string) string {
	t.Helper()

	c := sdksqs.NewFromConfig(cfg, func(o *sdksqs.Options) { o.BaseEndpoint = aws.String(benchEndpoint) })
	q, err := c.CreateQueue(context.Background(), &sdksqs.CreateQueueInput{QueueName: aws.String(name)})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(context.Background(), &sdksqs.GetQueueAttributesInput{
		QueueUrl:       q.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	return attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
}

func createFunctionARN(t *testing.T, cfg aws.Config, name string) string {
	t.Helper()

	out, err := sdklambda.NewFromConfig(cfg, func(o *sdklambda.Options) { o.BaseEndpoint = aws.String(benchEndpoint) }).
		CreateFunction(context.Background(), &sdklambda.CreateFunctionInput{
			FunctionName: aws.String(name),
			Runtime:      lambdatypes.RuntimePython312,
			Handler:      aws.String("index.handler"),
			Role:         aws.String("arn:aws-cn:iam::000000000000:role/r"),
			Code:         &lambdatypes.FunctionCode{ZipFile: emptyZip(t)},
		})
	require.NoError(t, err)

	return aws.ToString(out.FunctionArn)
}

const govDLQPolicy = `{"deadLetterTargetArn":"arn:aws-us-gov:sqs:us-gov-west-1:000000000000:dlq","maxReceiveCount":3}`

func TestPartitionStrict_RejectsCrossPartitionARN(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		call   func(cfg aws.Config) error
		name   string
		region string
		code   string
	}{
		{func(cfg aws.Config) error {
			_, err := sdkiam.NewFromConfig(cfg, func(o *sdkiam.Options) { o.BaseEndpoint = aws.String(benchEndpoint) }).
				AttachRolePolicy(ctx, &sdkiam.AttachRolePolicyInput{
					RoleName:  aws.String("nope"),
					PolicyArn: aws.String("arn:aws-cn:iam::aws:policy/ReadOnlyAccess"),
				})

			return err
		}, "iam_attach_cn_arn_from_aws", "us-east-1", "ValidationError"},
		{func(cfg aws.Config) error {
			_, err := sdkiam.NewFromConfig(cfg, func(o *sdkiam.Options) { o.BaseEndpoint = aws.String(benchEndpoint) }).
				AttachRolePolicy(ctx, &sdkiam.AttachRolePolicyInput{
					RoleName:  aws.String("nope"),
					PolicyArn: aws.String("arn:aws-us-gov:iam::aws:policy/ReadOnlyAccess"),
				})

			return err
		}, "iam_attach_gov_arn_from_cn", "cn-north-1", "ValidationError"},
		{func(cfg aws.Config) error {
			_, err := sdksqs.NewFromConfig(cfg, func(o *sdksqs.Options) { o.BaseEndpoint = aws.String(benchEndpoint) }).
				CreateQueue(ctx, &sdksqs.CreateQueueInput{
					QueueName:  aws.String("x"),
					Attributes: map[string]string{"RedrivePolicy": govDLQPolicy},
				})

			return err
		}, "sqs_tag_gov_arn_from_aws", "us-east-1", "InvalidParameterValue"},
		{func(cfg aws.Config) error {
			_, err := sdklambda.NewFromConfig(cfg, func(o *sdklambda.Options) { o.BaseEndpoint = aws.String(benchEndpoint) }).
				GetFunction(ctx, &sdklambda.GetFunctionInput{
					FunctionName: aws.String("arn:aws-cn:lambda:cn-north-1:000000000000:function:f"),
				})

			return err
		}, "lambda_get_cn_arn_from_aws", "us-east-1", "InvalidParameterValueException"},
		{func(cfg aws.Config) error {
			_, err := sdks3.NewFromConfig(cfg, func(o *sdks3.Options) {
				o.BaseEndpoint = aws.String(benchEndpoint)
				o.UsePathStyle = true
			}).CopyObject(ctx, &sdks3.CopyObjectInput{
				Bucket:     aws.String("dst"),
				Key:        aws.String("k"),
				CopySource: aws.String("arn:aws-cn:s3:::src/k"),
			})

			return err
		}, "s3_copy_source_cn_arn_from_aws", "us-east-1", "InvalidArgument"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(partitionConfig(t, tt.region))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.code)
			assert.Contains(t, err.Error(), "partition")
		})
	}
}
