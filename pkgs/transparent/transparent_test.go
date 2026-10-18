package transparent_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/transparent"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

type env struct {
	ca   *devtls.CA
	addr string
}

func newEnv(t *testing.T) *env {
	t.Helper()

	registry := service.NewRegistry()
	regs := []service.Registerable{
		ddbbackend.NewHandler(ddbbackend.NewInMemoryDB()),
		sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()),
		snsbackend.NewHandler(snsbackend.NewInMemoryBackend()),
		stsbackend.NewHandler(stsbackend.NewInMemoryBackend()),
		iambackend.NewHandler(iambackend.NewInMemoryBackend()),
		kinesisbackend.NewHandler(kinesisbackend.NewInMemoryBackend()),
		lambdabackend.NewHandler(lambdabackend.NewInMemoryBackend(
			nil, nil, lambdabackend.Settings{}, "000000000000", "us-east-1")),
		s3backend.NewHandler(s3backend.NewInMemoryBackend(&s3backend.ZstdCompressor{})),
	}

	for _, r := range regs {
		require.NoError(t, registry.Register(r), r.Name())
	}

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	srv, err := transparent.Start(t.Context(), "127.0.0.1:0", e, ca)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	return &env{ca: ca, addr: srv.Addr().String()}
}

// cfg returns an SDK config with NO endpoint override: it dials the listener but
// verifies TLS and sends Host exactly as it would against real AWS.
func (e *env) cfg(region string) aws.Config {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: e.ca.Pool(), MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, network, e.addr)
		},
	}

	return aws.Config{
		Region:           region,
		Credentials:      credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:       &http.Client{Transport: tr},
		RetryMaxAttempts: 1,
	}
}

func TestRealEndpointRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		run  func(t *testing.T, cfg aws.Config)
		name string
	}{
		{name: "s3 virtual host", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := s3.NewFromConfig(cfg)
			_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("vhost-bkt")})
			require.NoError(t, err)
			_, err = c.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String("vhost-bkt"), Key: aws.String("a/b.txt"), Body: strings.NewReader("hello"),
			})
			require.NoError(t, err)
			out, err := c.GetObject(t.Context(), &s3.GetObjectInput{
				Bucket: aws.String("vhost-bkt"), Key: aws.String("a/b.txt"),
			})
			require.NoError(t, err)
			b, _ := io.ReadAll(out.Body)
			assert.Equal(t, "hello", string(b))
		}},
		{name: "s3 path style", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
			_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("path-bkt")})
			require.NoError(t, err)
			out, err := c.ListBuckets(t.Context(), &s3.ListBucketsInput{})
			require.NoError(t, err)
			names := make([]string, 0, len(out.Buckets))
			for _, b := range out.Buckets {
				names = append(names, aws.ToString(b.Name))
			}
			assert.Contains(t, names, "path-bkt")
		}},
		{name: "sqs", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := sqs.NewFromConfig(cfg)
			q, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("q1")})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(q.QueueUrl), "sqs.us-east-1.amazonaws.com")
			_, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
				QueueUrl: q.QueueUrl, MessageBody: aws.String("m"),
			})
			require.NoError(t, err)
			r, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl})
			require.NoError(t, err)
			require.Len(t, r.Messages, 1)
		}},
		{name: "dynamodb", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := dynamodb.NewFromConfig(cfg)
			_, err := c.CreateTable(t.Context(), &dynamodb.CreateTableInput{
				TableName:   aws.String("table1"),
				BillingMode: ddbtypes.BillingModePayPerRequest,
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
				},
				AttributeDefinitions: []ddbtypes.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)
			_, err = c.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String("table1"),
				Item:      map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
			})
			require.NoError(t, err)
			g, err := c.GetItem(t.Context(), &dynamodb.GetItemInput{
				TableName: aws.String("table1"),
				Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
			})
			require.NoError(t, err)
			assert.Len(t, g.Item, 1)
		}},
		{name: "lambda", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			_, err := lambda.NewFromConfig(cfg).ListFunctions(t.Context(), &lambda.ListFunctionsInput{})
			require.NoError(t, err)
		}},
		{name: "sts", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			out, err := sts.NewFromConfig(cfg).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.Account))
		}},
		{name: "iam global", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := iam.NewFromConfig(cfg)
			_, err := c.CreateUser(t.Context(), &iam.CreateUserInput{UserName: aws.String("bob")})
			require.NoError(t, err)
			out, err := c.ListUsers(t.Context(), &iam.ListUsersInput{})
			require.NoError(t, err)
			assert.Len(t, out.Users, 1)
		}},
		{name: "kinesis", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := kinesis.NewFromConfig(cfg)
			_, err := c.CreateStream(t.Context(), &kinesis.CreateStreamInput{
				StreamName: aws.String("s1"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)
			out, err := c.ListStreams(t.Context(), &kinesis.ListStreamsInput{})
			require.NoError(t, err)
			assert.Contains(t, out.StreamNames, "s1")
		}},
		{name: "sns", run: func(t *testing.T, cfg aws.Config) {
			t.Helper()

			c := sns.NewFromConfig(cfg)
			_, err := c.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("tp")})
			require.NoError(t, err)
			out, err := c.ListTopics(t.Context(), &sns.ListTopicsInput{})
			require.NoError(t, err)
			assert.Len(t, out.Topics, 1)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			region := "us-east-1"
			if tt.name == "dynamodb" {
				region = "eu-west-1"
			}

			tt.run(t, e.cfg(region))
		})
	}
}

func TestCAEndpoint(t *testing.T) {
	t.Parallel()

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	rec := &captureWriter{h: http.Header{}}
	transparent.CAHandler(ca).ServeHTTP(rec, &http.Request{})

	assert.Equal(t, string(ca.CertPEM()), rec.body.String())
	assert.Equal(t, "application/x-pem-file", rec.h.Get("Content-Type"))
}

func TestIsAWSHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host string
		want bool
	}{
		{"sqs.us-east-1.amazonaws.com", true},
		{"bkt.s3.amazonaws.com:443", true},
		{"ec2.cn-north-1.amazonaws.com.cn", true},
		{"s3.us-east-1.api.aws", true},
		{"amazonaws.com.evil.example", false},
		{"localhost:4566", false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, transparent.IsAWSHost(tt.host))
		})
	}
}

type captureWriter struct {
	h    http.Header
	body strings.Builder
}

func (c *captureWriter) Header() http.Header         { return c.h }
func (c *captureWriter) WriteHeader(int)             {}
func (c *captureWriter) Write(b []byte) (int, error) { return c.body.Write(b) }
