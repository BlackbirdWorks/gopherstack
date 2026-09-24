package main

// Benchmarks the shared per-request path every service pays for: routing
// (pkgs/service.Router), the global middleware chain (panic recovery,
// request ID, API console capture, memory stats, aws-meta, chaos, region
// tracking), and the JSON/REST protocol dispatch helpers. Requests are built
// by real AWS SDK v2 clients (accurate wire shapes) but delivered straight
// to the *echo.Echo instance via benchTransport, skipping the TCP socket so
// CPU/mem profiles attribute time to router/middleware code rather than
// net/http transport plumbing.
//
// Run with profiling:
//
//	go test -run=^$ -bench=BenchmarkSharedPath -benchmem -cpuprofile=/tmp/cpu.out -memprofile=/tmp/mem.out .
//	go tool pprof -top /tmp/cpu.out
import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdklambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdksqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sdksts "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dynamodbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const benchEndpoint = "http://gopherstack.bench"

// benchTransport drives an *http.Request straight through the echo server's
// ServeHTTP, bypassing any real socket.
type benchTransport struct {
	e *echo.Echo
}

func (t benchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		req.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	t.e.ServeHTTP(rec, req)

	return rec.Result(), nil
}

//nolint:gochecknoglobals // one shared full-server instance across benchmark functions in this file
var (
	benchServerOnce sync.Once
	benchEcho       *echo.Echo
	benchByName     map[string]service.Registerable
)

// benchServer builds the full production composition (all ~169 services,
// the real router, and the real global middleware chain) exactly once per
// benchmark process and reuses it across every BenchmarkSharedPath_* case.
func benchServer(b *testing.B) (*echo.Echo, map[string]service.Registerable) {
	b.Helper()

	benchServerOnce.Do(func() {
		log := buildLogger("")
		ctx := context.Background()

		cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
		cli.portAlloc = setupPortAllocatorWithReservations(ctx, log, cli)
		cli.faultStore = chaos.NewFaultStore()

		appCtx := &service.AppContext{
			Logger:     log,
			Config:     &cli,
			JanitorCtx: ctx,
			PortAlloc:  cli.portAlloc,
		}

		services, err := initializeServices(appCtx)
		if err != nil {
			panic(err)
		}

		e := buildEchoServer(ctx, log, nil, services, cli)
		if setupErr := setupChaosAndRegistry(e, log, &cli, services); setupErr != nil {
			panic(setupErr)
		}

		benchEcho = e
		benchByName = serviceByName(services)
	})

	return benchEcho, benchByName
}

func newBenchAWSConfig(b *testing.B, rt http.RoundTripper) aws.Config {
	b.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(&http.Client{Transport: rt}),
	)
	require.NoError(b, err)

	return cfg
}

//nolint:gochecknoglobals // guards one-time SQS fixture setup shared across calibration runs
var (
	sqsSetupOnce sync.Once
	sqsQueueURL  string
)

func BenchmarkSharedPath_SQSSendMessage(b *testing.B) {
	e, byName := benchServer(b)

	sqsH, ok := byName["SQS"].(*sqsbackend.Handler)
	require.True(b, ok, "SQS handler must be registered")

	sqsSetupOnce.Do(func() {
		out, err := sqsH.Backend.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "bench-shared-path-sqs"})
		require.NoError(b, err)
		sqsQueueURL = out.QueueURL
	})

	cfg := newBenchAWSConfig(b, benchTransport{e: e})
	client := sdksqs.NewFromConfig(cfg, func(o *sdksqs.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
	})

	ctx := b.Context()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, sendErr := client.SendMessage(ctx, &sdksqs.SendMessageInput{
			QueueUrl:    aws.String(sqsQueueURL),
			MessageBody: aws.String("bench message body"),
		})
		require.NoError(b, sendErr)
	}
}

//nolint:gochecknoglobals // guards one-time DynamoDB fixture setup shared across calibration runs
var ddbSetupOnce sync.Once

func BenchmarkSharedPath_DynamoDBGetItem(b *testing.B) {
	e, byName := benchServer(b)

	ddbH, ok := byName["DynamoDB"].(*dynamodbbackend.DynamoDBHandler)
	require.True(b, ok, "DynamoDB handler must be registered")

	ctx := b.Context()
	const tableName = "bench-shared-path-table"

	// go test's benchmark calibration re-invokes this function with
	// increasing b.N, so fixture creation must run exactly once, not once
	// per calibration pass.
	ddbSetupOnce.Do(func() {
		_, err := ddbH.Backend.CreateTable(ctx, &sdkdynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
			},
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
		})
		require.NoError(b, err)

		_, err = ddbH.Backend.PutItem(ctx, &sdkdynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "bench-item"},
			},
		})
		require.NoError(b, err)
	})

	cfg := newBenchAWSConfig(b, benchTransport{e: e})
	client := sdkdynamodb.NewFromConfig(cfg, func(o *sdkdynamodb.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
	})

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, getErr := client.GetItem(ctx, &sdkdynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: "bench-item"},
			},
		})
		require.NoError(b, getErr)
	}
}

//nolint:gochecknoglobals // guards one-time S3 fixture setup shared across calibration runs
var s3SetupOnce sync.Once

func BenchmarkSharedPath_S3GetObject(b *testing.B) {
	e, byName := benchServer(b)

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(b, ok, "S3 handler must be registered")

	ctx := b.Context()
	const bucket = "bench-shared-path-bucket"
	const key = "bench-object.txt"

	// go test's benchmark calibration re-invokes this function with
	// increasing b.N, so fixture creation must run exactly once, not once
	// per calibration pass.
	s3SetupOnce.Do(func() {
		_, err := s3H.Backend.CreateBucket(ctx, &sdks3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(b, err)

		_, err = s3H.Backend.PutObject(ctx, &sdks3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("bench object body"),
		})
		require.NoError(b, err)
	})

	cfg := newBenchAWSConfig(b, benchTransport{e: e})
	client := sdks3.NewFromConfig(cfg, func(o *sdks3.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
		o.UsePathStyle = true
	})

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		out, getErr := client.GetObject(ctx, &sdks3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(b, getErr)
		_ = out.Body.Close()
	}
}

func BenchmarkSharedPath_STSGetCallerIdentity(b *testing.B) {
	e, _ := benchServer(b)

	cfg := newBenchAWSConfig(b, benchTransport{e: e})
	client := sdksts.NewFromConfig(cfg, func(o *sdksts.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
	})

	ctx := b.Context()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := client.GetCallerIdentity(ctx, &sdksts.GetCallerIdentityInput{})
		require.NoError(b, err)
	}
}

func BenchmarkSharedPath_LambdaListFunctions(b *testing.B) {
	e, _ := benchServer(b)

	cfg := newBenchAWSConfig(b, benchTransport{e: e})
	client := sdklambda.NewFromConfig(cfg, func(o *sdklambda.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
	})

	ctx := b.Context()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := client.ListFunctions(ctx, &sdklambda.ListFunctionsInput{})
		require.NoError(b, err)
	}
}
