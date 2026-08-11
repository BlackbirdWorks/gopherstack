package main

import (
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestInitializeServices_SageMakerS3PipelineWiring drives the actual
// composition root (initializeServices, the function cli.go's Run() calls)
// rather than invoking wireSageMakerS3 directly, so that deleting the wiring
// call from wireStorageAndSecretsIntegrations -- not just breaking the helper
// function itself -- is what this test is sensitive to. This is the shape
// iot/appsync/sagemaker's undetected routing gaps had: correct code nothing
// reaches (gopherstack-2mwl).
func TestInitializeServices_SageMakerS3PipelineWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(t, ok, "S3 handler must be registered")

	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok, "S3 backend must be an InMemoryBackend")

	smH, ok := byName["SageMaker"].(*sagemakerbackend.Handler)
	require.True(t, ok, "SageMaker handler must be registered")

	ctx := t.Context()
	bucketName := "wiring-test-bucket"
	objectKey := "defs/pipeline.json"
	definition := `{"Version":"2020-12-01","Steps":[]}`

	_, err = s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	_, err = s3Bk.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(definition),
	})
	require.NoError(t, err)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(smH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := sagemakersdk.NewFromConfig(cfg, func(o *sagemakersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreatePipeline(ctx, &sagemakersdk.CreatePipelineInput{
		PipelineName: aws.String("s3-wired-pipeline"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/Role"),
		PipelineDefinitionS3Location: &smtypes.PipelineDefinitionS3Location{
			Bucket:    aws.String(bucketName),
			ObjectKey: aws.String(objectKey),
		},
	})
	require.NoError(t, err,
		"CreatePipeline must fetch the definition through the actual cli.go "+
			"composition root's S3 wiring, not just the wiring helper called directly")

	out, err := client.DescribePipeline(ctx, &sagemakersdk.DescribePipelineInput{
		PipelineName: aws.String("s3-wired-pipeline"),
	})
	require.NoError(t, err)
	require.JSONEq(t, definition, aws.ToString(out.PipelineDefinition))
}
