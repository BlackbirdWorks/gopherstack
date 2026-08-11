package main

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	glaciertypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	glacierbackend "github.com/blackbirdworks/gopherstack/services/glacier"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// TestInitializeServices_GlacierSelectS3Wiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than invoking
// wireGlacierS3 directly, so that deleting the wiring call from
// wireStorageAndSecretsIntegrations -- not just breaking the helper function
// itself -- is what this test is sensitive to. This is the shape
// iot/appsync/sagemaker's undetected routing gaps had: correct code nothing
// reaches (gopherstack-2mwl). Covers gopherstack-6zvp's S3 write-back: a
// completed Select job's real OutputLocation objects (job.txt/results/
// result_manifest.txt) must land in the S3 backend wired through cli.go, not just
// be servable via GetJobOutput.
func TestInitializeServices_GlacierSelectS3Wiring(t *testing.T) {
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

	glH, ok := byName["Glacier"].(*glacierbackend.Handler)
	require.True(t, ok, "Glacier handler must be registered")

	_, ok = glH.Backend.(*glacierbackend.InMemoryBackend)
	require.True(t, ok, "Glacier backend must be an InMemoryBackend")

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(t, ok, "S3 handler must be registered")

	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok, "S3 backend must be an InMemoryBackend")

	ctx := t.Context()
	bucketName := "wiring-test-bucket"

	_, err = s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(glH))
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

	client := glaciersdk.NewFromConfig(cfg, func(o *glaciersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	vaultName := "wired-select-vault"
	_, err = client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)

	archiveData := "1,alice,30\n2,bob,25\n"

	uploadOut, err := client.UploadArchive(ctx, &glaciersdk.UploadArchiveInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		Body:      strings.NewReader(archiveData),
	})
	require.NoError(t, err)

	initOut, err := client.InitiateJob(ctx, &glaciersdk.InitiateJobInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		JobParameters: &glaciertypes.JobParameters{
			Type:      aws.String("select"),
			ArchiveId: uploadOut.ArchiveId,
			SelectParameters: &glaciertypes.SelectParameters{
				Expression:     aws.String("SELECT * FROM archive WHERE _3 > 27"),
				ExpressionType: glaciertypes.ExpressionTypeSql,
				InputSerialization: &glaciertypes.InputSerialization{
					Csv: &glaciertypes.CSVInput{FileHeaderInfo: glaciertypes.FileHeaderInfoNone},
				},
				OutputSerialization: &glaciertypes.OutputSerialization{
					Csv: &glaciertypes.CSVOutput{},
				},
			},
			OutputLocation: &glaciertypes.OutputLocation{
				S3: &glaciertypes.S3Location{BucketName: aws.String(bucketName), Prefix: aws.String("select-out/")},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, initOut.JobId)

	jobID := aws.ToString(initOut.JobId)

	// DescribeJob observes the job's Succeeded transition and is one of the
	// trigger points for materializing S3 output. The job completes only after
	// the simulated retrieval window (services/glacier defaultRetrievalDelay,
	// ~100ms) elapses -- poll until then, matching
	// test/integration/glacier_test.go's TestIntegration_Glacier_JobLifecycle.
	require.Eventually(t, func() bool {
		out, descErr := client.DescribeJob(ctx, &glaciersdk.DescribeJobInput{
			AccountId: aws.String("-"), VaultName: aws.String(vaultName), JobId: aws.String(jobID),
		})

		return descErr == nil && out.Completed
	}, 10*time.Second, 25*time.Millisecond, "select job should reach Succeeded after the retrieval window")

	resultsKey := "select-out/" + jobID + "/results/1"

	out, err := s3Bk.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String(resultsKey),
	})
	require.NoError(t, err,
		"a completed Select job must write its real result through the actual "+
			"cli.go composition root's Glacier->S3 wiring, not just the wiring "+
			"helper called directly")

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, "1,alice,30\n", string(data))

	manifestOut, err := s3Bk.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName), Key: aws.String("select-out/" + jobID + "/result_manifest.txt"),
	})
	require.NoError(t, err, "a completed Select job must write a result manifest")
	_ = manifestOut.Body.Close()
}
