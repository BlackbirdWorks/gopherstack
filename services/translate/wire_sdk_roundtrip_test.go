package translate_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	translatesdk "github.com/aws/aws-sdk-go-v2/service/translate"
	translatetypes "github.com/aws/aws-sdk-go-v2/service/translate/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/translate"
)

const wireTestRegion = "us-east-1"

// newTestTranslateSDKClient stands up the real aws-sdk-go-v2 translate client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- so a shape is
// verified by the real client's own JSON-RPC deserializer
// (awsAwsjson11_deserializeOpDocument<Op>Output), not gopherstack's own map
// keys.
func newTestTranslateSDKClient(t *testing.T, h *translate.Handler) *translatesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(wireTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return translatesdk.NewFromConfig(cfg, func(o *translatesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetParallelData_SDKRoundTrip_EncryptionKey proves ParallelDataProperties.
// EncryptionKey (types.EncryptionKey{Id,Type}) survives the real SDK client's
// deserializer. CreateParallelData accepts and persists EncryptionKey
// (parallel_data.go's CreateParallelData -> ParallelData.EncryptionKey), and
// the sibling terminologyToMap (handler_terminologies.go) already surfaces
// the analogous field on GetTerminology -- but parallelDataToMap
// (handler_parallel_data.go) omitted the "EncryptionKey" key entirely, so a
// real client's GetParallelDataOutput.ParallelDataProperties.EncryptionKey
// silently deserialized as nil regardless of what was set at creation.
func TestGetParallelData_SDKRoundTrip_EncryptionKey(t *testing.T) {
	t.Parallel()

	backend := translate.NewInMemoryBackend("000000000000", wireTestRegion)
	h := translate.NewHandler(backend)
	client := newTestTranslateSDKClient(t, h)

	_, err := client.CreateParallelData(t.Context(), &translatesdk.CreateParallelDataInput{
		Name: aws.String("wire-test-pd"),
		ParallelDataConfig: &translatetypes.ParallelDataConfig{
			S3Uri:  aws.String("s3://bucket/pd.csv"),
			Format: translatetypes.ParallelDataFormatCsv,
		},
		EncryptionKey: &translatetypes.EncryptionKey{
			Id:   aws.String("arn:aws:kms:us-east-1:000000000000:key/wire-test-key"),
			Type: translatetypes.EncryptionKeyTypeKms,
		},
	})
	require.NoError(t, err)

	out, err := client.GetParallelData(t.Context(), &translatesdk.GetParallelDataInput{
		Name: aws.String("wire-test-pd"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ParallelDataProperties)

	require.NotNil(t, out.ParallelDataProperties.EncryptionKey, "EncryptionKey must survive GetParallelData")
	assert.Equal(
		t,
		"arn:aws:kms:us-east-1:000000000000:key/wire-test-key",
		aws.ToString(out.ParallelDataProperties.EncryptionKey.Id),
	)
	assert.Equal(t, translatetypes.EncryptionKeyTypeKms, out.ParallelDataProperties.EncryptionKey.Type)

	require.NotNil(t, out.DataLocation)
	assert.Equal(t, "S3", aws.ToString(out.DataLocation.RepositoryType))
}

// startJobForFilterTest starts a translation job through the real SDK client
// and returns its JobId.
func startJobForFilterTest(t *testing.T, client *translatesdk.Client, jobName string) string {
	t.Helper()

	out, err := client.StartTextTranslationJob(t.Context(), &translatesdk.StartTextTranslationJobInput{
		JobName:             aws.String(jobName),
		SourceLanguageCode:  aws.String("en"),
		TargetLanguageCodes: []string{"fr"},
		DataAccessRoleArn:   aws.String("arn:aws:iam::000000000000:role/TranslateRole"),
		InputDataConfig: &translatetypes.InputDataConfig{
			S3Uri:       aws.String("s3://bucket/input/"),
			ContentType: aws.String("text/plain"),
		},
		OutputDataConfig: &translatetypes.OutputDataConfig{
			S3Uri: aws.String("s3://bucket/output/"),
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.JobId)
}

// TestListTextTranslationJobs_SDKRoundTrip_Filters proves
// ListTextTranslationJobsInput.Filter's JobName/SubmittedAfterTime/
// SubmittedBeforeTime members (api_op_ListTextTranslationJobs.go,
// types.TextTranslationJobFilter) actually constrain the result. Only
// Filter.JobStatus was ever read by the handler -- the other three were
// silently ignored, so a real client's JobName/time-window request returned
// every job in the account regardless of what it asked for. Also proves the
// documented sort order: "SubmittedAfterTime ... returned in descending
// order, newest to oldest" and "SubmittedBeforeTime ... returned in
// ascending order, oldest to newest" (both doc comments on
// TextTranslationJobFilter).
func TestListTextTranslationJobs_SDKRoundTrip_Filters(t *testing.T) {
	t.Parallel()

	backend := translate.NewInMemoryBackend("000000000000", wireTestRegion)
	h := translate.NewHandler(backend)
	client := newTestTranslateSDKClient(t, h)

	idA := startJobForFilterTest(t, client, "filter-job-a")
	idB := startJobForFilterTest(t, client, "filter-job-b")
	idC := startJobForFilterTest(t, client, "filter-job-c")

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, translate.SetJobSubmittedAtForTest(backend, idA, base))
	require.True(t, translate.SetJobSubmittedAtForTest(backend, idB, base.Add(time.Hour)))
	require.True(t, translate.SetJobSubmittedAtForTest(backend, idC, base.Add(2*time.Hour)))

	t.Run("JobName filters to the exact match", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTextTranslationJobs(t.Context(), &translatesdk.ListTextTranslationJobsInput{
			Filter: &translatetypes.TextTranslationJobFilter{JobName: aws.String("filter-job-b")},
		})
		require.NoError(t, err)
		require.Len(t, out.TextTranslationJobPropertiesList, 1)
		assert.Equal(t, idB, aws.ToString(out.TextTranslationJobPropertiesList[0].JobId))
	})

	t.Run("SubmittedAfterTime excludes earlier jobs and sorts newest first", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTextTranslationJobs(t.Context(), &translatesdk.ListTextTranslationJobsInput{
			Filter: &translatetypes.TextTranslationJobFilter{SubmittedAfterTime: aws.Time(base)},
		})
		require.NoError(t, err)
		require.Len(t, out.TextTranslationJobPropertiesList, 2)
		assert.Equal(t, idC, aws.ToString(out.TextTranslationJobPropertiesList[0].JobId))
		assert.Equal(t, idB, aws.ToString(out.TextTranslationJobPropertiesList[1].JobId))
	})

	t.Run("SubmittedBeforeTime excludes later jobs and sorts oldest first", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTextTranslationJobs(t.Context(), &translatesdk.ListTextTranslationJobsInput{
			Filter: &translatetypes.TextTranslationJobFilter{SubmittedBeforeTime: aws.Time(base.Add(2 * time.Hour))},
		})
		require.NoError(t, err)
		require.Len(t, out.TextTranslationJobPropertiesList, 2)
		assert.Equal(t, idA, aws.ToString(out.TextTranslationJobPropertiesList[0].JobId))
		assert.Equal(t, idB, aws.ToString(out.TextTranslationJobPropertiesList[1].JobId))
	})

	t.Run("no time filter still defaults to newest first", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTextTranslationJobs(t.Context(), &translatesdk.ListTextTranslationJobsInput{})
		require.NoError(t, err)
		require.Len(t, out.TextTranslationJobPropertiesList, 3)
		assert.Equal(t, idC, aws.ToString(out.TextTranslationJobPropertiesList[0].JobId))
		assert.Equal(t, idB, aws.ToString(out.TextTranslationJobPropertiesList[1].JobId))
		assert.Equal(t, idA, aws.ToString(out.TextTranslationJobPropertiesList[2].JobId))
	})
}
