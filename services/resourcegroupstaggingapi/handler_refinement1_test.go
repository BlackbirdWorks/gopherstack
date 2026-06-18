package resourcegroupstaggingapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newBackend(t *testing.T) *resourcegroupstaggingapi.InMemoryBackend {
	t.Helper()

	return resourcegroupstaggingapi.NewInMemoryBackend(testAccountID, testRegion)
}

// seedResources registers a provider that returns the given resources.
func seedResources(
	b *resourcegroupstaggingapi.InMemoryBackend,
	resources []resourcegroupstaggingapi.TaggedResource,
) {
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return resources
	})
}

// ------------------------------------------------------------------ Reset ---

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{}
	})
	b.RegisterARNTagger(func(_ context.Context, _ string, _ map[string]string) (bool, error) { return false, nil })
	b.RegisterARNUntagger(func(_ context.Context, _ string, _ []string) (bool, error) { return false, nil })
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bucket/path", "2025-01-01T00:00:00Z")

	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.UntaggerCount(b))
	require.True(t, resourcegroupstaggingapi.HasReportState(b))

	b.Reset()

	// Reset() must only clear dynamic per-test state (reportState).
	// providers/taggers/untaggers are wired at startup and must survive reset
	// so that cross-service tagging continues to work after /_gopherstack/reset.
	assert.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b), "providers must survive Reset()")
	assert.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b), "taggers must survive Reset()")
	assert.Equal(t, 1, resourcegroupstaggingapi.UntaggerCount(b), "untaggers must survive Reset()")
	assert.False(t, resourcegroupstaggingapi.HasReportState(b), "reportState must be cleared by Reset()")
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource { return nil })
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bucket/path", "2025-01-01T00:00:00Z")

	h := resourcegroupstaggingapi.NewHandler(b)
	require.True(t, resourcegroupstaggingapi.HasReportState(b))
	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))

	h.Reset()

	// Only reportState is cleared; wired providers survive.
	assert.False(t, resourcegroupstaggingapi.HasReportState(b))
	assert.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b), "providers must survive Handler.Reset()")
}

// ------------------------------------------------------------------ AccountID/Region ---

func TestRefinement1_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "eu-west-1")

	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// ------------------------------------------------------------------ Provider ---

func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &resourcegroupstaggingapi.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrNilAppContext)
}

// ------------------------------------------------------------------ Snapshot/Restore ---

func TestRefinement1_SnapshotRestoreRoundtrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://my-bucket/report.csv", "2025-06-01T00:00:00Z")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := resourcegroupstaggingapi.NewInMemoryBackend("111111111111", "ap-southeast-1")
	err := b2.Restore(snap)
	require.NoError(t, err)

	assert.Equal(t, testAccountID, b2.AccountID())
	assert.Equal(t, testRegion, b2.Region())
	assert.True(t, resourcegroupstaggingapi.HasReportState(b2))
	assert.Equal(t, "SUCCEEDED", resourcegroupstaggingapi.ReportStatus(b2))
	assert.Equal(t, "s3://my-bucket/report.csv", resourcegroupstaggingapi.ReportS3Location(b2))
}

func TestRefinement1_SnapshotEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	snap := b.Snapshot()

	require.NotNil(t, snap)
	assert.False(t, resourcegroupstaggingapi.HasReportState(b))
}

func TestRefinement1_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.Restore([]byte("not-json"))

	require.Error(t, err)
}

func TestRefinement1_SnapshotClearsProvidersOnRestore(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{}
	})
	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))

	snap := b.Snapshot()

	b2 := newBackend(t)
	require.NoError(t, b2.Restore(snap))

	// Providers are runtime callbacks and cannot be serialized.
	assert.Equal(t, 0, resourcegroupstaggingapi.ProviderCount(b2))
}

// ------------------------------------------------------------------ Dispatch cached ---

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))

	assert.Equal(t, 9, resourcegroupstaggingapi.HandlerOpsLen(h))
}

// ------------------------------------------------------------------ GetTagKeys ---

func TestRefinement1_GetTagKeysPaginationToken(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"alpha": "v"}},
	})

	tok := "ignored-token"
	out := b.GetTagKeys(context.Background(), &resourcegroupstaggingapi.GetTagKeysInput{PaginationToken: &tok})

	require.NotNil(t, out)
	assert.Contains(t, out.TagKeys, "alpha")
}

func TestRefinement1_GetTagKeysEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.GetTagKeys(context.Background(), &resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	assert.Empty(t, out.TagKeys)
}

// ------------------------------------------------------------------ GetTagValues ---

func TestRefinement1_GetTagValuesNilKey(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"env": "prod"}},
	})

	// Key is nil — no values should be returned.
	out := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{})

	require.NotNil(t, out)
	assert.Empty(t, out.TagValues)
}

func TestRefinement1_GetTagValuesSorted(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	envKey := "env"
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"env": "staging"}},
		{ResourceARN: "arn:2", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:3", Tags: map[string]string{"env": "dev"}},
	})

	out := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{Key: &envKey})

	require.NotNil(t, out)
	assert.Equal(t, []string{"dev", "prod", "staging"}, out.TagValues)
}

// ------------------------------------------------------------------ buildTagMappings non-nil Tags ---

func TestRefinement1_GetResourcesNonNilTagsSlice(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:no-tags", ResourceType: "sqs:queue", Tags: map[string]string{}},
	})

	out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{})

	require.NoError(t, err)
	require.Len(t, out.ResourceTagMappingList, 1)
	// Tags must be a non-nil empty slice, not nil.
	assert.NotNil(t, out.ResourceTagMappingList[0].Tags)
	assert.Empty(t, out.ResourceTagMappingList[0].Tags)
}

// ------------------------------------------------------------------ TagResources deep copy ---

func TestRefinement1_TagResourcesDeepCopyTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	var receivedTags map[string]string

	b.RegisterARNTagger(func(_ context.Context, _ string, tags map[string]string) (bool, error) {
		receivedTags = tags

		return true, nil
	})

	originalTags := map[string]string{"env": "prod"}
	b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            originalTags,
	})

	// Mutating the original map must not affect the copy received by the tagger.
	originalTags["env"] = "mutated"

	require.NotNil(t, receivedTags)
	assert.Equal(t, "prod", receivedTags["env"])
}

// ------------------------------------------------------------------ StartReportCreation ---

func TestRefinement1_StartReportCreationSetsS3Location(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "report-bucket"},
	)

	require.NoError(t, err)
	assert.Equal(t, "s3://report-bucket/AwsTagPolicies/report.csv", resourcegroupstaggingapi.ReportS3Location(b))
}

func TestRefinement1_StartReportCreationSetsSucceededStatus(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "bkt"},
	)

	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", resourcegroupstaggingapi.ReportStatus(b))
}

func TestRefinement1_StartReportCreationTimestampFromNowFunc(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resourcegroupstaggingapi.SetNowFunc(b, func() string { return "2025-12-31T23:59:59Z" })

	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "bkt"},
	)
	require.NoError(t, err)

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "2025-12-31T23:59:59Z")
}

func TestRefinement1_StartReportCreationOverwritesPrevious(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "first"},
	)
	require.NoError(t, err)

	_, err = b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "second"},
	)
	require.NoError(t, err)

	assert.Contains(t, resourcegroupstaggingapi.ReportS3Location(b), "second")
}

// ------------------------------------------------------------------ DescribeReportCreation ---

func TestRefinement1_DescribeReportCreationNoReport(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.DescribeReportCreation(context.Background())

	require.NotNil(t, out)
	require.NotNil(t, out.Status)
	assert.Equal(t, "NO REPORT", *out.Status)
	assert.Nil(t, out.S3Location)
	assert.Nil(t, out.StartDate)
}

func TestRefinement1_DescribeReportCreationAfterStart(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(
		context.Background(),
		&resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "my-bucket"},
	)
	require.NoError(t, err)

	out := b.DescribeReportCreation(context.Background())

	require.NotNil(t, out)
	require.NotNil(t, out.Status)
	require.NotNil(t, out.S3Location)
	require.NotNil(t, out.StartDate)
	assert.Equal(t, "SUCCEEDED", *out.Status)
	assert.Contains(t, *out.S3Location, "my-bucket")
}

// ------------------------------------------------------------------ GetComplianceSummary ---

func TestRefinement1_GetComplianceSummaryEmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.GetComplianceSummary(context.Background(), &resourcegroupstaggingapi.GetComplianceSummaryInput{})

	require.NotNil(t, out)
	assert.NotNil(t, out.SummaryList)
	assert.Empty(t, out.SummaryList)
	assert.Nil(t, out.PaginationToken)
}

// ------------------------------------------------------------------ ListRequiredTags ---

func TestRefinement1_ListRequiredTagsEmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.ListRequiredTags(context.Background(), &resourcegroupstaggingapi.ListRequiredTagsInput{})

	require.NotNil(t, out)
	assert.NotNil(t, out.RequiredTags)
	assert.Empty(t, out.RequiredTags)
	assert.Nil(t, out.NextToken)
}

// ------------------------------------------------------------------ Handler JSON bad body ---

func TestRefinement1_HandlerBadJSON(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequestRaw(t, h, "GetResources", []byte("not-json"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// ------------------------------------------------------------------ StorageBackend interface ---

func TestRefinement1_HandlerAcceptsStorageBackend(t *testing.T) {
	t.Parallel()

	var backend resourcegroupstaggingapi.StorageBackend = newBackend(t)
	h := resourcegroupstaggingapi.NewHandler(backend)

	assert.NotNil(t, h)
}

// ------------------------------------------------------------------ JSON response shape ---

func TestRefinement1_DescribeReportCreationJSONShape(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bkt/path.csv", "2025-01-01T00:00:00Z")

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "DescribeReportCreation", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "SUCCEEDED", out["Status"])
	assert.Equal(t, "s3://bkt/path.csv", out["S3Location"])
	assert.Equal(t, "2025-01-01T00:00:00Z", out["StartDate"])
}

func TestRefinement1_GetComplianceSummaryJSONShape(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "GetComplianceSummary", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["SummaryList"].([]any)
	require.True(t, ok, "SummaryList should be a JSON array")
	assert.Empty(t, list)
}

func TestRefinement1_ListRequiredTagsJSONShape(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "ListRequiredTags", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["RequiredTags"].([]any)
	require.True(t, ok, "RequiredTags should be a JSON array")
	assert.Empty(t, list)
}
