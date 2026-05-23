package resourcegroupstaggingapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// ------------------------------------------------------------------ GetResources validation ---

func TestRefinement2_GetResources_TooManyTagFilters(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	filters := make([]resourcegroupstaggingapi.TagFilter, 51)

	for i := range filters {
		filters[i] = resourcegroupstaggingapi.TagFilter{Key: "key"}
	}

	_, err := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{TagFilters: filters})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation, "expected ErrValidation, got %v", err)
}

func TestRefinement2_GetResources_ExactlyMaxTagFilters(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	filters := make([]resourcegroupstaggingapi.TagFilter, 50)

	// Keys must be unique; AWS rejects duplicate TagFilter keys.
	for i := range filters {
		filters[i] = resourcegroupstaggingapi.TagFilter{Key: fmt.Sprintf("key%d", i)}
	}

	out, err := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{TagFilters: filters})

	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestRefinement2_GetResources_IncludeComplianceDetails(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
	})

	out, err := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{IncludeComplianceDetails: true})

	require.NoError(t, err)
	require.Len(t, out.ResourceTagMappingList, 1)
	require.NotNil(t, out.ResourceTagMappingList[0].ComplianceDetails)
	assert.True(t, out.ResourceTagMappingList[0].ComplianceDetails.ComplianceStatus)
}

func TestRefinement2_GetResources_ExcludeCompliantResources_NoEffect(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
	})

	// ExcludeCompliantResources=false → all resources returned.
	out, err := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{ExcludeCompliantResources: false})

	require.NoError(t, err)
	assert.Len(t, out.ResourceTagMappingList, 1)
}

func TestRefinement2_GetResources_NoComplianceDetailsWhenNotRequested(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
	})

	out, err := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{IncludeComplianceDetails: false})

	require.NoError(t, err)
	require.Len(t, out.ResourceTagMappingList, 1)
	assert.Nil(t, out.ResourceTagMappingList[0].ComplianceDetails)
}

// ------------------------------------------------------------------ TagResources validation ---

func TestRefinement2_TagResources_EmptyARNList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{},
		Tags:            map[string]string{"env": "test"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_TooManyARNs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arns := make([]string, 21)

	for i := range arns {
		arns[i] = "arn:aws:sqs:us-east-1:000000000000:q"
	}

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: arns,
		Tags:            map[string]string{"env": "test"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_EmptyTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            map[string]string{},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_TooManyTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	tags := make(map[string]string, 51)

	for i := range 51 {
		tags[fmt.Sprintf("key%d", i)] = "v"
	}

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            tags,
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_EmptyTagKey(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            map[string]string{"": "value"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_TagKeyTooLong(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            map[string]string{strings.Repeat("k", 129): "v"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_TagValueTooLong(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            map[string]string{"key": strings.Repeat("v", 257)},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_TagResources_ExactlyMaxARNs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arns := make([]string, 20)

	for i := range arns {
		arns[i] = "arn:aws:sqs:us-east-1:000000000000:q" + strings.Repeat("x", i+1)
	}

	// No taggers registered → all 20 fail, but no validation error.
	out, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: arns,
		Tags:            map[string]string{"env": "test"},
	})

	require.NoError(t, err)
	assert.Len(t, out.FailedResourcesMap, 20)
}

// ------------------------------------------------------------------ UntagResources validation ---

func TestRefinement2_UntagResources_EmptyARNList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.UntagResources(&resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: []string{},
		TagKeys:         []string{"env"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_UntagResources_TooManyARNs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arns := make([]string, 21)

	for i := range arns {
		arns[i] = "arn:aws:sqs:us-east-1:000000000000:q"
	}

	_, err := b.UntagResources(&resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: arns,
		TagKeys:         []string{"env"},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestRefinement2_UntagResources_EmptyTagKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.UntagResources(&resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		TagKeys:         []string{},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

// ------------------------------------------------------------------ GetTagKeys pagination ---

func TestRefinement2_GetTagKeys_Pagination(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	tags := make(map[string]string, 10)

	for _, k := range []string{"alpha", "beta", "delta", "epsilon", "gamma", "kappa", "omega", "sigma", "theta", "zeta"} {
		tags[k] = "v"
	}

	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: tags},
	})

	out := b.GetTagKeys(&resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	// All 10 keys returned; pagination token should be nil since < 100.
	assert.Len(t, out.TagKeys, 10)
	assert.Nil(t, out.PaginationToken)
}

func TestRefinement2_GetTagKeys_NilToken(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"a": "1", "b": "2"}},
	})

	out := b.GetTagKeys(&resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	assert.Equal(t, []string{"a", "b"}, out.TagKeys)
	assert.Nil(t, out.PaginationToken)
}

func TestRefinement2_GetTagKeys_TokenResumption(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Exactly 3 keys; we'll use paginateStrings via GetTagKeys with big page, no token.
	// To test token resumption we need >100 keys. Test simpler case with token set to middle.
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"aa": "1", "bb": "2", "cc": "3"}},
	})

	// Passing token = "bb" should start after "bb", returning ["cc"].
	tok := "bb"
	out := b.GetTagKeys(&resourcegroupstaggingapi.GetTagKeysInput{PaginationToken: &tok})

	require.NotNil(t, out)
	assert.Equal(t, []string{"cc"}, out.TagKeys)
	assert.Nil(t, out.PaginationToken)
}

// ------------------------------------------------------------------ GetTagValues pagination ---

func TestRefinement2_GetTagValues_NilKey(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
	})

	// Key is nil → must return empty list, not panic.
	out := b.GetTagValues(&resourcegroupstaggingapi.GetTagValuesInput{})

	require.NotNil(t, out)
	assert.Empty(t, out.TagValues)
}

func TestRefinement2_GetTagValues_TokenResumption(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev"}},
		{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:3", ResourceType: "sqs:queue", Tags: map[string]string{"env": "staging"}},
	})

	tok := "prod"
	key := "env"
	out := b.GetTagValues(&resourcegroupstaggingapi.GetTagValuesInput{Key: &key, PaginationToken: &tok})

	require.NotNil(t, out)
	assert.Equal(t, []string{"staging"}, out.TagValues)
}

// ------------------------------------------------------------------ handler error mapping ---

func TestRefinement2_Handler_ValidationError_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))

	// TagResources with empty ARN list → ErrValidation → 400.
	rec := doTaggingRequest(t, h, "TagResources", map[string]any{
		"ResourceARNList": []string{},
		"Tags":            map[string]string{"k": "v"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_Handler_GetResources_TooManyTagFilters_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	filters := make([]map[string]any, 51)

	for i := range filters {
		filters[i] = map[string]any{"Key": "k"}
	}

	rec := doTaggingRequest(t, h, "GetResources", map[string]any{"TagFilters": filters})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_Handler_UntagResources_EmptyTagKeys_Returns400(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))

	rec := doTaggingRequest(t, h, "UntagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		"TagKeys":         []string{},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ------------------------------------------------------------------ Snapshot/Restore ---

func TestRefinement2_SnapshotRestore_PreservesReportState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bucket/key.csv", "2025-06-01T00:00:00Z")

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := newBackend(t)
	require.NoError(t, b2.Restore(snap))
	assert.True(t, resourcegroupstaggingapi.HasReportState(b2))
	assert.Equal(t, "SUCCEEDED", resourcegroupstaggingapi.ReportStatus(b2))
	assert.Equal(t, "s3://bucket/key.csv", resourcegroupstaggingapi.ReportS3Location(b2))
}

func TestRefinement2_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := newBackend(t)
	require.NoError(t, b2.Restore(snap))
	assert.False(t, resourcegroupstaggingapi.HasReportState(b2))
}

func TestRefinement2_SnapshotRestore_ClearsProviders(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource { return nil })

	snap := b.Snapshot()

	b2 := newBackend(t)
	b2.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource { return nil })
	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b2))

	require.NoError(t, b2.Restore(snap))
	// Restore clears providers (they are runtime callbacks, cannot be serialized).
	assert.Equal(t, 0, resourcegroupstaggingapi.ProviderCount(b2))
}

func TestRefinement2_Restore_BadJSON(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	err := b.Restore([]byte("not-json"))
	require.Error(t, err)
}

// ------------------------------------------------------------------ paginateStrings ---

func TestRefinement2_GetTagKeys_Empty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out := b.GetTagKeys(&resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	assert.NotNil(t, out.TagKeys, "TagKeys must be non-nil empty slice")
	assert.Empty(t, out.TagKeys)
}

func TestRefinement2_GetTagValues_Empty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	key := "env"

	out := b.GetTagValues(&resourcegroupstaggingapi.GetTagValuesInput{Key: &key})

	require.NotNil(t, out)
	assert.NotNil(t, out.TagValues)
	assert.Empty(t, out.TagValues)
}

// ------------------------------------------------------------------ handler JSON error body ---

func TestRefinement2_Handler_ValidationError_HasValidationExceptionBody(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequest(t, h, "TagResources", map[string]any{
		"ResourceARNList": []string{},
		"Tags":            map[string]string{"k": "v"},
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, "ValidationException", body["__type"])
}

func TestRefinement2_Handler_GetResources_IncludeComplianceDetails(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"k": "v"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{"IncludeComplianceDetails": true})

	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		ResourceTagMappingList []struct {
			ComplianceDetails *struct {
				ComplianceStatus bool `json:"ComplianceStatus"`
			} `json:"ComplianceDetails"`
		} `json:"ResourceTagMappingList"`
	}

	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Len(t, body.ResourceTagMappingList, 1)
	require.NotNil(t, body.ResourceTagMappingList[0].ComplianceDetails)
	assert.True(t, body.ResourceTagMappingList[0].ComplianceDetails.ComplianceStatus)
}

func TestRefinement2_AccountID_And_Region_Preserved(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("111122223333", "eu-central-1")

	assert.Equal(t, "111122223333", b.AccountID())
	assert.Equal(t, "eu-central-1", b.Region())
}

func TestRefinement2_TagResources_DeepCopy_NoMutation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	var received map[string]string

	b.RegisterARNTagger(func(_ string, tags map[string]string) (bool, error) {
		received = tags

		return true, nil
	})

	original := map[string]string{"env": "test"}

	out, err := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		Tags:            original,
	})

	require.NoError(t, err)
	require.NotNil(t, out)

	// Mutate received inside tagger shouldn't affect original.
	received["extra"] = "modified"
	assert.NotContains(t, original, "extra", "original tags map must not be mutated by tagger")
}

func TestRefinement2_Reset_OnlyClears_ReportState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource { return nil })
	b.RegisterARNTagger(func(_ string, _ map[string]string) (bool, error) { return false, nil })
	resourcegroupstaggingapi.AddReportStateInternal(b, "RUNNING", "", "")

	b.Reset()

	assert.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b), "providers must survive Reset")
	assert.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b), "taggers must survive Reset")
	assert.False(t, resourcegroupstaggingapi.HasReportState(b), "reportState must be cleared")
}
