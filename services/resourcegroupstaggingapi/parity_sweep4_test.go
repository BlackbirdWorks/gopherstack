package resourcegroupstaggingapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// ======================================================================
// Wire error code: resourcegroupstaggingapi has no ValidationException shape.
// ======================================================================

func TestParitySweep4_ValidationFailures_UseInvalidParameterException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "GetResources_duplicate_tag_filter_keys",
			op:   "GetResources",
			body: map[string]any{
				"TagFilters": []map[string]any{{"Key": "env"}, {"Key": "env"}},
			},
		},
		{
			name: "TagResources_empty_arn_list",
			op:   "TagResources",
			body: map[string]any{
				"ResourceARNList": []string{},
				"Tags":            map[string]string{"k": "v"},
			},
		},
		{
			name: "UntagResources_empty_tag_keys",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         []string{},
			},
		},
		{
			name: "StartReportCreation_missing_bucket",
			op:   "StartReportCreation",
			body: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			rec := doTaggingRequest(t, h, tt.op, tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())

			var body map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
			assert.Equal(t, "InvalidParameterException", body["__type"],
				"real resourcegroupstaggingapi has no ValidationException shape")
			assert.NotContains(t, rec.Body.String(), "ValidationException")
		})
	}
}

func TestParitySweep4_MalformedBody_UsesSerializationException(t *testing.T) {
	t.Parallel()

	h := resourcegroupstaggingapi.NewHandler(newBackend(t))
	rec := doTaggingRequestRaw(t, h, "GetResources", []byte("{not-json"))

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, "SerializationException", body["__type"])
}

// ======================================================================
// GetResources: ResourceARNList is mutually exclusive with ResourceTypeFilters,
// TagFilters, and the pagination parameters.
// ======================================================================

func TestParitySweep4_GetResources_ResourceARNList_MutualExclusion(t *testing.T) {
	t.Parallel()

	perPage := int32(10)
	tagsPerPage := int32(100)

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "with_resource_type_filters",
			body: map[string]any{
				"ResourceARNList":     []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"ResourceTypeFilters": []string{"sqs:queue"},
			},
		},
		{
			name: "with_tag_filters",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagFilters":      []map[string]any{{"Key": "env"}},
			},
		},
		{
			name: "with_resources_per_page",
			body: map[string]any{
				"ResourceARNList":  []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"ResourcesPerPage": perPage,
			},
		},
		{
			name: "with_tags_per_page",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagsPerPage":     tagsPerPage,
			},
		},
		{
			name: "with_pagination_token",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"PaginationToken": "some-token",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(newBackend(t))
			rec := doTaggingRequest(t, h, "GetResources", tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			assert.Contains(t, rec.Body.String(), "InvalidParameterException")
		})
	}
}

func TestParitySweep4_GetResources_ResourceARNList_Alone_OK(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
	})

	assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
}

// ======================================================================
// GetResources: TagsPerPage actually constrains page splits (previously
// validated but silently discarded).
// ======================================================================

func TestParitySweep4_GetResources_TagsPerPage_SplitsPages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	resources := make([]resourcegroupstaggingapi.TaggedResource, 0, 5)
	for i := range 5 {
		resources = append(resources, resourcegroupstaggingapi.TaggedResource{
			ResourceARN:  fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i),
			ResourceType: "sqs:queue",
			// Each resource carries 40 tags; TagsPerPage=100 should therefore only fit
			// two resources (80 tags) per page before rolling over.
			Tags: manyTags(t, 40),
		})
	}

	seedResources(b, resources)

	h := resourcegroupstaggingapi.NewHandler(b)
	tagsPerPage := int32(100)

	rec := doTaggingRequest(t, h, "GetResources", map[string]any{"TagsPerPage": tagsPerPage})
	require.Equal(t, http.StatusOK, rec.Code)

	var out resourcegroupstaggingapi.GetResourcesOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	require.NotNil(t, out.PaginationToken, "TagsPerPage must force an early page break")
	assert.LessOrEqual(t, len(out.ResourceTagMappingList), 2,
		"page must not exceed TagsPerPage's cumulative tag-count cap")
}

func TestParitySweep4_GetResources_TagsPerPage_OversizedFirstResourceStillReturnedAlone(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:big",
			ResourceType: "sqs:queue",
			Tags:         manyTags(t, 200),
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:small",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	tagsPerPage := int32(100)

	rec := doTaggingRequest(t, h, "GetResources", map[string]any{"TagsPerPage": tagsPerPage})
	require.Equal(t, http.StatusOK, rec.Code)

	var out resourcegroupstaggingapi.GetResourcesOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	// GetResources never splits a single resource and its tags across pages, so the
	// oversized resource is still returned alone rather than yielding zero results.
	require.Len(t, out.ResourceTagMappingList, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:big", out.ResourceTagMappingList[0].ResourceARN)
	require.NotNil(t, out.PaginationToken)
}

func TestParitySweep4_GetResources_NoTagsPerPage_Unconstrained(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	resources := make([]resourcegroupstaggingapi.TaggedResource, 0, 5)

	for i := range 5 {
		resources = append(resources, resourcegroupstaggingapi.TaggedResource{
			ResourceARN:  fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i),
			ResourceType: "sqs:queue",
			Tags:         manyTags(t, 40),
		})
	}

	seedResources(b, resources)

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out resourcegroupstaggingapi.GetResourcesOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	assert.Len(t, out.ResourceTagMappingList, 5, "without TagsPerPage all resources fit on one page")
	assert.Nil(t, out.PaginationToken)
}

// manyTags returns a map with n distinct tag keys/values, used to force TagsPerPage
// page splits deterministically.
func manyTags(t *testing.T, n int) map[string]string {
	t.Helper()

	tags := make(map[string]string, n)
	for i := range n {
		tags[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}

	return tags
}

// ======================================================================
// StartReportCreation: S3BucketRegion is not part of the real AWS API and must
// not be modeled as an input field.
// ======================================================================

func TestParitySweep4_StartReportCreationInput_HasNoS3BucketRegionField(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
		S3Bucket: "cross-region-bucket",
	})

	require.NoError(t, err)
	assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))
	assert.Contains(t, resourcegroupstaggingapi.ReportS3Location(b), "cross-region-bucket")

	// A request body containing S3BucketRegion (as some non-AWS client might send) must
	// simply be ignored as an unknown field, not silently accepted as a modeled member.
	marshaled, err := json.Marshal(resourcegroupstaggingapi.StartReportCreationInput{S3Bucket: "b"})
	require.NoError(t, err)
	assert.NotContains(t, string(marshaled), "S3BucketRegion")
}
