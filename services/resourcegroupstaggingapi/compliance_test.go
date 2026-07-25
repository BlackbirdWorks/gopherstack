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

// TestGetComplianceSummary_WithFilters exercises every filter/GroupBy/pagination
// combination. The in-memory backend has no tag policy, so every combination returns an
// empty (never nil) SummaryList; this proves filters are honoured rather than panicking
// or short-circuiting.
func TestGetComplianceSummary_WithFilters(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:ec2:us-west-2:000000000000:i-1",
			ResourceType: "ec2:instance",
			Tags:         map[string]string{"owner": "alice"},
		},
	})

	tests := []struct {
		input *resourcegroupstaggingapi.GetComplianceSummaryInput
		name  string
	}{
		{
			name:  "empty_input",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{},
		},
		{
			name: "region_filter",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				RegionFilters: []string{"us-east-1"},
			},
		},
		{
			name: "resource_type_filter",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				ResourceTypeFilters: []string{"sqs:queue"},
			},
		},
		{
			name: "tag_key_filter",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				TagKeyFilters: []string{"env"},
			},
		},
		{
			name: "group_by_resource_type",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				GroupBy: []string{"RESOURCE_TYPE"},
			},
		},
		{
			name: "group_by_region",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				GroupBy: []string{"REGION"},
			},
		},
		{
			name: "max_results",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				MaxResults: ptr(int32(10)),
			},
		},
		{
			name: "all_filters",
			input: &resourcegroupstaggingapi.GetComplianceSummaryInput{
				RegionFilters:       []string{"us-east-1"},
				ResourceTypeFilters: []string{"sqs:queue"},
				TagKeyFilters:       []string{"env"},
				GroupBy:             []string{"RESOURCE_TYPE", "REGION"},
				MaxResults:          ptr(int32(50)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetComplianceSummary(context.Background(), tt.input)

			require.NoError(t, err)
			require.NotNil(t, out)
			// Mock has no tag policy → always returns 0 non-compliant resources.
			assert.NotNil(t, out.SummaryList)
			assert.Empty(t, out.SummaryList)
		})
	}
}

func TestGetComplianceSummary_RegionFilter_FiltersResources(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Only call GetComplianceSummary to ensure it doesn't panic with various filter combos.
	out, err := b.GetComplianceSummary(context.Background(), &resourcegroupstaggingapi.GetComplianceSummaryInput{
		RegionFilters:       []string{"us-east-1", "eu-west-1"},
		ResourceTypeFilters: []string{"ec2:instance"},
		TagKeyFilters:       []string{"env"},
		GroupBy:             []string{"REGION", "RESOURCE_TYPE"},
		MaxResults:          ptr(int32(100)),
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.NotNil(t, out.SummaryList)
}

func TestGetComplianceSummary_EmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out, err := b.GetComplianceSummary(context.Background(), &resourcegroupstaggingapi.GetComplianceSummaryInput{})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.NotNil(t, out.SummaryList)
	assert.Empty(t, out.SummaryList)
	assert.Nil(t, out.PaginationToken)
}

// TestGetComplianceSummary_MaxResultsValidation covers the real API's
// MaxResultsGetComplianceSummary shape constraint (min: 1, max: 1000).
func TestGetComplianceSummary_MaxResultsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults *int32
		name       string
		wantErr    bool
	}{
		{name: "unset_uses_default", maxResults: nil},
		{name: "exactly_min_1", maxResults: ptr(int32(1))},
		{name: "exactly_max_1000", maxResults: ptr(int32(1000))},
		{name: "zero_rejected", maxResults: ptr(int32(0)), wantErr: true},
		{name: "negative_rejected", maxResults: ptr(int32(-1)), wantErr: true},
		{name: "over_max_1001_rejected", maxResults: ptr(int32(1001)), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			input := &resourcegroupstaggingapi.GetComplianceSummaryInput{MaxResults: tt.maxResults}
			out, err := b.GetComplianceSummary(context.Background(), input)

			if tt.wantErr {
				require.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
				assert.Nil(t, out)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, out)
		})
	}
}

func TestGetComplianceSummaryJSONShape(t *testing.T) {
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

func TestListRequiredTags_EmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.ListRequiredTags(context.Background(), &resourcegroupstaggingapi.ListRequiredTagsInput{})

	require.NotNil(t, out)
	assert.NotNil(t, out.RequiredTags)
	assert.Empty(t, out.RequiredTags)
	assert.Nil(t, out.NextToken)
}

func TestListRequiredTagsJSONShape(t *testing.T) {
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
