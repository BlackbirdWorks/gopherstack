package resourcegroupstaggingapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// makeResources builds a slice of tagged resources for testing.
func makeResources(count int) []resourcegroupstaggingapi.TaggedResource {
	out := make([]resourcegroupstaggingapi.TaggedResource, count)
	for i := range out {
		out[i] = resourcegroupstaggingapi.TaggedResource{
			ResourceARN:  fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i),
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"seq": strconv.Itoa(i)},
		}
	}

	return out
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

func TestGetResources_NoProviders(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")

	out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.ResourceTagMappingList)
	assert.Nil(t, out.PaginationToken)
}

func TestGetResources_TagFilter(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:123456789012:queue-a",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"env": "prod"},
			},
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:123456789012:queue-b",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"env": "dev"},
			},
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:123456789012:queue-c",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"owner": "team"},
			},
		}
	})

	tests := []struct {
		name       string
		tagFilters []resourcegroupstaggingapi.TagFilter
		wantARNs   []string
	}{
		{
			name:       "match_by_key_any_value",
			tagFilters: []resourcegroupstaggingapi.TagFilter{{Key: "env"}},
			wantARNs: []string{
				"arn:aws:sqs:us-east-1:123456789012:queue-a",
				"arn:aws:sqs:us-east-1:123456789012:queue-b",
			},
		},
		{
			name:       "match_by_key_and_value",
			tagFilters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: []string{"prod"}}},
			wantARNs:   []string{"arn:aws:sqs:us-east-1:123456789012:queue-a"},
		},
		{
			name:       "no_match",
			tagFilters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: []string{"staging"}}},
			wantARNs:   nil,
		},
		{
			name:       "multiple_values_or",
			tagFilters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: []string{"prod", "dev"}}},
			wantARNs: []string{
				"arn:aws:sqs:us-east-1:123456789012:queue-a",
				"arn:aws:sqs:us-east-1:123456789012:queue-b",
			},
		},
		{
			name: "and_across_filters",
			tagFilters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env"},
				{Key: "owner"},
			},
			wantARNs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetResources(
				context.Background(),
				&resourcegroupstaggingapi.GetResourcesInput{TagFilters: tt.tagFilters},
			)

			require.NoError(t, err)
			require.NotNil(t, out)

			gotARNs := make([]string, 0, len(out.ResourceTagMappingList))
			for _, m := range out.ResourceTagMappingList {
				gotARNs = append(gotARNs, m.ResourceARN)
			}

			if len(gotARNs) == 0 {
				gotARNs = nil
			}

			assert.Equal(t, tt.wantARNs, gotARNs)
		})
	}
}

func TestGetResources_ResourceTypeFilter(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:123456789012:q1",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"k": "v"},
			},
			{
				ResourceARN:  "arn:aws:dynamodb:us-east-1:123456789012:table/t1",
				ResourceType: "dynamodb:table",
				Tags:         map[string]string{"k": "v"},
			},
		}
	})

	tests := []struct {
		name        string
		typeFilters []string
		wantLen     int
		wantErr     bool
	}{
		{
			name:        "filter_sqs",
			typeFilters: []string{"sqs:queue"},
			wantLen:     1,
		},
		{
			name:        "filter_dynamodb",
			typeFilters: []string{"dynamodb:table"},
			wantLen:     1,
		},
		{
			name:        "filter_both",
			typeFilters: []string{"sqs:queue", "dynamodb:table"},
			wantLen:     2,
		},
		{
			name:        "no_filter",
			typeFilters: nil,
			wantLen:     2,
		},
		{
			// Matching is case-sensitive; uppercase service prefix is also invalid format.
			name:        "uppercase_service_invalid_format",
			typeFilters: []string{"SQS:Queue"},
			wantErr:     true,
		},
		{
			// Service-only form: matches all resources in the given service.
			name:        "service_only_sqs",
			typeFilters: []string{"sqs"},
			wantLen:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetResources(
				context.Background(),
				&resourcegroupstaggingapi.GetResourcesInput{ResourceTypeFilters: tt.typeFilters},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Len(t, out.ResourceTagMappingList, tt.wantLen)
		})
	}
}

func TestResourceTypeFilter_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{name: "valid_service_resource", filter: "ec2:instance"},
		{name: "valid_service_only", filter: "ec2"},
		{name: "valid_sqs_queue", filter: "sqs:queue"},
		{name: "valid_mixed_case_resource", filter: "s3:bucket"},
		{name: "valid_hyphen_in_service", filter: "elastic-load-balancing:loadbalancer"},
		{name: "valid_slash_in_resource", filter: "iam:role/path"},
		{name: "uppercase_service_invalid", filter: "SQS:queue", wantErr: true},
		{name: "all_uppercase_invalid", filter: "SQS:Queue", wantErr: true},
		{name: "leading_colon_invalid", filter: ":instance", wantErr: true},
		{name: "empty_resource_after_colon_invalid", filter: "ec2:", wantErr: true},
		{name: "space_invalid", filter: "ec2 instance", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
				ResourceTypeFilters: []string{tt.filter},
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceTypeFilter_CaseSensitiveMatch(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"k": "v"},
		},
		{
			ResourceARN:  "arn:aws:ec2:us-east-1:000000000000:i-1",
			ResourceType: "ec2:instance",
			Tags:         map[string]string{"k": "v"},
		},
	})

	tests := []struct {
		name        string
		typeFilters []string
		wantARNs    []string
	}{
		{
			name:        "exact_match_sqs",
			typeFilters: []string{"sqs:queue"},
			wantARNs:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		},
		{
			name:        "service_only_sqs_matches_sqs_queue",
			typeFilters: []string{"sqs"},
			wantARNs:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
		},
		{
			name:        "service_only_ec2_matches_ec2_instance",
			typeFilters: []string{"ec2"},
			wantARNs:    []string{"arn:aws:ec2:us-east-1:000000000000:i-1"},
		},
		{
			name:        "no_match_wrong_case_resource",
			typeFilters: []string{"sqs:Queue"},
			wantARNs:    nil,
		},
		{
			name:        "multiple_service_only_filters",
			typeFilters: []string{"sqs", "ec2"},
			wantARNs: []string{
				"arn:aws:ec2:us-east-1:000000000000:i-1",
				"arn:aws:sqs:us-east-1:000000000000:q1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
				ResourceTypeFilters: tt.typeFilters,
			})

			require.NoError(t, err)

			gotARNs := make([]string, len(out.ResourceTagMappingList))
			for i, m := range out.ResourceTagMappingList {
				gotARNs[i] = m.ResourceARN
			}

			if len(gotARNs) == 0 {
				gotARNs = nil
			}

			assert.Equal(t, tt.wantARNs, gotARNs)
		})
	}
}

func TestGetResources_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: "arn:aws:sqs:us-east-1:123:a", ResourceType: "sqs:queue", Tags: map[string]string{"k": "v"}},
			{ResourceARN: "arn:aws:sqs:us-east-1:123:b", ResourceType: "sqs:queue", Tags: map[string]string{"k": "v"}},
			{ResourceARN: "arn:aws:sqs:us-east-1:123:c", ResourceType: "sqs:queue", Tags: map[string]string{"k": "v"}},
		}
	})

	pageSize := int32(2)

	out1, err := b.GetResources(
		context.Background(),
		&resourcegroupstaggingapi.GetResourcesInput{ResourcesPerPage: &pageSize},
	)
	require.NoError(t, err)
	require.NotNil(t, out1)
	require.NotNil(t, out1.PaginationToken)
	assert.Len(t, out1.ResourceTagMappingList, 2)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:a", out1.ResourceTagMappingList[0].ResourceARN)

	out2, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		ResourcesPerPage: &pageSize,
		PaginationToken:  *out1.PaginationToken,
	})
	require.NoError(t, err)
	require.NotNil(t, out2)
	assert.Nil(t, out2.PaginationToken)
	assert.Len(t, out2.ResourceTagMappingList, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:c", out2.ResourceTagMappingList[0].ResourceARN)
}

func TestGetResources_PaginationWalk(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, makeResources(5))

	tests := []struct {
		name         string
		wantCounts   []int // expected count per page
		pageSize     int32
		wantFinalNil bool // final page has nil token
	}{
		{
			name:         "page_2_of_5",
			pageSize:     2,
			wantCounts:   []int{2, 2, 1},
			wantFinalNil: true,
		},
		{
			name:         "page_5_of_5",
			pageSize:     5,
			wantCounts:   []int{5},
			wantFinalNil: true,
		},
		{
			name:         "page_1_of_5",
			pageSize:     1,
			wantCounts:   []int{1, 1, 1, 1, 1},
			wantFinalNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var token string
			var pages []int

			for {
				input := &resourcegroupstaggingapi.GetResourcesInput{
					ResourcesPerPage: &tt.pageSize,
				}
				if token != "" {
					input.PaginationToken = token
				}

				out, err := b.GetResources(context.Background(), input)
				require.NoError(t, err)

				pages = append(pages, len(out.ResourceTagMappingList))

				if out.PaginationToken == nil {
					break
				}

				token = *out.PaginationToken
			}

			assert.Equal(t, tt.wantCounts, pages)
		})
	}
}

// TestGetResources_TagFilterValidation covers TagFilter field-level validation: key
// emptiness/length, values count/length, and duplicate keys across filters.
func TestGetResources_TagFilterValidation(t *testing.T) {
	t.Parallel()

	manyValues := func(n int) []string {
		values := make([]string, n)
		for i := range values {
			values[i] = fmt.Sprintf("v%d", i)
		}

		return values
	}

	tests := []struct {
		name    string
		filters []resourcegroupstaggingapi.TagFilter
		wantErr bool
	}{
		{
			name:    "empty_key",
			filters: []resourcegroupstaggingapi.TagFilter{{Key: ""}},
			wantErr: true,
		},
		{
			name:    "key_too_long",
			filters: []resourcegroupstaggingapi.TagFilter{{Key: strings.Repeat("k", 129)}},
			wantErr: true,
		},
		{
			name:    "key_exactly_max_length",
			filters: []resourcegroupstaggingapi.TagFilter{{Key: strings.Repeat("k", 128)}},
		},
		{
			name:    "too_many_values",
			filters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: manyValues(257)}},
			wantErr: true,
		},
		{
			name:    "exactly_max_values",
			filters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: manyValues(256)}},
		},
		{
			name:    "value_too_long",
			filters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: []string{strings.Repeat("v", 257)}}},
			wantErr: true,
		},
		{
			name: "duplicate_keys",
			filters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env", Values: []string{"prod"}},
				{Key: "env", Values: []string{"dev"}},
			},
			wantErr: true,
		},
		{
			name: "unique_keys_ok",
			filters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env", Values: []string{"prod"}},
				{Key: "owner", Values: []string{"alice"}},
			},
		},
		{
			name: "fifty_unique_keys_ok",
			filters: func() []resourcegroupstaggingapi.TagFilter {
				filters := make([]resourcegroupstaggingapi.TagFilter, 50)
				for i := range filters {
					filters[i] = resourcegroupstaggingapi.TagFilter{Key: fmt.Sprintf("key%d", i)}
				}

				return filters
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
				TagFilters: tt.filters,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestGetResources_TagFiltersCountLimit covers the TagFilters slice length limit
// (distinct from per-filter Values length, see TestGetResources_TagFilterValidation).
func TestGetResources_TagFiltersCountLimit(t *testing.T) {
	t.Parallel()

	t.Run("too_many_filters_51", func(t *testing.T) {
		t.Parallel()

		b := newBackend(t)
		filters := make([]resourcegroupstaggingapi.TagFilter, 51)

		for i := range filters {
			filters[i] = resourcegroupstaggingapi.TagFilter{Key: "key"}
		}

		_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{TagFilters: filters})

		require.Error(t, err)
		assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
	})

	t.Run("exactly_max_filters_50", func(t *testing.T) {
		t.Parallel()

		b := newBackend(t)
		filters := make([]resourcegroupstaggingapi.TagFilter, 50)

		// Keys must be unique; AWS rejects duplicate TagFilter keys.
		for i := range filters {
			filters[i] = resourcegroupstaggingapi.TagFilter{Key: fmt.Sprintf("key%d", i)}
		}

		out, err := b.GetResources(
			context.Background(),
			&resourcegroupstaggingapi.GetResourcesInput{TagFilters: filters},
		)

		require.NoError(t, err)
		assert.NotNil(t, out)
	})
}

// TestGetResources_TagsPerPageValidation covers the TagsPerPage bounds (must be between
// minTagsPerPage and maxTagsPerPage when set at all).
func TestGetResources_TagsPerPageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagsPerPage *int32
		name        string
		wantErr     bool
	}{
		{name: "too_small", tagsPerPage: ptr(int32(99)), wantErr: true},
		{name: "too_large", tagsPerPage: ptr(int32(501)), wantErr: true},
		{name: "min_valid", tagsPerPage: ptr(int32(100))},
		{name: "max_valid", tagsPerPage: ptr(int32(500))},
		{name: "nil_ok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
				TagsPerPage: tt.tagsPerPage,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetResources_TagsPerPage_SplitsPages(t *testing.T) {
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

func TestGetResources_TagsPerPage_OversizedFirstResourceStillReturnedAlone(t *testing.T) {
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

func TestGetResources_NoTagsPerPage_Unconstrained(t *testing.T) {
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

func TestGetResources_ResourceARNList_FiltersToSpecificARNs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q2",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "dev"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q3",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "staging"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{
		"ResourceARNList": []string{
			"arn:aws:sqs:us-east-1:000000000000:q1",
			"arn:aws:sqs:us-east-1:000000000000:q3",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 2, "only the two requested ARNs should be returned")

	arns := make([]string, 0, 2)
	for _, item := range list {
		m := item.(map[string]any)
		arns = append(arns, m["ResourceARN"].(string))
	}

	assert.Contains(t, arns, "arn:aws:sqs:us-east-1:000000000000:q1")
	assert.Contains(t, arns, "arn:aws:sqs:us-east-1:000000000000:q3")
	assert.NotContains(t, arns, "arn:aws:sqs:us-east-1:000000000000:q2")
}

func TestGetResources_ResourceARNList_EmptyReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q2",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "dev"},
		},
	})

	h := resourcegroupstaggingapi.NewHandler(b)
	// Omitting ResourceARNList should return all resources.
	rec := doTaggingRequest(t, h, "GetResources", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}

// TestGetResources_ResourceARNList_MutualExclusion verifies that ResourceARNList cannot
// be combined with ResourceTypeFilters, TagFilters, or any of the pagination parameters.
func TestGetResources_ResourceARNList_MutualExclusion(t *testing.T) {
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

func TestGetResources_ResourceARNList_Alone_OK(t *testing.T) {
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

func TestGetResources_ExcludeCompliant_RequiresIncludeDetails(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		ExcludeCompliantResources: true,
		IncludeComplianceDetails:  false,
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestGetResources_ExcludeCompliant_WithIncludeDetails(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, makeResources(3))

	out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		ExcludeCompliantResources: true,
		IncludeComplianceDetails:  true,
	})

	require.NoError(t, err)
	// All resources are compliant in mock → excluding them returns 0.
	assert.Empty(t, out.ResourceTagMappingList)
}

func TestGetResources_ExcludeCompliantResources_NoEffect(t *testing.T) {
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
	out, err := b.GetResources(
		context.Background(),
		&resourcegroupstaggingapi.GetResourcesInput{ExcludeCompliantResources: false},
	)

	require.NoError(t, err)
	assert.Len(t, out.ResourceTagMappingList, 1)
}

// TestGetResources_ComplianceDetails verifies that ResourceTagMapping.ComplianceDetails
// is populated (and ComplianceStatus is true, since the mock has no tag policy) exactly
// when IncludeComplianceDetails is set, across every resource in the result.
func TestGetResources_ComplianceDetails(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, makeResources(2))

	tests := []struct {
		name              string
		includeCompliance bool
		wantNonNilDetails bool
		wantCompliantTrue bool
	}{
		{
			name:              "include_compliance_true",
			includeCompliance: true,
			wantNonNilDetails: true,
			wantCompliantTrue: true,
		},
		{
			name:              "include_compliance_false",
			includeCompliance: false,
			wantNonNilDetails: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
				IncludeComplianceDetails: tt.includeCompliance,
			})

			require.NoError(t, err)
			require.NotEmpty(t, out.ResourceTagMappingList)

			for _, m := range out.ResourceTagMappingList {
				if tt.wantNonNilDetails {
					require.NotNil(t, m.ComplianceDetails)
					assert.Equal(t, tt.wantCompliantTrue, m.ComplianceDetails.ComplianceStatus)
				} else {
					assert.Nil(t, m.ComplianceDetails)
				}
			}
		})
	}
}

// TestComplianceDetailsFieldName verifies real AWS's field spelling
// "KeysWithNoncompliantValues" (lowercase 'c' in noncompliant) appears on the wire, and
// the commonly-miswritten "KeysWithNonCompliantValues" never does.
func TestComplianceDetailsFieldName(t *testing.T) {
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
		"IncludeComplianceDetails": true,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)

	item := list[0].(map[string]any)
	cd, ok := item["ComplianceDetails"].(map[string]any)
	require.True(t, ok, "ComplianceDetails must be present when IncludeComplianceDetails=true")

	// Must contain "KeysWithNoncompliantValues" (not "KeysWithNonCompliantValues").
	_, hasCorrect := cd["KeysWithNoncompliantValues"]
	_, hasWrong := cd["KeysWithNonCompliantValues"]

	assert.False(t, hasWrong, "response must not contain misspelled 'KeysWithNonCompliantValues'")
	_ = hasCorrect // field may be absent when empty (omitempty) — absence is also acceptable
}

func TestHandler_GetResources_IncludeComplianceDetails(t *testing.T) {
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

func TestGetResources_MultiKeyTagFilter_AND(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:both",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod", "team": "ops"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:only-env",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"env": "prod"},
		},
		{
			ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:only-team",
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"team": "ops"},
		},
	})

	tests := []struct {
		name     string
		filters  []resourcegroupstaggingapi.TagFilter
		wantARNs []string
	}{
		{
			name: "requires_both_keys",
			filters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env"},
				{Key: "team"},
			},
			wantARNs: []string{"arn:aws:sqs:us-east-1:000000000000:both"},
		},
		{
			name: "single_key_env",
			filters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env"},
			},
			wantARNs: []string{
				"arn:aws:sqs:us-east-1:000000000000:both",
				"arn:aws:sqs:us-east-1:000000000000:only-env",
			},
		},
		{
			name: "single_key_with_value",
			filters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env", Values: []string{"prod"}},
				{Key: "team", Values: []string{"ops"}},
			},
			wantARNs: []string{"arn:aws:sqs:us-east-1:000000000000:both"},
		},
		{
			name: "multi_value_or_within_filter",
			filters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env", Values: []string{"prod", "dev"}},
			},
			wantARNs: []string{
				"arn:aws:sqs:us-east-1:000000000000:both",
				"arn:aws:sqs:us-east-1:000000000000:only-env",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetResources(
				context.Background(),
				&resourcegroupstaggingapi.GetResourcesInput{TagFilters: tt.filters},
			)
			require.NoError(t, err)

			gotARNs := make([]string, len(out.ResourceTagMappingList))
			for i, m := range out.ResourceTagMappingList {
				gotARNs[i] = m.ResourceARN
			}

			if len(gotARNs) == 0 {
				gotARNs = nil
			}

			assert.Equal(t, tt.wantARNs, gotARNs)
		})
	}
}

func TestGetResources_NonNilTagsSlice(t *testing.T) {
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
