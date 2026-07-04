package resourcegroupstaggingapi_test

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// ------------------------------------------------------------------ helpers ---

func ptr[T any](v T) *T {
	p := new(T)
	*p = v

	return p
}

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

// ======================================================================
// validateGetResourcesInput
// ======================================================================

func TestAudit1_GetResources_ExcludeCompliant_RequiresIncludeDetails(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		ExcludeCompliantResources: true,
		IncludeComplianceDetails:  false,
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_ExcludeCompliant_WithIncludeDetails(t *testing.T) {
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

func TestAudit1_GetResources_TagFilter_EmptyKey(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{{Key: ""}},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagFilter_KeyTooLong(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: strings.Repeat("k", 129)},
		},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagFilter_KeyExactlyMaxLength(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: strings.Repeat("k", 128)},
		},
	})

	require.NoError(t, err)
}

func TestAudit1_GetResources_TagFilter_TooManyValues(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	values := make([]string, 257)
	for i := range values {
		values[i] = fmt.Sprintf("v%d", i)
	}

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: "env", Values: values},
		},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagFilter_ExactlyMaxValues(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	values := make([]string, 256)
	for i := range values {
		values[i] = fmt.Sprintf("v%d", i)
	}

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: "env", Values: values},
		},
	})

	require.NoError(t, err)
}

func TestAudit1_GetResources_TagFilter_ValueTooLong(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: "env", Values: []string{strings.Repeat("v", 257)}},
		},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagFilter_DuplicateKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: "env", Values: []string{"prod"}},
			{Key: "env", Values: []string{"dev"}},
		},
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagFilter_UniqueKeys_OK(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []resourcegroupstaggingapi.TagFilter{
			{Key: "env", Values: []string{"prod"}},
			{Key: "owner", Values: []string{"alice"}},
		},
	})

	require.NoError(t, err)
}

func TestAudit1_GetResources_TagFilter_50Unique_OK(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	filters := make([]resourcegroupstaggingapi.TagFilter, 50)
	for i := range filters {
		filters[i] = resourcegroupstaggingapi.TagFilter{Key: fmt.Sprintf("key%d", i)}
	}

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{TagFilters: filters})

	require.NoError(t, err)
}

// ======================================================================
// TagsPerPage validation
// ======================================================================

func TestAudit1_GetResources_TagsPerPage_TooSmall(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagsPerPage: ptr(int32(99)),
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagsPerPage_TooLarge(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagsPerPage: ptr(int32(501)),
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
}

func TestAudit1_GetResources_TagsPerPage_MinValid(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagsPerPage: ptr(int32(100)),
	})

	require.NoError(t, err)
}

func TestAudit1_GetResources_TagsPerPage_MaxValid(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
		TagsPerPage: ptr(int32(500)),
	})

	require.NoError(t, err)
}

func TestAudit1_GetResources_TagsPerPage_Nil_OK(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{})

	require.NoError(t, err)
}

// ======================================================================
// ResourceTypeFilter format validation
// ======================================================================

func TestAudit1_ResourceTypeFilter_Validation(t *testing.T) {
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

// ======================================================================
// Case-sensitive ResourceTypeFilter matching
// ======================================================================

func TestAudit1_ResourceTypeFilter_CaseSensitiveMatch(t *testing.T) {
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

// ======================================================================
// ARN validation in TagResources / UntagResources
// ======================================================================

func TestAudit1_TagResources_ARN_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arns    []string
		wantErr bool
	}{
		{
			name:    "empty_arn",
			arns:    []string{""},
			wantErr: true,
		},
		{
			name:    "invalid_arn_no_colons",
			arns:    []string{"not-an-arn"},
			wantErr: true,
		},
		{
			name:    "valid_sqs_arn",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:my-queue"},
			wantErr: false,
		},
		{
			name:    "valid_ec2_arn",
			arns:    []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-1234"},
			wantErr: false,
		},
		{
			name:    "mixed_valid_and_invalid",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1", "bad"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
				ResourceARNList: tt.arns,
				Tags:            map[string]string{"env": "test"},
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

func TestAudit1_UntagResources_ARN_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arns    []string
		wantErr bool
	}{
		{
			name:    "empty_arn",
			arns:    []string{""},
			wantErr: true,
		},
		{
			name:    "invalid_arn",
			arns:    []string{"bogus"},
			wantErr: true,
		},
		{
			name:    "valid_arn",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
				ResourceARNList: tt.arns,
				TagKeys:         []string{"env"},
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

// ======================================================================
// UntagResources TagKeys validation
// ======================================================================

func TestAudit1_UntagResources_TagKeys_Validation(t *testing.T) {
	t.Parallel()

	validARN := "arn:aws:sqs:us-east-1:000000000000:q1"

	tests := []struct {
		name    string
		keys    []string
		wantErr bool
	}{
		{
			name:    "empty_key_in_list",
			keys:    []string{""},
			wantErr: true,
		},
		{
			name:    "key_too_long",
			keys:    []string{strings.Repeat("k", 129)},
			wantErr: true,
		},
		{
			name:    "key_exactly_max_length",
			keys:    []string{strings.Repeat("k", 128)},
			wantErr: false,
		},
		{
			name:    "too_many_keys_51",
			keys:    makeKeys(51),
			wantErr: true,
		},
		{
			name:    "exactly_50_keys",
			keys:    makeKeys(50),
			wantErr: false,
		},
		{
			name:    "single_valid_key",
			keys:    []string{"env"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
				ResourceARNList: []string{validARN},
				TagKeys:         tt.keys,
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

func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	return keys
}

// ======================================================================
// Dedup across providers
// ======================================================================

func TestAudit1_GetResources_Dedup_AcrossProviders(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Both providers return the same ARN; last writer wins.
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"source": "first"},
			},
		}
	})
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"source": "second"},
			},
		}
	})

	out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{})

	require.NoError(t, err)
	require.Len(t, out.ResourceTagMappingList, 1, "duplicate ARN must appear exactly once")

	tags := out.ResourceTagMappingList[0].Tags
	require.Len(t, tags, 1)
	assert.Equal(t, "source", tags[0].Key)
	assert.Equal(t, "second", tags[0].Value, "last provider wins")
}

func TestAudit1_GetResources_Dedup_UniqueARNs_AllAppear(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q1",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"k": "v"},
			},
		}
	})
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{
				ResourceARN:  "arn:aws:sqs:us-east-1:000000000000:q2",
				ResourceType: "sqs:queue",
				Tags:         map[string]string{"k": "v"},
			},
		}
	})

	out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{})

	require.NoError(t, err)
	assert.Len(t, out.ResourceTagMappingList, 2)
}

// ======================================================================
// GetResources multi-key TagFilter (AND semantics)
// ======================================================================

func TestAudit1_GetResources_MultiKeyTagFilter_AND(t *testing.T) {
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

// ======================================================================
// GetResources pagination
// ======================================================================

func TestAudit1_GetResources_Pagination_FullCoverage(t *testing.T) {
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

// ======================================================================
// GetResources IncludeComplianceDetails
// ======================================================================

func TestAudit1_GetResources_ComplianceDetails_TableDriven(t *testing.T) {
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

// ======================================================================
// GetTagKeys comprehensive
// ======================================================================

func TestAudit1_GetTagKeys_TableDriven(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"alpha": "1", "beta": "2"}},
		{ResourceARN: "arn:2", Tags: map[string]string{"beta": "3", "gamma": "4"}},
		{ResourceARN: "arn:3", Tags: map[string]string{}},
	})

	tests := []struct {
		name     string
		token    *string
		wantKeys []string
		wantNil  bool
	}{
		{
			name:     "no_token_all_keys",
			wantKeys: []string{"alpha", "beta", "gamma"},
			wantNil:  true,
		},
		{
			name:     "token_after_alpha",
			token:    ptr("alpha"),
			wantKeys: []string{"beta", "gamma"},
			wantNil:  true,
		},
		{
			name:     "token_after_beta",
			token:    ptr("beta"),
			wantKeys: []string{"gamma"},
			wantNil:  true,
		},
		{
			name:     "token_after_last",
			token:    ptr("gamma"),
			wantKeys: nil,
			wantNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := b.GetTagKeys(
				context.Background(),
				&resourcegroupstaggingapi.GetTagKeysInput{PaginationToken: tt.token},
			)
			require.NotNil(t, out)

			if len(tt.wantKeys) == 0 {
				assert.Empty(t, out.TagKeys)
			} else {
				assert.Equal(t, tt.wantKeys, out.TagKeys)
			}

			if tt.wantNil {
				assert.Nil(t, out.PaginationToken)
			} else {
				assert.NotNil(t, out.PaginationToken)
			}
		})
	}
}

// ======================================================================
// GetTagValues comprehensive
// ======================================================================

func TestAudit1_GetTagValues_TableDriven(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"env": "dev", "region": "us-east-1"}},
		{ResourceARN: "arn:2", Tags: map[string]string{"env": "staging"}},
		{ResourceARN: "arn:3", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:4", Tags: map[string]string{"other": "value"}},
	})

	tests := []struct {
		name       string
		key        *string
		token      *string
		wantValues []string
	}{
		{
			name:       "all_env_values",
			key:        ptr("env"),
			wantValues: []string{"dev", "prod", "staging"},
		},
		{
			name:       "token_after_dev",
			key:        ptr("env"),
			token:      ptr("dev"),
			wantValues: []string{"prod", "staging"},
		},
		{
			name:       "nil_key_returns_empty",
			key:        nil,
			wantValues: nil,
		},
		{
			name:       "key_with_no_resources",
			key:        ptr("nonexistent"),
			wantValues: nil,
		},
		{
			name:       "region_key",
			key:        ptr("region"),
			wantValues: []string{"us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{
				Key:             tt.key,
				PaginationToken: tt.token,
			})

			require.NotNil(t, out)

			if len(tt.wantValues) == 0 {
				assert.Empty(t, out.TagValues)
			} else {
				assert.Equal(t, tt.wantValues, out.TagValues)
			}
		})
	}
}

// ======================================================================
// TagResources / UntagResources batch semantics
// ======================================================================

func TestAudit1_TagResources_Batch(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	taggedState := make(map[string]map[string]string)

	b.RegisterARNTagger(func(_ context.Context, arn string, tags map[string]string) (bool, error) {
		if !strings.Contains(arn, "sqs") {
			return false, nil
		}

		taggedState[arn] = maps.Clone(tags)

		return true, nil
	})

	arns := []string{
		"arn:aws:sqs:us-east-1:000000000000:q1",
		"arn:aws:sqs:us-east-1:000000000000:q2",
		"arn:aws:sqs:us-east-1:000000000000:q3",
	}

	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: arns,
		Tags:            map[string]string{"env": "prod", "owner": "team-a"},
	})

	require.NoError(t, err)
	assert.Empty(t, out.FailedResourcesMap)

	for _, arn := range arns {
		require.Contains(t, taggedState, arn)
		assert.Equal(t, "prod", taggedState[arn]["env"])
		assert.Equal(t, "team-a", taggedState[arn]["owner"])
	}
}

func TestAudit1_UntagResources_Batch(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	untaggedState := make(map[string][]string)

	b.RegisterARNUntagger(func(_ context.Context, arn string, keys []string) (bool, error) {
		if !strings.Contains(arn, "sqs") {
			return false, nil
		}

		untaggedState[arn] = append(untaggedState[arn], keys...)

		return true, nil
	})

	arns := []string{
		"arn:aws:sqs:us-east-1:000000000000:q1",
		"arn:aws:sqs:us-east-1:000000000000:q2",
	}

	out, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: arns,
		TagKeys:         []string{"env", "owner"},
	})

	require.NoError(t, err)
	assert.Empty(t, out.FailedResourcesMap)

	for _, arn := range arns {
		require.Contains(t, untaggedState, arn)
		assert.ElementsMatch(t, []string{"env", "owner"}, untaggedState[arn])
	}
}

// ======================================================================
// GetComplianceSummary with filters
// ======================================================================

func TestAudit1_GetComplianceSummary_WithFilters(t *testing.T) {
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

			out := b.GetComplianceSummary(context.Background(), tt.input)

			require.NotNil(t, out)
			// Mock has no tag policy → always returns 0 non-compliant resources.
			assert.NotNil(t, out.SummaryList)
			assert.Empty(t, out.SummaryList)
		})
	}
}

// ======================================================================
// StartReportCreation / DescribeReportCreation lifecycle
// ======================================================================

func TestAudit1_ReportCreation_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		wantS3Parts []string
		wantErr     bool
	}{
		{
			name:        "valid_bucket",
			bucket:      "my-report-bucket",
			wantS3Parts: []string{"my-report-bucket", "AwsTagPolicies", "report.csv"},
		},
		{
			name:    "empty_bucket",
			bucket:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.StartReportCreation(context.Background(), &resourcegroupstaggingapi.StartReportCreationInput{
				S3Bucket: tt.bucket,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Immediately after Start the report is RUNNING.
			assert.Equal(t, "RUNNING", resourcegroupstaggingapi.ReportStatus(b))

			// Advance clock past the running window so DescribeReportCreation returns SUCCEEDED.
			fastForward := time.Now().Add(resourcegroupstaggingapi.ReportRunningDuration() + time.Second)
			resourcegroupstaggingapi.SetClockFunc(b, func() time.Time { return fastForward })

			desc := b.DescribeReportCreation(context.Background())
			require.NotNil(t, desc.Status)
			assert.Equal(t, "SUCCEEDED", *desc.Status)
			require.NotNil(t, desc.S3Location)

			for _, part := range tt.wantS3Parts {
				assert.Contains(t, *desc.S3Location, part)
			}

			require.NotNil(t, desc.StartDate)
			assert.NotEmpty(t, *desc.StartDate)
		})
	}
}

// ======================================================================
// Handler-level 400 validation responses
// ======================================================================

func TestAudit1_Handler_ValidationErrors_Return400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body any
		name string
		op   string
		want int
	}{
		{
			name: "GetResources_exclude_compliant_without_include_details",
			op:   "GetResources",
			body: map[string]any{
				"ExcludeCompliantResources": true,
				"IncludeComplianceDetails":  false,
			},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_duplicate_tag_filter_keys",
			op:   "GetResources",
			body: map[string]any{
				"TagFilters": []map[string]any{
					{"Key": "env"},
					{"Key": "env"},
				},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_invalid_resource_type_filter",
			op:   "GetResources",
			body: map[string]any{"ResourceTypeFilters": []string{"SQS:Queue"}},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_tags_per_page_too_small",
			op:   "GetResources",
			body: map[string]any{"TagsPerPage": 50},
			want: http.StatusBadRequest,
		},
		{
			name: "GetResources_tags_per_page_too_large",
			op:   "GetResources",
			body: map[string]any{"TagsPerPage": 501},
			want: http.StatusBadRequest,
		},
		{
			name: "TagResources_empty_arn",
			op:   "TagResources",
			body: map[string]any{
				"ResourceARNList": []string{""},
				"Tags":            map[string]string{"k": "v"},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "TagResources_invalid_arn",
			op:   "TagResources",
			body: map[string]any{
				"ResourceARNList": []string{"not-an-arn"},
				"Tags":            map[string]string{"k": "v"},
			},
			want: http.StatusBadRequest,
		},
		{
			name: "UntagResources_too_many_keys",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         makeKeys(51),
			},
			want: http.StatusBadRequest,
		},
		{
			name: "UntagResources_empty_key",
			op:   "UntagResources",
			body: map[string]any{
				"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				"TagKeys":         []string{""},
			},
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTaggingRequest(t, h, tt.op, tt.body)

			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

// ======================================================================
// GetResources cross-service / region aggregation
// ======================================================================

func TestAudit1_GetResources_CrossService_Aggregation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	services := []struct {
		arn      string
		resType  string
		tagKey   string
		tagValue string
	}{
		{"arn:aws:sqs:us-east-1:000000000000:q1", "sqs:queue", "env", "prod"},
		{"arn:aws:dynamodb:us-east-1:000000000000:table/t1", "dynamodb:table", "env", "prod"},
		{"arn:aws:ec2:us-east-1:000000000000:instance/i-1", "ec2:instance", "env", "dev"},
		{"arn:aws:s3:::my-bucket", "s3:bucket", "team", "ops"},
		{"arn:aws:lambda:us-east-1:000000000000:function:fn1", "lambda:function", "env", "prod"},
	}

	resources := make([]resourcegroupstaggingapi.TaggedResource, len(services))
	for i, s := range services {
		resources[i] = resourcegroupstaggingapi.TaggedResource{
			ResourceARN:  s.arn,
			ResourceType: s.resType,
			Tags:         map[string]string{s.tagKey: s.tagValue},
		}
	}

	seedResources(b, resources)

	tests := []struct {
		name        string
		tagFilters  []resourcegroupstaggingapi.TagFilter
		typeFilters []string
		wantLen     int
	}{
		{
			name:    "all_resources",
			wantLen: 5,
		},
		{
			name:       "filter_prod_env",
			tagFilters: []resourcegroupstaggingapi.TagFilter{{Key: "env", Values: []string{"prod"}}},
			wantLen:    3,
		},
		{
			name:        "type_filter_sqs",
			typeFilters: []string{"sqs:queue"},
			wantLen:     1,
		},
		{
			name:        "type_filter_service_only_ec2",
			typeFilters: []string{"ec2"},
			wantLen:     1,
		},
		{
			name: "combined_tag_and_type_filter",
			tagFilters: []resourcegroupstaggingapi.TagFilter{
				{Key: "env", Values: []string{"prod"}},
			},
			typeFilters: []string{"sqs:queue"},
			wantLen:     1,
		},
		{
			name:       "tag_key_only_team",
			tagFilters: []resourcegroupstaggingapi.TagFilter{{Key: "team"}},
			wantLen:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetResources(context.Background(), &resourcegroupstaggingapi.GetResourcesInput{
				TagFilters:          tt.tagFilters,
				ResourceTypeFilters: tt.typeFilters,
			})

			require.NoError(t, err)
			assert.Len(t, out.ResourceTagMappingList, tt.wantLen)
		})
	}
}

// ======================================================================
// Region filtering in GetComplianceSummary
// ======================================================================

func TestAudit1_GetComplianceSummary_RegionFilter_FiltersResources(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Only call GetComplianceSummary to ensure it doesn't panic with various filter combos.
	out := b.GetComplianceSummary(context.Background(), &resourcegroupstaggingapi.GetComplianceSummaryInput{
		RegionFilters:       []string{"us-east-1", "eu-west-1"},
		ResourceTypeFilters: []string{"ec2:instance"},
		TagKeyFilters:       []string{"env"},
		GroupBy:             []string{"REGION", "RESOURCE_TYPE"},
		MaxResults:          ptr(int32(100)),
	})

	require.NotNil(t, out)
	assert.NotNil(t, out.SummaryList)
}

// ======================================================================
// Snapshot / Restore (additional coverage)
// ======================================================================

func TestAudit1_SnapshotRestore_ProvidersClearedTaggersCleared(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource { return nil })
	b.RegisterARNTagger(func(_ context.Context, _ string, _ map[string]string) (bool, error) { return false, nil })
	b.RegisterARNUntagger(func(_ context.Context, _ string, _ []string) (bool, error) { return false, nil })

	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.UntaggerCount(b))

	snap := b.Snapshot(t.Context())

	b2 := newBackend(t)
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Providers, taggers, and untaggers are runtime callbacks; they cannot be serialized.
	assert.Equal(t, 0, resourcegroupstaggingapi.ProviderCount(b2))
	assert.Equal(t, 0, resourcegroupstaggingapi.TaggerCount(b2))
	assert.Equal(t, 0, resourcegroupstaggingapi.UntaggerCount(b2))
}
