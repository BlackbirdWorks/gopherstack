package resourcegroupstaggingapi_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestGetResources_NoProviders(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")

	out := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{})

	require.NotNil(t, out)
	assert.Empty(t, out.ResourceTagMappingList)
	assert.Nil(t, out.PaginationToken)
}

func TestGetResources_TagFilter(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource {
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

			out := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{TagFilters: tt.tagFilters})

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
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource {
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
			name:        "case_insensitive",
			typeFilters: []string{"SQS:Queue"},
			wantLen:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{ResourceTypeFilters: tt.typeFilters})

			require.NotNil(t, out)
			assert.Len(t, out.ResourceTagMappingList, tt.wantLen)
		})
	}
}

func TestGetResources_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: "arn:aws:sqs:us-east-1:123:a", ResourceType: "sqs:queue", Tags: map[string]string{"k": "v"}},
			{ResourceARN: "arn:aws:sqs:us-east-1:123:b", ResourceType: "sqs:queue", Tags: map[string]string{"k": "v"}},
			{ResourceARN: "arn:aws:sqs:us-east-1:123:c", ResourceType: "sqs:queue", Tags: map[string]string{"k": "v"}},
		}
	})

	pageSize := int32(2)

	out1 := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{ResourcesPerPage: &pageSize})
	require.NotNil(t, out1)
	require.NotNil(t, out1.PaginationToken)
	assert.Len(t, out1.ResourceTagMappingList, 2)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:a", out1.ResourceTagMappingList[0].ResourceARN)

	out2 := b.GetResources(&resourcegroupstaggingapi.GetResourcesInput{
		ResourcesPerPage: &pageSize,
		PaginationToken:  *out1.PaginationToken,
	})
	require.NotNil(t, out2)
	assert.Nil(t, out2.PaginationToken)
	assert.Len(t, out2.ResourceTagMappingList, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:c", out2.ResourceTagMappingList[0].ResourceARN)
}

func TestGetTagKeys(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod", "team": "ops"}},
			{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev", "owner": "alice"}},
		}
	})

	out := b.GetTagKeys()

	require.NotNil(t, out)
	assert.Equal(t, []string{"env", "owner", "team"}, out.TagKeys)
}

func TestGetTagValues(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")
	b.RegisterProvider(func() []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
			{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev"}},
			{ResourceARN: "arn:3", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
		}
	})

	out := b.GetTagValues(&resourcegroupstaggingapi.GetTagValuesInput{Key: "env"})

	require.NotNil(t, out)
	assert.Equal(t, []string{"dev", "prod"}, out.TagValues)
}

func TestTagResources_Handled(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")

	taggedARNs := make(map[string]map[string]string)

	b.RegisterARNTagger(func(arn string, tags map[string]string) (bool, error) {
		if !isARN(arn, "sqs") {
			return false, nil
		}

		taggedARNs[arn] = tags

		return true, nil
	})

	out := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:123:q1"},
		Tags:            map[string]string{"env": "test"},
	})

	require.NotNil(t, out)
	assert.Empty(t, out.FailedResourcesMap)
	assert.Equal(t, map[string]string{"env": "test"}, taggedARNs["arn:aws:sqs:us-east-1:123:q1"])
}

func TestTagResources_Unhandled(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")

	out := b.TagResources(&resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:123:q1"},
		Tags:            map[string]string{"env": "test"},
	})

	require.NotNil(t, out)
	assert.Len(t, out.FailedResourcesMap, 1)
	assert.Contains(t, out.FailedResourcesMap, "arn:aws:sqs:us-east-1:123:q1")
}

func TestUntagResources(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "us-east-1")

	untaggedARNs := make(map[string][]string)

	b.RegisterARNUntagger(func(arn string, keys []string) (bool, error) {
		if !isARN(arn, "sqs") {
			return false, nil
		}

		untaggedARNs[arn] = keys

		return true, nil
	})

	out := b.UntagResources(&resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:123:q1"},
		TagKeys:         []string{"env"},
	})

	require.NotNil(t, out)
	assert.Empty(t, out.FailedResourcesMap)
	assert.Equal(t, []string{"env"}, untaggedARNs["arn:aws:sqs:us-east-1:123:q1"])
}

// isARN is a helper used in test code to check service prefix in an ARN.
func isARN(arn, service string) bool {
	// arn:aws:<service>:...
	parts := splitARN(arn)

	return len(parts) >= 3 && parts[2] == service
}

func splitARN(arn string) []string {
	out := make([]string, 0)
	start := 0

	for i := range len(arn) {
		if arn[i] == ':' {
			out = append(out, arn[start:i])
			start = i + 1
		}
	}

	out = append(out, arn[start:])

	return out
}
