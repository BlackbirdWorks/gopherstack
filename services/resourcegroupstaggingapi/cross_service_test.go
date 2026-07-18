package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestGetResources_Dedup_AcrossProviders(t *testing.T) {
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

func TestGetResources_Dedup_UniqueARNs_AllAppear(t *testing.T) {
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

// TestGetResources_CrossService_Aggregation verifies that GetResources aggregates
// resources registered by providers representing several different services and that
// tag/type filters apply correctly across the aggregated set.
func TestGetResources_CrossService_Aggregation(t *testing.T) {
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
