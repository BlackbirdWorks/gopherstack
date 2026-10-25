package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestInventoryProviders(t *testing.T) {
	t.Parallel()

	const (
		untagged = "arn:aws:aps:us-east-1:000000000000:workspace/ws-1"
		tagged   = "arn:aws:aps:us-east-1:000000000000:workspace/ws-2"
	)

	b := newBackend(t)

	b.RegisterInventoryProvider(func(context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: untagged, ResourceType: "aps:workspace"},
			{ResourceARN: tagged, ResourceType: "aps:workspace"},
		}
	})
	b.RegisterProvider(func(context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: tagged, ResourceType: "aps:workspace", Tags: map[string]string{"env": "prod"}},
		}
	})

	tests := []struct {
		list func() []string
		name string
		want []string
	}{
		{
			name: "all_resources_include_untagged",
			list: func() []string {
				var arns []string
				for _, r := range b.AllResources(t.Context()) {
					arns = append(arns, r.ResourceARN)
				}

				return arns
			},
			want: []string{untagged, tagged},
		},
		{
			name: "get_resources_excludes_inventory_only",
			list: func() []string {
				out, err := b.GetResources(t.Context(), &resourcegroupstaggingapi.GetResourcesInput{})
				require.NoError(t, err)

				var arns []string
				for _, m := range out.ResourceTagMappingList {
					arns = append(arns, m.ResourceARN)
				}

				return arns
			},
			want: []string{tagged},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.ElementsMatch(t, tt.want, tt.list())
		})
	}
}

func TestAllResourcesTaggedTwinWins(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterInventoryProvider(func(context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{{ResourceARN: "arn:aws:sqs:us-east-1:000000000000:q"}}
	})
	b.RegisterProvider(func(context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: "arn:aws:sqs:us-east-1:000000000000:q", Tags: map[string]string{"k": "v"}},
		}
	})

	got := b.AllResources(t.Context())
	require.Len(t, got, 1)
	assert.Equal(t, map[string]string{"k": "v"}, got[0].Tags)
}
