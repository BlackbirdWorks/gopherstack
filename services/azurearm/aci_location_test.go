package azurearm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACILocationReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		location string
		groups   int
		wantUsed int32
	}{
		{name: "empty", location: "westus"},
		{name: "counts groups in the location", location: "local", groups: 2, wantUsed: 2},
		{name: "other location unaffected", location: "westus", groups: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)

			for i := range tt.groups {
				_, err := s.create(t, "rg", string(rune('a'+i))+"grp", aciOpts{})
				require.NoError(t, err)
			}

			loc := s.factory.NewLocationClient()

			usage, err := loc.NewListUsagePager(tt.location, nil).NextPage(t.Context())
			require.NoError(t, err)
			require.Len(t, usage.Value, 1)
			assert.Equal(t, "ContainerGroups", *usage.Value[0].Name.Value)
			assert.Equal(t, tt.wantUsed, *usage.Value[0].CurrentValue)
			assert.Equal(t, "Count", *usage.Value[0].Unit)
			assert.Positive(t, *usage.Value[0].Limit)

			images, err := loc.NewListCachedImagesPager(tt.location, nil).NextPage(t.Context())
			require.NoError(t, err)
			assert.NotEmpty(t, images.Value)

			caps, err := loc.NewListCapabilitiesPager(tt.location, nil).NextPage(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, caps.Value)
			assert.Equal(t, "containerGroups", *caps.Value[0].ResourceType)
			assert.Positive(t, *caps.Value[0].Capabilities.MaxCPU)
		})
	}
}
