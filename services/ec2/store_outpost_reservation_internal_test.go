package ec2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOutpostReservedInstanceIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "single instance", count: 1},
		{name: "small batch", count: 5},
		{name: "at the request limit", count: maxInstancesPerRunInstancesRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ids := newOutpostReservedInstanceIDs(tt.count)

			require.Len(t, ids, tt.count)
			assert.LessOrEqualf(
				t,
				cap(ids),
				tt.count*4,
				"cap(%d) = %d: reservation must scale with count, not stay pinned near maxInstancesPerRunInstancesRequest (%d)",
				tt.count,
				cap(ids),
				maxInstancesPerRunInstancesRequest,
			)
		})
	}
}
