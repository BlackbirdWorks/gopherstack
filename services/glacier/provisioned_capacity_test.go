package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestNonNilProvisionedCapacity verifies ListProvisionedCapacity returns non-nil empty slice.
func TestNonNilProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no_capacity_returns_non_nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			caps := b.ListProvisionedCapacity(testAccountID)

			assert.NotNil(t, caps, tt.name)
			assert.Empty(t, caps)
		})
	}
}

// TestSortedListProvisionedCapacity verifies capacity units are sorted by CapacityID.
func TestSortedListProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "capacity_sorted_by_id", count: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			for range tt.count {
				_, err := b.PurchaseProvisionedCapacity(testAccountID)
				require.NoError(t, err)
			}

			caps := b.ListProvisionedCapacity(testAccountID)
			require.Len(t, caps, tt.count)

			for i := 1; i < len(caps); i++ {
				assert.LessOrEqual(t, caps[i-1].CapacityID, caps[i].CapacityID)
			}
		})
	}
}
