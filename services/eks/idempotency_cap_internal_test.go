package eks

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIdempotency_IsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		inserts    int
		wantOldest bool
	}{
		{name: "below_cap_keeps_oldest", inserts: 10, wantOldest: true},
		{name: "past_cap_evicts_oldest", inserts: maxIdempotencyRecords + 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

			for i := range tt.inserts {
				b.storeIdempotency("CreateCluster", "tok-"+strconv.Itoa(i), "fp", 200, []byte("{}"))
			}

			_, oldest := b.lookupIdempotency("CreateCluster", "tok-0")
			_, newest := b.lookupIdempotency("CreateCluster", "tok-"+strconv.Itoa(tt.inserts-1))

			assert.Equal(t, tt.wantOldest, oldest)
			assert.True(t, newest)
			require.LessOrEqual(t, b.idempotency.Len(), maxIdempotencyRecords)
		})
	}
}
