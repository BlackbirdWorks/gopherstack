package redshiftdata

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedshiftData_IdempotencyIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		inserts    int
		wantOldest bool
	}{
		{name: "below cap keeps oldest", inserts: 10, wantOldest: true},
		{name: "past cap evicts oldest", inserts: idempotencyEntries + 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))

			for i := range tt.inserts {
				h.storeIdempotentStatement("k-"+strconv.Itoa(i), "stmt")
			}

			_, oldest := h.lookupIdempotentStatement("k-0")
			_, newest := h.lookupIdempotentStatement("k-" + strconv.Itoa(tt.inserts-1))

			assert.Equal(t, tt.wantOldest, oldest)
			assert.True(t, newest)
			assert.LessOrEqual(t, h.idem.Len(), idempotencyEntries)
		})
	}
}
