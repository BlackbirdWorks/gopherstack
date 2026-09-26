package redshiftdata

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRedshiftData_IdempotencyEviction proves storeIdempotentStatement
// opportunistically sweeps expired entries once the cache grows past
// idempotencyEvictThreshold, so an ExecuteStatement/BatchExecuteStatement
// call whose ClientToken is never replayed does not sit in the cache forever
// (only lookupIdempotentStatement pruned before this fix, and only for the
// exact key it was asked about).
func TestRedshiftData_IdempotencyEviction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		seedExpired int
		inserts     int
		wantSwept   bool
	}{
		{name: "below threshold keeps expired", seedExpired: 1, inserts: 1},
		{
			name:        "threshold and sweep interval evicts expired",
			seedExpired: idempotencyEvictThreshold + 16,
			inserts:     idempotencyEvictSweepInterval,
			wantSwept:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))
			past := time.Now().Add(-time.Hour)

			for i := range tt.seedExpired {
				h.idempotency.Set(
					fmt.Sprintf("expired-%d", i),
					idempotentStatement{id: "stmt-expired", expiresAt: past},
				)
			}

			for i := range tt.inserts {
				h.storeIdempotentStatement(fmt.Sprintf("live-%d", i), "stmt-live")
			}

			_, stillPresent := h.idempotency.Get("expired-0")
			if tt.wantSwept {
				assert.False(t, stillPresent, "expired entry should have been swept")
			} else {
				assert.True(t, stillPresent, "expired entry should remain below the eviction threshold")
			}
		})
	}
}
