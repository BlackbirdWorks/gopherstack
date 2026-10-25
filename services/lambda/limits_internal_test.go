package lambda

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireAccountSlot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limit    int
		reserved int
		acquire  int
		wantErr  bool
	}{
		{name: "under limit", limit: 3, acquire: 3},
		{name: "over limit", limit: 2, acquire: 3, wantErr: true},
		{name: "reservations shrink pool", limit: 3, reserved: 2, acquire: 2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(nil, nil, Settings{ConcurrentExecutions: tt.limit}, "000000000000", "us-east-1")
			t.Cleanup(func() { b.Close(context.Background()) })

			if tt.reserved > 0 {
				b.functionConcurrencies["reserved-fn"] = tt.reserved
			}

			var err error

			for range tt.acquire {
				var tracked bool

				tracked, err = b.acquireAccountSlot("fn")
				if err != nil {
					break
				}

				assert.True(t, tracked)
			}

			if tt.wantErr {
				require.ErrorIs(t, err, ErrTooManyRequests)

				return
			}

			require.NoError(t, err)
			b.releaseConcurrencySlot("fn")
			assert.Equal(t, tt.acquire-1, b.activeConcurrencies["fn"])
		})
	}
}
