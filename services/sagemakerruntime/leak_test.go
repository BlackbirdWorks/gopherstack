package sagemakerruntime_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// TestInMemoryBackend_BoundedGrowth proves that the per-request session and
// async-invocation maps stay bounded under sustained insertion (parity §E:
// unbounded-growth leak). Inserting well past the cap must leave the retained
// set at exactly the cap, with the oldest entries evicted FIFO-style.
func TestInMemoryBackend_BoundedGrowth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		// insert performs one record-creating call and returns the live count.
		insert  func(b *sagemakerruntime.InMemoryBackend, i int) int
		name    string
		cap     int
		inserts int
	}{
		{
			name:    "sessions stay capped",
			cap:     sagemakerruntime.MaxSessions,
			inserts: sagemakerruntime.MaxSessions + 500,
			insert: func(b *sagemakerruntime.InMemoryBackend, i int) int {
				b.StartSession(fmt.Sprintf("endpoint-%d", i))

				return len(b.ListSessions())
			},
		},
		{
			name:    "async invocations stay capped",
			cap:     sagemakerruntime.MaxAsyncInvocations,
			inserts: sagemakerruntime.MaxAsyncInvocations + 500,
			insert: func(b *sagemakerruntime.InMemoryBackend, i int) int {
				// Empty requestedID forces a fresh generated inference ID each call,
				// guaranteeing a distinct map key per insert.
				b.RecordAsyncInvocation(fmt.Sprintf("endpoint-%d", i), "", "payload", "")

				return len(b.ListAsyncInvocations())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")

			var count int
			for i := range tc.inserts {
				count = tc.insert(b, i)
				require.LessOrEqual(t, count, tc.cap,
					"retained set must never exceed the cap during insertion")
			}

			require.Equal(t, tc.cap, count,
				"after inserting past the cap the retained set must equal the cap")
		})
	}
}

// TestInMemoryBackend_AsyncInvocationEvictsOldest proves FIFO semantics: the
// oldest async-invocation record is the one evicted once the cap is exceeded.
func TestInMemoryBackend_AsyncInvocationEvictsOldest(t *testing.T) {
	t.Parallel()

	b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")

	// The first inserted inference ID is the oldest; it must be evicted first.
	const oldestID = "inference-oldest"
	b.RecordAsyncInvocation("endpoint-0", oldestID, "payload", "")

	for i := range sagemakerruntime.MaxAsyncInvocations {
		b.RecordAsyncInvocation(fmt.Sprintf("endpoint-%d", i+1), "", "payload", "")
	}

	got := b.ListAsyncInvocations()
	require.Len(t, got, sagemakerruntime.MaxAsyncInvocations)

	for _, inv := range got {
		require.NotEqual(t, oldestID, inv.InferenceID,
			"oldest async invocation must be evicted once the cap is exceeded")
	}
}
