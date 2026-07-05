package secretsmanager //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// Test_ConcurrentRegionReads_NoDataRace exercises read-only ops (RLock-guarded) against
// many never-before-seen regions concurrently. Before the fix, ListSecrets,
// ListSecretVersionIDs, DescribeSecret, GetResourcePolicy, and ValidateResourcePolicy all
// called the lazily-creating *Store(region) helper (which writes b.secrets[region] =
// make(...) on first touch) while holding only an RLock — a concurrent map write during
// a shared read lock, which the Go race detector (and, at higher contention, the runtime
// itself) flags as a data race. The fix introduces non-mutating *StoreRO(region) helpers
// for read paths. Run with `go test -race` to verify.
func Test_ConcurrentRegionReads_NoDataRace(t *testing.T) {
	t.Parallel()

	const goroutinesPerOp = 24

	cases := []struct {
		run  func(ctx context.Context, b *InMemoryBackend)
		name string
	}{
		{
			name: opListSecrets,
			run: func(ctx context.Context, b *InMemoryBackend) {
				_, _ = b.ListSecrets(ctx, &ListSecretsInput{})
			},
		},
		{
			name: "ListSecretVersionIDs",
			run: func(ctx context.Context, b *InMemoryBackend) {
				_, _ = b.ListSecretVersionIDs(ctx, &ListSecretVersionIDsInput{SecretID: "nonexistent"})
			},
		},
		{
			name: opDescribeSecret,
			run: func(ctx context.Context, b *InMemoryBackend) {
				_, _ = b.DescribeSecret(ctx, &DescribeSecretInput{SecretID: "nonexistent"})
			},
		},
		{
			name: opGetResourcePolicy,
			run: func(ctx context.Context, b *InMemoryBackend) {
				_, _ = b.GetResourcePolicy(ctx, &GetResourcePolicyInput{SecretID: "nonexistent"})
			},
		},
		{
			name: opValidateResourcePolicy,
			run: func(ctx context.Context, b *InMemoryBackend) {
				_, _ = b.ValidateResourcePolicy(ctx, &ValidateResourcePolicyInput{
					SecretID:       "nonexistent",
					ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Fresh, isolated backend per subtest.
			b := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

			var wg sync.WaitGroup

			for i := range goroutinesPerOp {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()

					// Each goroutine targets a distinct, never-before-seen region so the
					// lazy per-region map entry is created concurrently by many readers.
					region := fmt.Sprintf("race-region-%d", i)
					ctx := context.WithValue(context.Background(), regionContextKey{}, region)
					tc.run(ctx, b)
				}(i)
			}

			wg.Wait()
		})
	}
}
