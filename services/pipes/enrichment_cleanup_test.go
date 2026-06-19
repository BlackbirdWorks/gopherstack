package pipes_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func newPipesBackend(t *testing.T) *pipes.InMemoryBackend {
	t.Helper()

	return pipes.NewInMemoryBackend("000000000000", "us-east-1")
}

func createEnrichmentTestPipe(t *testing.T, b *pipes.InMemoryBackend, name string) {
	t.Helper()
	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:    name,
		RoleARN: "arn:aws:iam::000000000000:role/test",
		Source:  "arn:aws:sqs:us-east-1:000000000000:source",
		Target:  "arn:aws:sqs:us-east-1:000000000000:target",
	})
	require.NoError(t, err)
}

// waitPipeDeleted waits up to 500ms for a pipe to be fully deleted (removed from store).
func waitPipeDeleted(t *testing.T, b *pipes.InMemoryBackend, name string) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, err := b.GetPipe(context.Background(), name)
		if err != nil {
			return // pipe is gone
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("pipe %q was not deleted within 500ms", name)
}

// TestPipesEnrichmentCallCountPrunedOnDelete verifies that when a pipe is deleted
// its enrichment call counter is removed from the index, preventing unbounded growth.
func TestPipesEnrichmentCallCountPrunedOnDelete(t *testing.T) {
	t.Parallel()

	b := newPipesBackend(t)

	createEnrichmentTestPipe(t, b, "my-pipe")

	// Record some enrichment calls.
	b.RecordEnrichmentCall(context.Background(), "my-pipe")
	b.RecordEnrichmentCall(context.Background(), "my-pipe")
	assert.Equal(t, int64(2), b.EnrichmentCallCountForTest("my-pipe"))
	assert.Equal(t, 1, b.EnrichmentIndexSizeForTest())

	// Delete the pipe and wait for the async transition to complete.
	_, err := b.DeletePipe(context.Background(), "my-pipe")
	require.NoError(t, err)

	waitPipeDeleted(t, b, "my-pipe")

	// The enrichment counter for the deleted pipe must have been pruned.
	assert.Equal(t, int64(0), b.EnrichmentCallCountForTest("my-pipe"),
		"enrichment count for deleted pipe must be 0")
	assert.Equal(t, 0, b.EnrichmentIndexSizeForTest(),
		"enrichment index must be empty after pipe deletion")
}

// TestPipesEnrichmentCountOnlyPrunesDeletedPipe verifies that deleting one pipe
// does not affect the enrichment counters of other pipes.
func TestPipesEnrichmentCountOnlyPrunesDeletedPipe(t *testing.T) {
	t.Parallel()

	b := newPipesBackend(t)

	createEnrichmentTestPipe(t, b, "pipe-a")
	createEnrichmentTestPipe(t, b, "pipe-b")

	b.RecordEnrichmentCall(context.Background(), "pipe-a")
	b.RecordEnrichmentCall(context.Background(), "pipe-b")
	b.RecordEnrichmentCall(context.Background(), "pipe-b")

	assert.Equal(t, 2, b.EnrichmentIndexSizeForTest())

	_, err := b.DeletePipe(context.Background(), "pipe-a")
	require.NoError(t, err)
	waitPipeDeleted(t, b, "pipe-a")

	// pipe-b counter must be untouched.
	assert.Equal(t, int64(2), b.EnrichmentCallCountForTest("pipe-b"))
	assert.Equal(t, 1, b.EnrichmentIndexSizeForTest(), "only pipe-a should be pruned")
}

// BenchmarkPipesEnrichmentCallCount benchmarks RecordEnrichmentCall to confirm
// it stays O(1) per call with no accumulation leak.
func BenchmarkPipesEnrichmentCallCount(b *testing.B) {
	backend := pipes.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := backend.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:    "bench-pipe",
		RoleARN: "arn:aws:iam::000000000000:role/test",
		Source:  "arn:aws:sqs:us-east-1:000000000000:source",
		Target:  "arn:aws:sqs:us-east-1:000000000000:target",
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		backend.RecordEnrichmentCall(context.Background(), "bench-pipe")
	}
}
