package sagemaker //nolint:testpackage // needs sagemakrCtxRegion (isolation_test.go) + the unexported region ctx key.

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackendConcurrentReadWriteNoRace is a regression test for a class of
// data races where a List*/Describe* method — holding only b.mu.RLock() —
// lazily initialised a per-region resource map (e.g. b.models[region] = ...)
// exactly like the Create*/Delete* methods that hold the full b.mu.Lock().
// Two RLock-holding readers (or a reader racing a writer) could both observe
// a nil entry and concurrently write to the same outer map, which is a data
// race on a plain Go map regardless of the RWMutex, since RLock does not
// serialize readers against each other.
//
// It hammers several resource families across multiple regions — including a
// region no writer ever touches, to specifically exercise concurrent readers
// racing to "create" the same never-before-seen per-region entry — with
// concurrent Create/List/Describe calls. Run with `go test -race` to catch a
// regression of this class; it must pass cleanly with no race detected.
func TestBackendConcurrentReadWriteNoRace(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	const roleARN = "arn:aws:iam::000000000000:role/test-role"

	const (
		workersPerRegion = 8
		opsPerWorker     = 25
	)

	writeRegions := []string{"us-east-1", "us-west-2", "eu-central-1"}
	readOnlyRegion := "ap-southeast-1" // no Create* call ever targets this region.

	var wg sync.WaitGroup

	for _, region := range writeRegions {
		ctx := sagemakrCtxRegion(region)

		for w := range workersPerRegion {
			wg.Go(func() {
				for i := range opsPerWorker {
					modelName := fmt.Sprintf("model-%s-%d-%d", region, w, i)
					_, _ = backend.CreateModel(ctx, modelName, roleARN, nil, nil, nil)

					ecName := fmt.Sprintf("ec-%s-%d-%d", region, w, i)
					_, _ = backend.CreateEndpointConfig(ctx, ecName, nil, nil)

					epName := fmt.Sprintf("ep-%s-%d-%d", region, w, i)
					_, _ = backend.CreateEndpoint(ctx, epName, ecName, nil)

					tjName := fmt.Sprintf("tj-%s-%d-%d", region, w, i)
					_, _ = backend.CreateTrainingJob(
						ctx, tjName, roleARN, map[string]string{"TrainingImage": "an-image"}, nil,
					)
				}
			})

			wg.Go(func() {
				for i := range opsPerWorker {
					_, _ = backend.ListModels(ctx, "")
					_, _ = backend.ListEndpoints(ctx, "")
					_, _ = backend.ListEndpointConfigs(ctx, "")
					_, _ = backend.ListTrainingJobs(ctx, "")
					_, _ = backend.DescribeModel(ctx, fmt.Sprintf("model-%s-%d-%d", region, w, i))
				}
			})
		}
	}

	readOnlyCtx := sagemakrCtxRegion(readOnlyRegion)

	for range workersPerRegion {
		wg.Go(func() {
			for range opsPerWorker {
				_, _ = backend.ListModels(readOnlyCtx, "")
				_, _ = backend.ListEndpoints(readOnlyCtx, "")
				_, _ = backend.ListEndpointConfigs(readOnlyCtx, "")
				_, _ = backend.ListTrainingJobs(readOnlyCtx, "")
				_, _ = backend.DescribeModel(readOnlyCtx, "does-not-exist")
			}
		})
	}

	wg.Wait()

	// Functional sanity check, not just "the race detector stayed quiet":
	// the writers must actually have persisted models.
	require.NotZero(t, ModelCount(backend))
}
