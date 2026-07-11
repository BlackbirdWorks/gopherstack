// This file is package mediastore (not mediastore_test) so it can reach
// regionContextKey, matching isolation_test.go's existing precedent.
package mediastore //nolint:testpackage // existing issue, see isolation_test.go.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRegion and testAccountID are local to this file: backend_test.go's
// identically-named constants live in the separate mediastore_test package
// and are not visible here (this file is package mediastore, like
// isolation_test.go, so it can reach regionContextKey).
const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), regionContextKey{}, testRegion)

	b := NewInMemoryBackend()
	_, err := b.CreateContainer(ctx, testAccountID, "seed-container", nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"containers":{}}`))
	require.NoError(t, err)

	items, _, err := b.ListContainers(ctx, "", 0)
	require.NoError(t, err)
	assert.Empty(t, items)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches mediastoreSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape: MediaStore had no persistence at all
// before this conversion, so there is no legacy shape to actually collide
// with -- any input lacking "version" hits this same path.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), regionContextKey{}, testRegion)

	b := NewInMemoryBackend()
	_, err := b.CreateContainer(ctx, testAccountID, "seed-container", nil)
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"containers":{}}`))
	require.NoError(t, err)

	items, _, err := b.ListContainers(ctx, "", 0)
	require.NoError(t, err)
	assert.Empty(t, items)
}

// seedContainerState is every per-container sub-resource
// TestInMemoryBackend_SnapshotRestore_FullState exercises, kept together so
// the post-restore assertions can refer back to what was originally set.
type seedContainerState struct {
	name         string
	policy       string
	lifecycle    string
	metricPolicy MetricPolicy
	corsRules    []CorsRule
}

// seedRegion creates one fully-populated container (policy, CORS, lifecycle,
// metric policy, tags, access logging all set) in region under ctx, and
// returns the state used so the caller can assert on it after a round-trip.
func seedRegion(ctx context.Context, t *testing.T, b *InMemoryBackend, region string) seedContainerState {
	t.Helper()

	name := "container-" + region

	_, err := b.CreateContainer(ctx, testAccountID, name, map[string]string{"env": region})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	require.NoError(t, b.PutContainerPolicy(ctx, name, policy))

	corsRules := []CorsRule{{
		AllowedOrigins: []string{"https://example.com"},
		AllowedHeaders: []string{"*"},
		AllowedMethods: []string{"GET"},
		MaxAgeSeconds:  3600,
	}}
	require.NoError(t, b.PutCorsPolicy(ctx, name, corsRules))

	lifecycle := `{"rules":[]}`
	require.NoError(t, b.PutLifecyclePolicy(ctx, name, lifecycle))

	metricPolicy := MetricPolicy{
		ContainerLevelMetrics: "ENABLED",
		MetricPolicyRules: []MetricPolicyRule{
			{ObjectGroup: "AWSMediaTailor-*", ObjectGroupName: "MediaTailorGroup"},
		},
	}
	require.NoError(t, b.PutMetricPolicy(ctx, name, metricPolicy))

	require.NoError(t, b.StartAccessLogging(ctx, name))

	return seedContainerState{
		name:         name,
		policy:       policy,
		corsRules:    corsRules,
		lifecycle:    lifecycle,
		metricPolicy: metricPolicy,
	}
}

// assertRegionRestored verifies every field seedRegion set for one container
// survived a Snapshot/Restore round-trip.
func assertRegionRestored(
	ctx context.Context, t *testing.T, b *InMemoryBackend, region string, seed seedContainerState,
) {
	t.Helper()

	c, err := b.DescribeContainer(ctx, seed.name)
	require.NoError(t, err)
	assert.Equal(t, seed.name, c.Name)
	assert.Equal(t, containerStatusActive, c.Status)
	assert.Contains(t, c.ARN, ":"+region+":")
	assert.True(t, c.AccessLoggingEnabled)
	assert.Equal(t, map[string]string{"env": region}, c.Tags)

	gotPolicy, err := b.GetContainerPolicy(ctx, seed.name)
	require.NoError(t, err)
	assert.Equal(t, seed.policy, gotPolicy)

	gotCors, err := b.GetCorsPolicy(ctx, seed.name)
	require.NoError(t, err)
	assert.Equal(t, seed.corsRules, gotCors)

	gotLifecycle, err := b.GetLifecyclePolicy(ctx, seed.name)
	require.NoError(t, err)
	assert.Equal(t, seed.lifecycle, gotLifecycle)

	gotMetric, err := b.GetMetricPolicy(ctx, seed.name)
	require.NoError(t, err)
	assert.Equal(t, seed.metricPolicy, gotMetric)
}

// TestInMemoryBackend_SnapshotRestore_FullState is a full-state round-trip:
// it seeds a fully-populated container (every per-container sub-resource:
// policy, CORS, lifecycle, metric policy, tags, access logging) in two
// distinct regions, snapshots the backend, restores the snapshot into a
// fresh backend, and asserts every field survived -- including that region
// isolation is preserved (see store_setup.go for why containers is
// region-nested).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	const (
		regionEast = "us-east-1"
		regionWest = "us-west-2"
	)

	ctxEast := context.WithValue(t.Context(), regionContextKey{}, regionEast)
	ctxWest := context.WithValue(t.Context(), regionContextKey{}, regionWest)

	b := NewInMemoryBackend()

	eastSeed := seedRegion(ctxEast, t, b, regionEast)
	westSeed := seedRegion(ctxWest, t, b, regionWest)

	data := b.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restored := NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), data))

	assertRegionRestored(ctxEast, t, restored, regionEast, eastSeed)
	assertRegionRestored(ctxWest, t, restored, regionWest, westSeed)

	// Region isolation must survive the round-trip: east's list must not
	// contain west's container and vice versa.
	eastList, _, err := restored.ListContainers(ctxEast, "", 0)
	require.NoError(t, err)
	assert.Equal(t, []string{eastSeed.name}, containerNames(eastList))

	westList, _, err := restored.ListContainers(ctxWest, "", 0)
	require.NoError(t, err)
	assert.Equal(t, []string{westSeed.name}, containerNames(westList))
}

// TestInMemoryBackend_SnapshotRestore_EmptyBackend verifies that snapshotting
// and restoring a backend with no containers at all round-trips cleanly to
// an empty backend, rather than erroring or leaving stale state.
func TestInMemoryBackend_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), regionContextKey{}, testRegion)

	b := NewInMemoryBackend()
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	restored := NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), data))

	items, _, err := restored.ListContainers(ctx, "", 0)
	require.NoError(t, err)
	assert.Empty(t, items)
}

// TestHandler_SnapshotRestore verifies Handler.Snapshot/Restore delegate to
// the backend -- this is the dead-wiring fix: before Phase 3.3, MediaStore
// had no persistence at all, so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable for a Snapshot/Restore
// pair) never picked MediaStore up.
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), regionContextKey{}, testRegion)

	backend := NewInMemoryBackend()
	h := NewHandler(backend)

	_, err := backend.CreateContainer(ctx, testAccountID, "handler-container", nil)
	require.NoError(t, err)

	data := h.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restoredBackend := NewInMemoryBackend()
	restoredHandler := NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(t.Context(), data))

	got, err := restoredBackend.DescribeContainer(ctx, "handler-container")
	require.NoError(t, err)
	assert.Equal(t, "handler-container", got.Name)
}
