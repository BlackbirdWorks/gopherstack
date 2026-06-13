package textract //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region under regionContextKey.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestTextractJobRegionIsolation proves that same-named jobs in two regions are
// fully isolated: each region sees only its own jobs, and the other region is
// unaffected by operations in one region.
func TestTextractJobRegionIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackendSync("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// Start a document analysis job in us-east-1.
	eastJob, err := b.StartDocumentAnalysis(ctxEast, "s3://bucket/doc.pdf")
	require.NoError(t, err)
	assert.NotEmpty(t, eastJob.JobID)

	// Start a document analysis job in us-west-2 (same S3 URI, different region).
	westJob, err := b.StartDocumentAnalysis(ctxWest, "s3://bucket/doc.pdf")
	require.NoError(t, err)
	assert.NotEmpty(t, westJob.JobID)

	// Jobs in different regions get different IDs.
	assert.NotEqual(t, eastJob.JobID, westJob.JobID)

	// us-east-1 can retrieve its job.
	fetchedEast, err := b.GetDocumentAnalysis(ctxEast, eastJob.JobID)
	require.NoError(t, err)
	assert.Equal(t, eastJob.JobID, fetchedEast.JobID)

	// us-west-2 can retrieve its job.
	fetchedWest, err := b.GetDocumentAnalysis(ctxWest, westJob.JobID)
	require.NoError(t, err)
	assert.Equal(t, westJob.JobID, fetchedWest.JobID)

	// us-east-1 cannot see us-west-2's job and vice versa.
	_, err = b.GetDocumentAnalysis(ctxEast, westJob.JobID)
	require.Error(t, err, "us-east-1 must not see us-west-2's job")

	_, err = b.GetDocumentAnalysis(ctxWest, eastJob.JobID)
	require.Error(t, err, "us-west-2 must not see us-east-1's job")

	// ListJobs is region-scoped.
	eastJobs := b.ListJobs(ctxEast)
	require.Len(t, eastJobs, 1)
	assert.Equal(t, eastJob.JobID, eastJobs[0].JobID)

	westJobs := b.ListJobs(ctxWest)
	require.Len(t, westJobs, 1)
	assert.Equal(t, westJob.JobID, westJobs[0].JobID)
}

// TestTextractAdapterRegionIsolation proves adapters are region-isolated: the
// same adapter name in two regions produces distinct resources that don't
// interfere with each other.
func TestTextractAdapterRegionIsolation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackendSync("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// Create an adapter named "model-adapter" in us-east-1.
	eastAdapter, err := b.CreateAdapter(
		ctxEast, "model-adapter", "east description", "DISABLED", []string{"FORMS"}, nil,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, eastAdapter.AdapterID)

	// Create an adapter with the same name in us-west-2.
	westAdapter, err := b.CreateAdapter(
		ctxWest, "model-adapter", "west description", "ENABLED", []string{"QUERIES"}, nil,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, westAdapter.AdapterID)

	// Each region gets its own adapter with distinct attributes.
	fetchedEast, err := b.GetAdapter(ctxEast, eastAdapter.AdapterID)
	require.NoError(t, err)
	assert.Equal(t, "east description", fetchedEast.Description)
	assert.Equal(t, "DISABLED", fetchedEast.AutoUpdate)

	fetchedWest, err := b.GetAdapter(ctxWest, westAdapter.AdapterID)
	require.NoError(t, err)
	assert.Equal(t, "west description", fetchedWest.Description)
	assert.Equal(t, "ENABLED", fetchedWest.AutoUpdate)

	// us-east-1 cannot see us-west-2's adapter.
	_, err = b.GetAdapter(ctxEast, westAdapter.AdapterID)
	require.Error(t, err, "us-east-1 must not see us-west-2's adapter")

	// Deleting the us-east-1 adapter leaves us-west-2 intact.
	require.NoError(t, b.DeleteAdapter(ctxEast, eastAdapter.AdapterID))

	_, err = b.GetAdapter(ctxEast, eastAdapter.AdapterID)
	require.Error(t, err, "us-east-1 adapter should be gone after deletion")

	_, err = b.GetAdapter(ctxWest, westAdapter.AdapterID)
	assert.NoError(t, err, "us-west-2 adapter must survive deletion in us-east-1")
}
