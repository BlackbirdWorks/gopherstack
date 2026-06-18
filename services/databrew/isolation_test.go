package databrew //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func databrewCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestDataBrewRegionIsolation proves that same-named DataBrew resources created
// in two different regions are fully isolated: each region sees only its own
// resources, ARNs embed the correct region, and deleting in one region leaves
// the other untouched.
func TestDataBrewRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := databrewCtxRegion("us-east-1")
	ctxWest := databrewCtxRegion("us-west-2")

	// 1. Create a dataset with the SAME name in both regions.
	eastDS, err := backend.CreateDataset(
		ctxEast,
		"shared-ds",
		"CSV",
		DatasetInput{},
		DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	assert.Contains(t, eastDS.Arn, "us-east-1")

	westDS, err := backend.CreateDataset(
		ctxWest,
		"shared-ds",
		"JSON",
		DatasetInput{},
		DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	assert.Contains(t, westDS.Arn, "us-west-2")

	// ARNs must differ even though names match.
	assert.NotEqual(t, eastDS.Arn, westDS.Arn)

	// 2. Each region reads back its own format.
	eastGot, err := backend.DescribeDataset(ctxEast, "shared-ds")
	require.NoError(t, err)
	assert.Equal(t, "CSV", eastGot.Format)

	westGot, err := backend.DescribeDataset(ctxWest, "shared-ds")
	require.NoError(t, err)
	assert.Equal(t, "JSON", westGot.Format)

	// 3. Listing returns exactly one dataset per region.
	eastList, _ := backend.ListDatasets(ctxEast, 0, "")
	require.Len(t, eastList, 1)

	westList, _ := backend.ListDatasets(ctxWest, 0, "")
	require.Len(t, westList, 1)

	// 4. Deleting in us-east-1 must not affect us-west-2.
	require.NoError(t, backend.DeleteDataset(ctxEast, "shared-ds"))

	_, err = backend.DescribeDataset(ctxEast, "shared-ds")
	require.ErrorIs(t, err, ErrNotFound)

	westStill, err := backend.DescribeDataset(ctxWest, "shared-ds")
	require.NoError(t, err)
	assert.Equal(t, "JSON", westStill.Format)
}

// TestDataBrewJobRegionIsolation proves that jobs and job runs are isolated
// per region.
func TestDataBrewJobRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := databrewCtxRegion("us-east-1")
	ctxWest := databrewCtxRegion("us-west-2")

	// Create same-named jobs in both regions.
	eastJob, err := backend.CreateJob(ctxEast, "shared-job", "RECIPE", "", "", "", "", nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastJob.Arn, "us-east-1")

	westJob, err := backend.CreateJob(ctxWest, "shared-job", "PROFILE", "", "", "", "", nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westJob.Arn, "us-west-2")

	// Each region sees its own job type.
	eastGot, err := backend.DescribeJob(ctxEast, "shared-job")
	require.NoError(t, err)
	assert.Equal(t, "RECIPE", eastGot.Type)

	westGot, err := backend.DescribeJob(ctxWest, "shared-job")
	require.NoError(t, err)
	assert.Equal(t, "PROFILE", westGot.Type)

	// Job runs are isolated: start a run in east only.
	run, err := backend.StartJobRun(ctxEast, "shared-job")
	require.NoError(t, err)

	eastRuns, _, err := backend.ListJobRuns(ctxEast, "shared-job", 0, "")
	require.NoError(t, err)
	require.Len(t, eastRuns, 1)
	assert.Equal(t, run.RunID, eastRuns[0].RunID)

	westRuns, _, err := backend.ListJobRuns(ctxWest, "shared-job", 0, "")
	require.NoError(t, err)
	assert.Empty(t, westRuns)

	// Deleting in east does not affect west.
	require.NoError(t, backend.DeleteJob(ctxEast, "shared-job"))

	_, err = backend.DescribeJob(ctxEast, "shared-job")
	require.ErrorIs(t, err, ErrNotFound)

	westStill, err := backend.DescribeJob(ctxWest, "shared-job")
	require.NoError(t, err)
	assert.Equal(t, "PROFILE", westStill.Type)
}

// TestDataBrewDefaultRegionFallback verifies that a context without a region
// falls back to the backend's configured default region.
func TestDataBrewDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "eu-central-1")

	// No region in context -> default region store.
	_, err := backend.CreateDataset(
		context.Background(),
		"def-ds",
		"CSV",
		DatasetInput{},
		DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)

	// Explicit default region context sees it.
	list, _ := backend.ListDatasets(databrewCtxRegion("eu-central-1"), 0, "")
	require.Len(t, list, 1)
	assert.Contains(t, list[0].Arn, "eu-central-1")

	// A different region sees nothing.
	other, _ := backend.ListDatasets(databrewCtxRegion("ap-south-1"), 0, "")
	assert.Empty(t, other)
}
