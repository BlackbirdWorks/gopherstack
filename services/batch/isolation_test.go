package batch //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region under the backend's
// region context key, mimicking what the HTTP handler injects from SigV4.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// TestBatchComputeEnvironmentRegionIsolation verifies that compute environments,
// job queues, job definitions and jobs with the SAME NAME in two different
// regions are fully isolated from each other.
func TestBatchComputeEnvironmentRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create a compute environment named "ce1" in us-east-1.
	eastCE, err := backend.CreateComputeEnvironment(ctxEast, "ce1", "MANAGED", "ENABLED", nil, "", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastCE.ComputeEnvironmentArn, "us-east-1")

	// 2. Create a CE with the SAME NAME in us-west-2 — must not collide.
	westCE, err := backend.CreateComputeEnvironment(ctxWest, "ce1", "MANAGED", "ENABLED", nil, "", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westCE.ComputeEnvironmentArn, "us-west-2")
	assert.NotEqual(t, eastCE.ComputeEnvironmentArn, westCE.ComputeEnvironmentArn)

	// 3. Each region sees only its own CE.
	eastList, _ := backend.DescribeComputeEnvironments(ctxEast, nil, 0, "")
	require.Len(t, eastList, 1)
	assert.Contains(t, eastList[0].ComputeEnvironmentArn, "us-east-1")

	westList, _ := backend.DescribeComputeEnvironments(ctxWest, nil, 0, "")
	require.Len(t, westList, 1)
	assert.Contains(t, westList[0].ComputeEnvironmentArn, "us-west-2")

	// 4. Deleting the CE in us-east-1 (after disabling) leaves us-west-2 intact.
	_, err = backend.UpdateComputeEnvironment(ctxEast, "ce1", "DISABLED", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, backend.DeleteComputeEnvironment(ctxEast, "ce1"))

	eastAfter, _ := backend.DescribeComputeEnvironments(ctxEast, nil, 0, "")
	assert.Empty(t, eastAfter)

	westAfter, _ := backend.DescribeComputeEnvironments(ctxWest, nil, 0, "")
	assert.Len(t, westAfter, 1)
}

// TestBatchJobRegionIsolation verifies that the cross-index maps (jobsByQueue,
// jobsByARN) stay region-scoped: a job submitted to a same-named queue in one
// region is invisible from another region.
func TestBatchJobRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// Same-named queue in each region.
	_, err := backend.CreateJobQueue(ctxEast, "queue1", 1, "ENABLED", nil, nil, "", nil, "", nil)
	require.NoError(t, err)

	_, err = backend.CreateJobQueue(ctxWest, "queue1", 1, "ENABLED", nil, nil, "", nil, "", nil)
	require.NoError(t, err)

	// Same-named job definition in each region.
	_, err = backend.RegisterJobDefinition(
		ctxEast,
		"jd1",
		"container",
		nil,
		nil,
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
	)
	require.NoError(t, err)

	_, err = backend.RegisterJobDefinition(
		ctxWest,
		"jd1",
		"container",
		nil,
		nil,
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
	)
	require.NoError(t, err)

	// Submit a job to queue1 in us-east-1 only.
	eastJob, err := backend.SubmitJob(
		ctxEast, "job1", "queue1", "jd1",
		nil, nil, nil, nil, nil, nil, nil, nil, "", 0, false,
	)
	require.NoError(t, err)
	assert.Contains(t, eastJob.JobARN, "us-east-1")

	// us-east-1 sees the job; us-west-2 does not (cross-index isolation).
	// job1 is still SUBMITTED (never scheduled); real AWS Batch's unfiltered
	// ListJobs defaults to RUNNING-only, so filter explicitly.
	eastJobs, _, err := backend.ListJobs(ctxEast, "queue1", "SUBMITTED", "", 0)
	require.NoError(t, err)
	require.Len(t, eastJobs, 1)
	assert.Equal(t, "job1", eastJobs[0].JobName)

	westJobs, _, err := backend.ListJobs(ctxWest, "queue1", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, westJobs)

	// The job ARN is resolvable in its own region but not the other (jobsByARN).
	eastDescribe := backend.DescribeJobs(ctxEast, []string{eastJob.JobARN})
	require.Len(t, eastDescribe, 1)

	westDescribe := backend.DescribeJobs(ctxWest, []string{eastJob.JobARN})
	assert.Empty(t, westDescribe)
}

// TestBatchSchedulingPolicyRegionIsolation verifies the schedulingPolicyByName
// cross-index map is region-scoped: a same-named policy can exist in two regions.
func TestBatchSchedulingPolicyRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	eastSP, err := backend.CreateSchedulingPolicy(ctxEast, "policy1", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, eastSP.Arn, "us-east-1")

	// Same name in us-west-2 must succeed (no collision via name index).
	westSP, err := backend.CreateSchedulingPolicy(ctxWest, "policy1", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, westSP.Arn, "us-west-2")

	eastPolicies := backend.ListSchedulingPolicies(ctxEast)
	require.Len(t, eastPolicies, 1)
	assert.Contains(t, eastPolicies[0].Arn, "us-east-1")

	westPolicies := backend.ListSchedulingPolicies(ctxWest)
	require.Len(t, westPolicies, 1)
	assert.Contains(t, westPolicies[0].Arn, "us-west-2")

	// Deleting in us-east-1 frees the name there but leaves us-west-2 intact,
	// and lets us-east-1 recreate the same name.
	require.NoError(t, backend.DeleteSchedulingPolicy(ctxEast, eastSP.Arn))

	assert.Empty(t, backend.ListSchedulingPolicies(ctxEast))
	assert.Len(t, backend.ListSchedulingPolicies(ctxWest), 1)

	_, err = backend.CreateSchedulingPolicy(ctxEast, "policy1", nil, nil, nil)
	require.NoError(t, err)
}

// TestBatchDefaultRegionFallback verifies that an empty context falls back to the
// backend's configured default region (single-region behaviour is unchanged).
func TestBatchDefaultRegionFallback(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	// No region in context → uses default region us-east-1.
	ce, err := backend.CreateComputeEnvironment(
		context.Background(), "ce1", "MANAGED", "ENABLED", nil, "", nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, ce.ComputeEnvironmentArn, "us-east-1")

	// A context explicitly carrying the default region sees the same resource.
	list, _ := backend.DescribeComputeEnvironments(ctxRegion("us-east-1"), nil, 0, "")
	require.Len(t, list, 1)
}
