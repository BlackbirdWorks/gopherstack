package batch_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := batch.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state (every store.Table
// plus the raw jobDefRevisions/cesByARN-family maps) and Restore returns no
// error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := batch.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateComputeEnvironment(t.Context(), "ce1", "MANAGED", "ENABLED", nil, "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.RegisterJobDefinition(
		t.Context(), "jd1", "container", nil, nil, 0, 0, nil, nil, nil, nil, nil, nil, false,
	)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.JobDefinitionCount())
	assert.False(t, b.HasRevisionCounter("jd1"))

	list, _ := b.DescribeComputeEnvironments(t.Context(), nil, 0, "")
	assert.Empty(t, list)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape) decodes
// with Version == 0, which mismatches batchSnapshotVersion and is discarded
// the same way any other incompatible version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := batch.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateComputeEnvironment(t.Context(), "ce1", "MANAGED", "ENABLED", nil, "", nil, nil, nil)
	require.NoError(t, err)

	// Pre-Phase-3.3 shape: plain region-nested resource maps, no "version" or
	// "tables" key.
	err = b.Restore(t.Context(), []byte(
		`{"computeEnvironments":{"us-east-1":{"ce1":{"computeEnvironmentName":"ce1"}}}}`,
	))
	require.NoError(t, err)

	list, _ := b.DescribeComputeEnvironments(t.Context(), nil, 0, "")
	assert.Empty(t, list)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (computeEnvironments, jobQueues, jobDefinitions, jobs,
// consumableResources, schedulingPolicies, serviceEnvironments, serviceJobs)
// plus the raw jobDefRevisions map left un-converted, and verifies every
// store.Index rebuilt by Table.Restore (byRegion, byARN, byQueue, byName)
// still resolves correctly afterward.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := batch.NewInMemoryBackend("111122223333", "us-west-2")

	ce, err := original.CreateComputeEnvironment(
		t.Context(), "ce-1", "MANAGED", "ENABLED", map[string]string{"env": "prod"}, "role-arn", nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Contains(t, ce.ComputeEnvironmentArn, "111122223333")

	jq, err := original.CreateJobQueue(t.Context(), "queue-1", 1, "ENABLED", []batch.ComputeEnvironmentOrder{
		{ComputeEnvironment: "ce-1", Order: 1},
	}, map[string]string{"team": "data"}, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "queue-1", jq.JobQueueName)

	jd, err := original.RegisterJobDefinition(
		t.Context(), "jd-1", "container", map[string]string{"k": "v"}, nil, 60, 0, nil, nil, nil, nil, nil, nil, false,
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), jd.Revision)

	require.NoError(t, original.DeregisterJobDefinition(t.Context(), jd.JobDefinitionArn))

	job, err := original.SubmitJob(
		t.Context(), "job-1", "queue-1", jd.JobDefinitionArn,
		map[string]string{"owner": "me"}, nil, nil, nil, nil, nil, nil, nil, "", 0, false,
	)
	require.NoError(t, err)
	assert.Contains(t, job.JobARN, "111122223333")

	cr, err := original.CreateConsumableResource(t.Context(), "cr-1", "REPLENISHABLE", 10, nil)
	require.NoError(t, err)

	sp, err := original.CreateSchedulingPolicy(t.Context(), "sp-1", nil, &batch.FairsharePolicy{
		ShareDecaySeconds: 60,
	})
	require.NoError(t, err)

	se, err := original.CreateServiceEnvironment(t.Context(), "se-1", "SAGEMAKER_TRAINING", "ENABLED", nil)
	require.NoError(t, err)

	sj, err := original.SubmitServiceJob(t.Context(), "sj-1", se.ServiceEnvironmentName, nil)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := batch.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Scalars.
	assert.Equal(t, "us-west-2", fresh.Region())

	// Every Describe*/List* call below passes a context with no region value,
	// which resolves to the backend's own (restored) default region --
	// "us-west-2", per the Region() assertion above.

	// computeEnvironments table (+ byRegion index, exercised via the empty-names
	// pagination branch).
	ceList, _ := fresh.DescribeComputeEnvironments(t.Context(), nil, 0, "")
	require.Len(t, ceList, 1)
	assert.Equal(t, "ce-1", ceList[0].ComputeEnvironmentName)
	assert.Equal(t, "prod", ceList[0].Tags["env"])

	// jobQueues table.
	jqList, _ := fresh.DescribeJobQueues(t.Context(), []string{"queue-1"}, 0, "")
	require.Len(t, jqList, 1)
	assert.Equal(t, "queue-1", jqList[0].JobQueueName)

	// jobDefinitions table: name and INACTIVE status (set via DeregisterJobDefinition
	// before the snapshot) both survive the round trip.
	jdList, _ := fresh.DescribeJobDefinitions(t.Context(), []string{jd.JobDefinitionArn}, "", "", 0, "")
	require.Len(t, jdList, 1)
	assert.Equal(t, "INACTIVE", jdList[0].Status)

	// jobDefRevisions (raw, un-converted map): the counter for jd-1 survives.
	assert.Equal(t, int32(1), fresh.RevisionFor("jd-1"))

	// jobs table + byQueue index (ListJobs by queue) + byARN index (DescribeJobs by ARN).
	jobsInQueue, _, err := fresh.ListJobs(t.Context(), "queue-1", "", "", 0)
	require.NoError(t, err)
	require.Len(t, jobsInQueue, 1)
	assert.Equal(t, "job-1", jobsInQueue[0].JobName)

	byARN := fresh.DescribeJobs(t.Context(), []string{job.JobARN})
	require.Len(t, byARN, 1)
	assert.Equal(t, "job-1", byARN[0].JobName)

	// consumableResources table.
	crGot, err := fresh.DescribeConsumableResource(t.Context(), "cr-1")
	require.NoError(t, err)
	assert.Equal(t, int64(10), crGot.TotalQuantity)
	assert.Equal(t, cr.ConsumableResourceArn, crGot.ConsumableResourceArn)

	// schedulingPolicies table + byName index: recreating "sp-1" in the same
	// region must fail with AlreadyExists, proving the byName index came back.
	spList := fresh.DescribeSchedulingPolicies(t.Context(), []string{sp.Arn})
	require.Len(t, spList, 1)
	assert.Equal(t, int32(60), spList[0].FairsharePolicy.ShareDecaySeconds)

	_, err = fresh.CreateSchedulingPolicy(t.Context(), "sp-1", nil, nil)
	require.ErrorIs(t, err, batch.ErrAlreadyExists)

	// serviceEnvironments table.
	seList := fresh.DescribeServiceEnvironments(t.Context(), []string{"se-1"})
	require.Len(t, seList, 1)
	assert.Equal(t, "se-1", seList[0].ServiceEnvironmentName)

	// serviceJobs table.
	sjGot, err := fresh.DescribeServiceJob(t.Context(), sj.ServiceJobID)
	require.NoError(t, err)
	assert.Equal(t, "sj-1", sjGot.ServiceJobName)
}

func TestBatch_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := batch.NewInMemoryBackend("000000000000", "us-east-1")

	// Create compute environment.
	ce, err := b.CreateComputeEnvironment(
		context.Background(), "test-ce", "MANAGED", "ENABLED", nil, "", nil, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ce.ComputeEnvironmentArn)

	// Create job queue.
	ceOrder := []batch.ComputeEnvironmentOrder{
		{ComputeEnvironment: ce.ComputeEnvironmentArn, Order: 1},
	}
	jq, err := b.CreateJobQueue(context.Background(), "test-jq", 10, "ENABLED", ceOrder, nil, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, jq.JobQueueArn)

	// Register job definition.
	jd, err := b.RegisterJobDefinition(
		context.Background(), "test-jd", "container", nil, nil, 0, 0, nil, nil, nil, nil, nil, nil, false)
	require.NoError(t, err)
	require.NotEmpty(t, jd.JobDefinitionArn)

	// Submit a job.
	job, err := b.SubmitJob(
		context.Background(),
		"test-job",
		jq.JobQueueName,
		jd.JobDefinitionArn,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		"",
		0,
		false,
	)
	require.NoError(t, err)
	require.NotEmpty(t, job.JobID)

	// Snapshot and restore.
	h := batch.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := batch.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := batch.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Compute environment is restored.
	ces, _ := b2.DescribeComputeEnvironments(context.Background(), []string{"test-ce"}, 0, "")
	require.Len(t, ces, 1)
	assert.Equal(t, "test-ce", ces[0].ComputeEnvironmentName)

	// Job queue is restored.
	jqs, _ := b2.DescribeJobQueues(context.Background(), []string{"test-jq"}, 0, "")
	require.Len(t, jqs, 1)
	assert.Equal(t, "test-jq", jqs[0].JobQueueName)

	// Job definition is restored.
	jds, _ := b2.DescribeJobDefinitions(context.Background(), []string{"test-jd"}, "", "", 0, "")
	require.NotEmpty(t, jds)
	assert.Equal(t, "test-jd", jds[0].JobDefinitionName)

	// Submitted job is restored.
	jobs := b2.DescribeJobs(context.Background(), []string{job.JobID})
	require.Len(t, jobs, 1)
	assert.Equal(t, "test-job", jobs[0].JobName)
	assert.Equal(t, jq.JobQueueName, jobs[0].JobQueue)

	// jobsByQueue index is rebuilt — ListJobs must return the submitted job.
	listed, _, err := b2.ListJobs(context.Background(), jq.JobQueueName, "", "", 0)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, job.JobID, listed[0].JobID)
}

// --- ConsumableResource tests ---

func TestBatch_PersistenceWithNewResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create resources of each new type.
	rec := post(t, h, "/v1/createconsumableresource", map[string]any{
		"consumableResourceName": "cr-persist",
		"totalQuantity":          int64(42),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createschedulingpolicy", map[string]any{
		"name": "sp-persist",
		"tags": map[string]string{"k": "v"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createserviceenvironment", map[string]any{
		"serviceEnvironmentName": "senv-persist",
		"serviceEnvironmentType": "SAGEMAKER_TRAINING",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Snapshot and restore.
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify consumable resource is restored.
	rec2 := post(t, h2, "/v1/describeconsumableresource", map[string]any{
		"consumableResource": "cr-persist",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var crOut map[string]any
	mustUnmarshal(t, rec2, &crOut)
	assert.Equal(t, "cr-persist", crOut["consumableResourceName"])
	assert.EqualValues(t, 42, crOut["totalQuantity"])

	// Verify scheduling policy is restored via describe.
	rec3 := post(t, h2, "/v1/describeschedulingpolicies", map[string]any{})
	require.Equal(t, http.StatusOK, rec3.Code)

	var spList map[string]any
	mustUnmarshal(t, rec3, &spList)
	items := spList["schedulingPolicies"].([]any)
	assert.Len(t, items, 1)

	// Verify scheduling policy name index is rebuilt (dedup by name should work).
	rec4 := post(t, h2, "/v1/createschedulingpolicy", map[string]any{"name": "sp-persist"})
	assert.Equal(t, http.StatusBadRequest, rec4.Code)
}

// --- ResourceTypeValidation tests ---
