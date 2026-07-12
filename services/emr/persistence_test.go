package emr_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.RunJobFlow(t.Context(), emr.RunJobFlowParams{Name: "seed-cluster"})
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ClusterCount())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape) decodes
// with Version == 0, which mismatches emrSnapshotVersion and is discarded
// the same way any other incompatible version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.RunJobFlow(t.Context(), emr.RunJobFlowParams{Name: "seed-cluster"})
	require.NoError(t, err)

	// Pre-Phase-3.3 shape: plain region-nested resource maps, no "version" or
	// "tables" key.
	err = b.Restore(t.Context(), []byte(`{"clusters":{"us-east-1":{"j-1":{"Id":"j-1","Name":"old"}}}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ClusterCount())
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (clusters, securityConfigs, studios,
// studioSessionMappings, persistentAppUIs, notebookExecutions,
// blockPublicAccess, blockPublicAccessMeta) plus the raw arnIndex map left
// un-converted, and the unexported extra Cluster fields (managed scaling
// policy, auto-termination policy, instance groups/fleets, steps, bootstrap
// actions) formerly carried via a separate clusterExtra side-table.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := emr.NewInMemoryBackend("111122223333", "us-west-2")

	cluster, err := original.RunJobFlow(t.Context(), emr.RunJobFlowParams{
		Name:         "chan-1",
		ReleaseLabel: "emr-7.3.0",
		Instances: emr.RunJobFlowInstances{
			InstanceGroups: []emr.InstanceGroupSpec{
				{Name: "core", InstanceRole: "CORE", InstanceType: "m5.xlarge", InstanceCount: 2},
			},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, cluster.ARN, "111122223333")

	stepIDs, err := original.AddJobFlowSteps(t.Context(), cluster.ID, []emr.StepSpec{
		{Name: "step-1", HadoopJarStep: emr.StepHadoopJarStep{Jar: "s3://bucket/job.jar"}},
	})
	require.NoError(t, err)
	require.Len(t, stepIDs, 1)

	_, _, err = original.AddInstanceFleet(t.Context(), cluster.ID, emr.InstanceFleetSpec{
		Name: "fleet-1", InstanceFleetType: "TASK",
	})
	require.NoError(t, err)

	require.NoError(t, original.PutManagedScalingPolicy(t.Context(), cluster.ID, emr.ManagedScalingPolicy{
		ComputeLimits: emr.ComputeLimits{MinimumCapacityUnits: 1, MaximumCapacityUnits: 10},
	}))

	require.NoError(t, original.PutAutoTerminationPolicy(t.Context(), cluster.ID, emr.AutoTerminationPolicy{
		IdleTimeout: 3600,
	}))

	sc, err := original.CreateSecurityConfiguration(t.Context(), "sc-1", `{"EncryptionConfiguration":{}}`)
	require.NoError(t, err)

	studio, err := original.CreateStudio(
		t.Context(), "studio-1", "SSO", "s3://bucket/studio", "sg-eng", "role-arn", "vpc-1", "sg-workspace",
		[]string{"subnet-1"}, nil,
	)
	require.NoError(t, err)

	require.NoError(t, original.CreateStudioSessionMapping(
		t.Context(), studio.StudioID, "USER", "user-1", "", "policy-arn",
	))

	ui, err := original.CreatePersistentAppUI(t.Context(), cluster.ARN)
	require.NoError(t, err)

	ne, err := original.StartNotebookExecution(t.Context(), "editor-1", "exec-1", "{}", "engine-1", nil)
	require.NoError(t, err)

	require.NoError(t, original.PutBlockPublicAccessConfiguration(t.Context(), emr.BlockPublicAccessConfiguration{
		BlockPublicSecurityGroupRules: true,
	}))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := emr.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Scalars.
	assert.Equal(t, "us-west-2", fresh.Region())

	// clusters table, plus the unexported extra fields formerly carried via
	// clusterExtra (instance groups/fleets, steps, managed-scaling and
	// auto-termination policies).
	gotCluster, err := fresh.DescribeCluster(t.Context(), cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "chan-1", gotCluster.Name)

	groups, err := fresh.ListInstanceGroups(t.Context(), cluster.ID)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "core", groups[0].Name)

	fleets, err := fresh.ListInstanceFleets(t.Context(), cluster.ID)
	require.NoError(t, err)
	require.Len(t, fleets, 1)
	assert.Equal(t, "fleet-1", fleets[0].Name)

	steps, _ := fresh.ListSteps(t.Context(), cluster.ID, nil, nil, "")
	require.Len(t, steps, 1)
	assert.Equal(t, "step-1", steps[0].Name)

	msp, err := fresh.GetManagedScalingPolicy(t.Context(), cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, 10, msp.ComputeLimits.MaximumCapacityUnits)

	atp, err := fresh.GetAutoTerminationPolicy(t.Context(), cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3600), atp.IdleTimeout)

	// arnIndex: the raw, un-converted region-nested map -- looking a cluster
	// up by ARN (rather than ID) after restore proves it round-tripped.
	tags, err := fresh.ListTagsForResource(t.Context(), cluster.ARN)
	require.NoError(t, err)
	assert.Empty(t, tags)

	// securityConfigs table.
	gotSC, err := fresh.DescribeSecurityConfiguration(t.Context(), sc.Name)
	require.NoError(t, err)
	assert.Equal(t, sc.SecurityConfig, gotSC.SecurityConfig)

	// studios table.
	gotStudio, err := fresh.DescribeStudio(t.Context(), studio.StudioID)
	require.NoError(t, err)
	assert.Equal(t, "studio-1", gotStudio.Name)

	// studioSessionMappings table.
	mapping, err := fresh.GetStudioSessionMapping(t.Context(), studio.StudioID, "USER", "user-1", "")
	require.NoError(t, err)
	assert.Equal(t, "policy-arn", mapping.SessionPolicyArn)

	// persistentAppUIs table.
	gotUI, err := fresh.DescribePersistentAppUI(t.Context(), ui.ID)
	require.NoError(t, err)
	assert.Equal(t, cluster.ARN, gotUI.TargetResourceArn)

	// notebookExecutions table.
	gotNE, err := fresh.DescribeNotebookExecution(t.Context(), ne.NotebookExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "exec-1", gotNE.NotebookExecutionName)

	// blockPublicAccess / blockPublicAccessMeta tables.
	cfg, meta := fresh.GetBlockPublicAccessConfiguration(t.Context())
	assert.True(t, cfg.BlockPublicSecurityGroupRules)
	assert.NotZero(t, meta.CreationDateTime)
}

// TestInMemoryBackend_SnapshotRestore_EmptyState verifies an empty backend
// round-trips to another empty backend without error.
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := emr.NewInMemoryBackend("000000000000", "us-east-1")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := emr.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 0, fresh.ClusterCount())
	assert.Equal(t, 0, fresh.SecurityConfigCount())
	assert.Equal(t, 0, fresh.StudioCount())
	assert.Equal(t, 0, fresh.PersistentAppUICount())
	assert.Equal(t, 0, fresh.StudioSessionMappingCount())
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which the persistence layer's setupPersistence actually calls,
// since it type-asserts the Registerable (Handler), not the backend directly
// -- correctly delegate to the backend. Before this conversion, EMR had no
// Handler.Snapshot/Restore at all, so it was never actually wired into the
// CLI's persistence manager despite InMemoryBackend implementing the
// Snapshot/Restore methods.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := emr.NewHandler(emr.NewInMemoryBackend("000000000000", "us-east-1"))

	_, err := h.Backend.RunJobFlow(t.Context(), emr.RunJobFlowParams{Name: "delegate-cluster"})
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := emr.NewHandler(emr.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.ClusterCount())
}

// TestInMemoryBackend_SnapshotRestore_RegionIsolation verifies that clusters
// in different regions remain isolated after a round trip through the
// composite "region|id" key store.Table conversion introduced -- the same
// isolation the old map[string]map[string]*Cluster nesting gave for free.
func TestInMemoryBackend_SnapshotRestore_RegionIsolation(t *testing.T) {
	t.Parallel()

	original := emr.NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := emr.WithRegionForTest(t.Context(), "us-east-1")
	ctxWest := emr.WithRegionForTest(t.Context(), "us-west-2")

	eastCluster, err := original.RunJobFlow(ctxEast, emr.RunJobFlowParams{Name: "east-cluster"})
	require.NoError(t, err)

	westCluster, err := original.RunJobFlow(ctxWest, emr.RunJobFlowParams{Name: "west-cluster"})
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := emr.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 2, fresh.ClusterCount())

	gotEast, err := fresh.DescribeCluster(ctxEast, eastCluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "east-cluster", gotEast.Name)

	gotWest, err := fresh.DescribeCluster(ctxWest, westCluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "west-cluster", gotWest.Name)

	// Cross-region lookup must still fail -- the composite key keeps
	// same-named/ID resources in different regions apart.
	_, err = fresh.DescribeCluster(ctxWest, eastCluster.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
