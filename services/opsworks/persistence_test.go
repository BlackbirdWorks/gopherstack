package opsworks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving IDs.
type persistenceFixtureIDs struct {
	stackID      string
	layerID      string
	instanceID   string
	appID        string
	deploymentID string
	iamUserArn   string
	elbName      string
	elasticIP    string
	volumeID     string
	rdsArn       string
	ecsArn       string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus
// the one raw map left un-converted (tags), so a Snapshot from it exercises
// the entire persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) (*opsworks.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := opsworks.NewInMemoryBackend("000000000000", "us-east-1")

	stack, err := b.CreateStack(
		"stack1", "us-east-1",
		"arn:aws:iam::000000000000:instance-profile/p",
		"arn:aws:iam::000000000000:role/r",
		opsworks.CreateStackOptions{
			VpcID:      "vpc-123",
			Attributes: map[string]string{"Color": "blue"},
			ConfigurationManager: &opsworks.StackConfigurationManager{
				Name: "Chef", Version: "12",
			},
			ChefConfiguration: &opsworks.ChefConfiguration{ManageBerkshelf: true, BerkshelfVersion: "5.1"},
		},
	)
	require.NoError(t, err)

	layer, err := b.CreateLayer(stack.StackID, "custom", "layer1", "layer1short")
	require.NoError(t, err)

	instance, err := b.CreateInstance(stack.StackID, layer.LayerID, "t2.micro")
	require.NoError(t, err)

	app, err := b.CreateApp(stack.StackID, "app1", "other")
	require.NoError(t, err)

	deployment, err := b.CreateDeployment(stack.StackID, app.AppID, "deploy") // also creates a command
	require.NoError(t, err)

	require.NoError(t, b.TagResource(stack.Arn, map[string]string{"env": "test"}))

	iamUserArn := "arn:aws:iam::000000000000:user/alice"
	_, err = b.CreateUserProfile(iamUserArn, "alice", "ssh-rsa AAAA", false)
	require.NoError(t, err)

	require.NoError(t, b.AttachElasticLoadBalancer("elb1", layer.LayerID))

	elasticIP := "203.0.113.5"
	_, err = b.RegisterElasticIP(elasticIP, "us-east-1")
	require.NoError(t, err)
	require.NoError(t, b.AssociateElasticIP(elasticIP, instance.InstanceID))

	volumeID, err := b.RegisterVolume("vol-0123456789", stack.StackID)
	require.NoError(t, err)
	require.NoError(t, b.AssignVolume(volumeID, instance.InstanceID))

	rdsArn := "arn:aws:rds:us-east-1:000000000000:db:mydb"
	require.NoError(t, b.RegisterRdsDBInstance(stack.StackID, rdsArn, "admin", "hunter2"))

	ecsArn := "arn:aws:ecs:us-east-1:000000000000:cluster/mycluster"
	_, err = b.RegisterEcsCluster(ecsArn, stack.StackID)
	require.NoError(t, err)

	require.NoError(t, b.SetPermission(stack.StackID, iamUserArn, "iam_only", true, false))

	require.NoError(t, b.SetTimeBasedAutoScaling(instance.InstanceID, &opsworks.AutoScalingSchedule{
		Monday: map[string]string{"00": "on"},
	}))

	require.NoError(t, b.SetLoadBasedAutoScaling(
		layer.LayerID, true,
		&opsworks.ScalingParameters{CPUThreshold: 80},
		&opsworks.ScalingParameters{CPUThreshold: 20},
	))

	return b, persistenceFixtureIDs{
		stackID:      stack.StackID,
		layerID:      layer.LayerID,
		instanceID:   instance.InstanceID,
		appID:        app.AppID,
		deploymentID: deployment.DeploymentID,
		iamUserArn:   iamUserArn,
		elbName:      "elb1",
		elasticIP:    elasticIP,
		volumeID:     volumeID,
		rdsArn:       rdsArn,
		ecsArn:       ecsArn,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (stacks, layers, instances, apps, deployments, commands,
// userProfiles, elasticLBs, elasticIPs, volumes, rdsDBInstances,
// ecsClusters, permissions, timeBasedAutoScale, loadBasedAutoScale), every
// secondary store.Index derived from them (byStack on layers, instances,
// apps, deployments, volumes, rdsDBInstances, ecsClusters, permissions), and
// the one raw map left un-converted (tags) through Snapshot -> Restore into
// a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opsworks.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// stacks table.
	stacks, err := fresh.DescribeStacks([]string{ids.stackID})
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, "stack1", stacks[0].Name)
	assert.Equal(t, "vpc-123", stacks[0].VpcID)
	assert.Equal(t, map[string]string{"Color": "blue"}, stacks[0].Attributes)
	require.NotNil(t, stacks[0].ConfigurationManager)
	assert.Equal(t, "Chef", stacks[0].ConfigurationManager.Name)
	assert.Equal(t, "12", stacks[0].ConfigurationManager.Version)
	require.NotNil(t, stacks[0].ChefConfiguration)
	assert.True(t, stacks[0].ChefConfiguration.ManageBerkshelf)
	assert.Equal(t, "5.1", stacks[0].ChefConfiguration.BerkshelfVersion)

	// layers table + layersByStack index.
	layers, err := fresh.DescribeLayers(ids.stackID, nil)
	require.NoError(t, err)
	require.Len(t, layers, 1)
	assert.Equal(t, "layer1", layers[0].Name)

	// instances table + instancesByStack index.
	instances, err := fresh.DescribeInstances(ids.stackID, "", nil)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.Equal(t, ids.instanceID, instances[0].InstanceID)

	// apps table + appsByStack index.
	apps, err := fresh.DescribeApps(ids.stackID, nil)
	require.NoError(t, err)
	require.Len(t, apps, 1)
	assert.Equal(t, "app1", apps[0].Name)

	// deployments table + deploymentsByStack index.
	deployments, err := fresh.DescribeDeployments(ids.stackID, "", nil)
	require.NoError(t, err)
	require.Len(t, deployments, 1)
	assert.Equal(t, ids.deploymentID, deployments[0].DeploymentID)

	// commands table (created as a side effect of CreateDeployment).
	commands, err := fresh.DescribeCommands(ids.deploymentID, "", nil)
	require.NoError(t, err)
	require.Len(t, commands, 1)

	// userProfiles table.
	profiles, err := fresh.DescribeUserProfiles([]string{ids.iamUserArn})
	require.NoError(t, err)
	require.Len(t, profiles, 1)
	assert.Equal(t, "alice", profiles[0].SSHUsername)

	// elasticLBs table.
	elbs, err := fresh.DescribeElasticLoadBalancers(ids.stackID, "")
	require.NoError(t, err)
	require.Len(t, elbs, 1)
	assert.Equal(t, ids.elbName, elbs[0].ElasticLoadBalancerName)

	// elasticIPs table.
	eips, err := fresh.DescribeElasticIps("", []string{ids.elasticIP})
	require.NoError(t, err)
	require.Len(t, eips, 1)
	assert.Equal(t, ids.instanceID, eips[0].InstanceID)

	// volumes table + volumesByStack index.
	volumes, err := fresh.DescribeVolumes("", "", "", []string{ids.volumeID})
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	assert.Equal(t, ids.instanceID, volumes[0].InstanceID)

	// rdsDBInstances table + rdsDBInstancesByStack index.
	rdsInstances, err := fresh.DescribeRdsDBInstances(ids.stackID, nil)
	require.NoError(t, err)
	require.Len(t, rdsInstances, 1)
	assert.Equal(t, ids.rdsArn, rdsInstances[0].RdsDBInstanceArn)

	// ecsClusters table + ecsClustersByStack index.
	ecsClusters, err := fresh.DescribeEcsClusters(ids.stackID, nil)
	require.NoError(t, err)
	require.Len(t, ecsClusters, 1)
	assert.Equal(t, ids.ecsArn, ecsClusters[0].EcsClusterArn)

	// permissions table + permissionsByStack index.
	perms, err := fresh.DescribePermissions(ids.stackID, ids.iamUserArn)
	require.NoError(t, err)
	require.Len(t, perms, 1)
	assert.True(t, perms[0].AllowSSH)

	// timeBasedAutoScale table.
	tbas, err := fresh.DescribeTimeBasedAutoScaling([]string{ids.instanceID})
	require.NoError(t, err)
	require.Len(t, tbas, 1)
	require.NotNil(t, tbas[0].AutoScalingSchedule)
	assert.Equal(t, "on", tbas[0].AutoScalingSchedule.Monday["00"])

	// loadBasedAutoScale table.
	lbas, err := fresh.DescribeLoadBasedAutoScaling([]string{ids.layerID})
	require.NoError(t, err)
	require.Len(t, lbas, 1)
	require.NotNil(t, lbas[0].UpScaling)
	assert.InDelta(t, 80, lbas[0].UpScaling.CPUThreshold, 0)

	// tags raw map (left un-converted, persisted alongside the tables).
	tags, _, err := fresh.ListTags(stacks[0].Arn, 0, "")
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, opsworks.StackCount(b))
	assert.Equal(t, 0, opsworks.LayerCount(b))
	assert.Equal(t, 0, opsworks.InstanceCount(b))
	assert.Equal(t, 0, opsworks.AppCount(b))
	assert.Equal(t, 0, opsworks.DeploymentCount(b))

	stacks, err := b.DescribeStacks(nil)
	require.NoError(t, err)
	assert.Empty(t, stacks)

	_, _, err = b.ListTags("arn:aws:opsworks:us-east-1:000000000000:stack/"+ids.stackID, 0, "")
	require.ErrorIs(t, err, opsworks.ErrStackNotFound)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := opsworks.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend. Before Phase 3.3, opsworks had no such delegation
// even though InMemoryBackend implemented both methods -- dead wiring, since
// nothing ever called them through the Handler/Persistable path the rest of
// the fleet uses.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := opsworks.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := opsworks.NewHandler(opsworks.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	stacks, err := h2.Backend.DescribeStacks([]string{ids.stackID})
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, "stack1", stacks[0].Name)
}
