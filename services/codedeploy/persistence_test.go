package codedeploy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty (plus re-seeded built-in
// deployment configs) and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateApplication("seed-app", "Server", nil)
	require.NoError(t, err)

	builtInCount := b.DeploymentConfigCount()

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListApplications())
	_, err = b.GetApplication("seed-app")
	require.Error(t, err)

	// Built-in deployment configs are re-seeded, same as a fresh backend.
	assert.Equal(t, builtInCount, b.DeploymentConfigCount())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches codedeploySnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape (flat resource maps keyed by name), which
// lacks "version"/"tables" entirely.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateApplication("seed-app", "Server", nil)
	require.NoError(t, err)

	// The pre-refactor shape: flat maps, no version/tables keys.
	oldShape := `{"applications":{"seed-app":{"applicationName":"seed-app"}},"region":"us-east-1"}`
	err = b.Restore(t.Context(), []byte(oldShape))
	require.NoError(t, err)

	assert.Empty(t, b.ListApplications())
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (deployments, deploymentConfigs -- "clean" tables --
// plus the "dirty" applications, deploymentGroups, and onPremisesInstances
// tables, each carrying a live *tags.Tags field round-tripped through a DTO)
// and the plain githubTokens set left unconverted.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := codedeploy.NewInMemoryBackend("111122223333", "us-west-2")
	ctx := t.Context()

	app, err := original.CreateApplication("app-1", "Server", map[string]string{"env": "test"})
	require.NoError(t, err)

	dg, err := original.CreateDeploymentGroup("app-1", "dg-1", codedeploy.DeploymentGroupInput{
		ServiceRoleArn: "arn:aws:iam::111122223333:role/CodeDeployRole",
	}, map[string]string{"team": "core"})
	require.NoError(t, err)

	deployment, err := original.CreateDeployment("app-1", "dg-1", codedeploy.DeploymentOptions{
		Description: "initial rollout",
		Creator:     "user",
	})
	require.NoError(t, err)

	cfg, err := original.CreateDeploymentConfig(
		"custom-config", "Server",
		&codedeploy.MinimumHealthyHosts{Type: "HOST_COUNT", Value: 2},
		nil, nil,
	)
	require.NoError(t, err)
	builtInCount := original.DeploymentConfigCount() - 1 // minus the custom config just created

	require.NoError(t, original.RegisterOnPremisesInstance("instance-1", "", "arn:aws:iam::111122223333:user/foo"))
	require.NoError(t, original.AddTagsToOnPremisesInstances([]string{"instance-1"}, map[string]string{"rack": "a1"}))

	original.AddGitHubAccountTokenInternal("gh-token-1")

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(ctx, snap))

	// Scalars: accountID/region surface indirectly through a freshly created
	// resource (there is no direct accessor).
	newApp, err := fresh.CreateApplication("app-2", "Server", nil)
	require.NoError(t, err)
	assert.Equal(t, "111122223333", newApp.AccountID)
	assert.Equal(t, "us-west-2", newApp.Region)

	// applications table ("dirty": hidden-from-JSON Tags field via a DTO).
	gotApp, err := fresh.GetApplication("app-1")
	require.NoError(t, err)
	assert.Equal(t, app.ApplicationID, gotApp.ApplicationID)
	tagVals, err := fresh.ListTagsForResource(fresh.ApplicationARN("app-1"))
	require.NoError(t, err)
	assert.Equal(t, "test", tagVals["env"])

	freshApps := fresh.ListApplications()
	assert.ElementsMatch(t, []string{"app-1", "app-2"}, freshApps)

	// deploymentGroups table ("dirty": composite key + byApplication index
	// rebuilt from the DTO's own ApplicationName/DeploymentGroupName fields).
	gotDG, err := fresh.GetDeploymentGroup("app-1", "dg-1")
	require.NoError(t, err)
	assert.Equal(t, dg.DeploymentGroupID, gotDG.DeploymentGroupID)
	assert.Equal(t, "arn:aws:iam::111122223333:role/CodeDeployRole", gotDG.ServiceRoleArn)
	dgTagVals, err := fresh.ListTagsForResource(fresh.DeploymentGroupARN("app-1", "dg-1"))
	require.NoError(t, err)
	assert.Equal(t, "core", dgTagVals["team"])

	dgNames, err := fresh.ListDeploymentGroups("app-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"dg-1"}, dgNames)

	// deployments table ("clean": DeploymentID is a real wire field).
	gotDeployment, err := fresh.GetDeployment(deployment.DeploymentID)
	require.NoError(t, err)
	assert.Equal(t, "initial rollout", gotDeployment.Description)

	// deploymentConfigs table ("clean"): built-ins plus the custom config,
	// not duplicated or dropped.
	assert.Equal(t, builtInCount+1, fresh.DeploymentConfigCount())
	gotCfg, err := fresh.GetDeploymentConfig("custom-config")
	require.NoError(t, err)
	assert.Equal(t, cfg.DeploymentConfigID, gotCfg.DeploymentConfigID)
	require.NotNil(t, gotCfg.MinimumHealthyHosts)
	assert.Equal(t, 2, gotCfg.MinimumHealthyHosts.Value)

	// onPremisesInstances table ("dirty": hidden-from-JSON Tags field via a DTO).
	gotInst, err := fresh.GetOnPremisesInstance("instance-1")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::111122223333:user/foo", gotInst.IamUserArn)
	require.NotNil(t, gotInst.Tags)
	assert.Equal(t, "a1", gotInst.Tags.Clone()["rack"])

	// githubTokens plain set (unconverted).
	assert.Equal(t, []string{"gh-token-1"}, fresh.ListGitHubAccountTokenNames())

	// UpdateApplication rename cascades to the restored deployment group and
	// deployment -- proves the byApplication index and each DTO's identity
	// fields were rebuilt correctly, not just the primary lookups.
	require.NoError(t, fresh.UpdateApplication("app-1", "app-1-renamed"))
	_, err = fresh.GetDeploymentGroup("app-1-renamed", "dg-1")
	require.NoError(t, err)
	renamedDeployment, err := fresh.GetDeployment(deployment.DeploymentID)
	require.NoError(t, err)
	assert.Equal(t, "app-1-renamed", renamedDeployment.ApplicationName)

	// DeleteApplication cascade still works post-restore/post-rename.
	require.NoError(t, fresh.DeleteApplication("app-1-renamed"))
	_, err = fresh.GetDeploymentGroup("app-1-renamed", "dg-1")
	require.Error(t, err)
	_, err = fresh.GetApplication("app-1-renamed")
	require.Error(t, err)
}
