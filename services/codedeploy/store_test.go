package codedeploy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	assert.Equal(t, config.DefaultRegion, b.Region())
}

func TestStore_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("app-1", "Server", nil)
	_, _ = h.Backend.CreateApplication("app-2", "Lambda", nil)
	require.NoError(t, h.Backend.RegisterApplicationRevision("app-1", codedeploy.RevisionLocation{
		RevisionType: "S3",
		S3Location:   &codedeploy.RevisionS3Location{Bucket: "b", Key: "k"},
	}, ""))

	require.Equal(t, 2, h.Backend.ApplicationCount())
	require.Equal(t, 1, h.Backend.ApplicationRevisionCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ApplicationCount())
	assert.Equal(t, 0, h.Backend.DeploymentCount())
	assert.Equal(t, 0, h.Backend.ApplicationRevisionCount())
	// 9 CodeDeployDefault.* configs are re-seeded on every Reset/NewInMemoryBackend.
	assert.Equal(t, 9, h.Backend.DeploymentConfigCount())
}

func TestStore_SeedHelpersCounts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddApplicationInternal(&codedeploy.Application{
		ApplicationName: "seeded-app",
		ComputePlatform: "Lambda",
	})

	h.Backend.AddDeploymentGroupInternal(&codedeploy.DeploymentGroup{
		ApplicationName:     "seeded-app",
		DeploymentGroupName: "seeded-dg",
	})

	h.Backend.AddDeploymentInternal(&codedeploy.Deployment{
		ApplicationName:     "seeded-app",
		DeploymentGroupName: "seeded-dg",
		Status:              "InProgress",
	})

	h.Backend.AddOnPremisesInstanceInternal(&codedeploy.OnPremisesInstance{
		InstanceName: "my-server",
	})

	h.Backend.AddDeploymentConfigInternal(&codedeploy.DeploymentConfig{
		DeploymentConfigName: "my-config",
		ComputePlatform:      "ECS",
	})

	assert.Equal(t, 1, h.Backend.ApplicationCount())
	assert.Equal(t, 1, h.Backend.DeploymentGroupCount("seeded-app"))
	assert.Equal(t, 1, h.Backend.DeploymentCount())
	assert.Equal(t, 1, h.Backend.OnPremisesInstanceCount())
	// 9 CodeDeployDefault.* pre-seeded configs + 1 added here.
	assert.Equal(t, 10, h.Backend.DeploymentConfigCount())
}
