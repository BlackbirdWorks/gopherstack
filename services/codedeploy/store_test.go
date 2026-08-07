package codedeploy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	assert.Equal(t, config.DefaultRegion, b.Region())
}

// TestStore_Reset lives in whitebox_test.go: it needs direct access to the
// unexported applicationRevisions map.

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
