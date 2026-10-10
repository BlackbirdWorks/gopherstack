package codedeploy_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// readyWaitGroupInput holds new deployments in Ready until ContinueDeployment or StopDeployment.
func readyWaitGroupInput() codedeploy.DeploymentGroupInput {
	return codedeploy.DeploymentGroupInput{
		ServiceRoleArn:  "arn:aws:iam::000000000000:role/role",
		DeploymentStyle: &codedeploy.DeploymentStyle{DeploymentType: "BLUE_GREEN"},
		BlueGreenDeploymentConfiguration: &codedeploy.BlueGreenDeploymentConfiguration{
			DeploymentReadyOption: &codedeploy.DeploymentReadyOption{ActionOnTimeout: "STOP_DEPLOYMENT"},
		},
	}
}

func createReadyWaitDeployment(t *testing.T, b *codedeploy.InMemoryBackend, app, dg string) string {
	t.Helper()

	_, err := b.CreateApplication(app, "Server", nil)
	require.NoError(t, err)

	_, err = b.CreateDeploymentGroup(app, dg, readyWaitGroupInput(), nil)
	require.NoError(t, err)

	d, err := b.CreateDeployment(app, dg, codedeploy.DeploymentOptions{Creator: "user"})
	require.NoError(t, err)
	require.Equal(t, "Ready", d.Status)

	return d.DeploymentID
}
