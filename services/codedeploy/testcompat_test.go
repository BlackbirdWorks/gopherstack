package codedeploy_test

// This file provides thin adapter helpers so that existing tests continue to
// compile after the CreateDeploymentGroup/CreateDeployment/CreateDeploymentConfig
// signatures were extended with richer parameter structs.

import "github.com/blackbirdworks/gopherstack/services/codedeploy"

// createDG is a convenience wrapper for tests that only need the legacy fields.
func createDG(
	b *codedeploy.InMemoryBackend,
	appName, dgName, serviceRoleArn, deploymentConfigName string,
	kv map[string]string,
) (*codedeploy.DeploymentGroup, error) {
	return b.CreateDeploymentGroup(appName, dgName, codedeploy.DeploymentGroupInput{
		ServiceRoleArn:       serviceRoleArn,
		DeploymentConfigName: deploymentConfigName,
	}, kv)
}

// createDeploy is a convenience wrapper for tests that use the legacy 4-arg form.
func createDeploy(
	b *codedeploy.InMemoryBackend,
	appName, dgName, description, creator string,
) (*codedeploy.Deployment, error) {
	return b.CreateDeployment(appName, dgName, codedeploy.DeploymentOptions{
		Description: description,
		Creator:     creator,
	})
}

// createCfg is a convenience wrapper for tests that use the legacy 2-arg form.
func createCfg(
	b *codedeploy.InMemoryBackend,
	name, computePlatform string,
) (*codedeploy.DeploymentConfig, error) {
	return b.CreateDeploymentConfig(name, computePlatform, nil, nil, nil)
}
