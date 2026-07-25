package codedeploy

// ApplicationCount returns the number of applications stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ApplicationCount() int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// DeploymentGroupCount returns the number of deployment groups for an application.
// Used only in tests.
func (b *InMemoryBackend) DeploymentGroupCount(appName string) int {
	b.mu.RLock("DeploymentGroupCount")
	defer b.mu.RUnlock()

	return len(b.deploymentGroupsByApp.Get(appName))
}

// DeploymentCount returns the number of deployments stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) DeploymentCount() int {
	b.mu.RLock("DeploymentCount")
	defer b.mu.RUnlock()

	return b.deployments.Len()
}

// OnPremisesInstanceCount returns the number of on-premises instances stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) OnPremisesInstanceCount() int {
	b.mu.RLock("OnPremisesInstanceCount")
	defer b.mu.RUnlock()

	return b.onPremisesInstances.Len()
}

// DeploymentConfigCount returns the number of deployment configs stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) DeploymentConfigCount() int {
	b.mu.RLock("DeploymentConfigCount")
	defer b.mu.RUnlock()

	return b.deploymentConfigs.Len()
}

// ApplicationRevisionCount returns the number of registered application
// revisions stored in the backend. Used only in tests.
func (b *InMemoryBackend) ApplicationRevisionCount() int {
	b.mu.RLock("ApplicationRevisionCount")
	defer b.mu.RUnlock()

	return b.applicationRevisions.Len()
}
