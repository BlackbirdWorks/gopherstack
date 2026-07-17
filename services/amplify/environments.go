package amplify

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateBackendEnvironment creates a new backend environment for an app.
func (b *InMemoryBackend) CreateBackendEnvironment(
	appID, environmentName, stackName, deploymentArtifacts string,
) (*BackendEnvironment, error) {
	b.mu.Lock("CreateBackendEnvironment")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	key := backendEnvKey(appID, environmentName)
	if b.backendEnvironments.Has(key) {
		return nil, fmt.Errorf(
			"%w: backend environment %s already exists for app %s",
			ErrAlreadyExists,
			environmentName,
			appID,
		)
	}

	envARN := arn.Build(
		"amplify", b.region, b.accountID,
		fmt.Sprintf("apps/%s/backendenvironments/%s", appID, environmentName),
	)
	now := time.Now().UTC()

	env := &BackendEnvironment{
		EnvironmentName:       environmentName,
		BackendEnvironmentARN: envARN,
		AppID:                 appID,
		StackName:             stackName,
		DeploymentArtifacts:   deploymentArtifacts,
		CreateTime:            now,
		UpdateTime:            now,
	}

	b.backendEnvironments.Put(env)

	cp := *env

	return &cp, nil
}

// GetBackendEnvironment returns a backend environment.
func (b *InMemoryBackend) GetBackendEnvironment(
	appID, environmentName string,
) (*BackendEnvironment, error) {
	b.mu.RLock("GetBackendEnvironment")
	defer b.mu.RUnlock()

	env, err := b.findBackendEnv(appID, environmentName)
	if err != nil {
		return nil, err
	}

	cp := *env

	return &cp, nil
}

// DeleteBackendEnvironment deletes a backend environment.
func (b *InMemoryBackend) DeleteBackendEnvironment(
	appID, environmentName string,
) (*BackendEnvironment, error) {
	b.mu.Lock("DeleteBackendEnvironment")
	defer b.mu.Unlock()

	env, err := b.findBackendEnv(appID, environmentName)
	if err != nil {
		return nil, err
	}

	cp := *env
	b.backendEnvironments.Delete(backendEnvKey(appID, environmentName))

	return &cp, nil
}

// ListBackendEnvironments lists backend environments for an app.
func (b *InMemoryBackend) ListBackendEnvironments(
	appID, nextToken string,
	maxResults int,
) ([]*BackendEnvironment, string, error) {
	b.mu.RLock("ListBackendEnvironments")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*BackendEnvironment

	for _, env := range b.backendEnvironmentsByApp.Get(appID) {
		cp := *env
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].EnvironmentName < all[j].EnvironmentName })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// findBackendEnv locates a backend environment. Must be called while holding a lock.
func (b *InMemoryBackend) findBackendEnv(
	appID, environmentName string,
) (*BackendEnvironment, error) {
	env, ok := b.backendEnvironments.Get(backendEnvKey(appID, environmentName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: backend environment %s not found for app %s",
			ErrNotFound,
			environmentName,
			appID,
		)
	}

	return env, nil
}
