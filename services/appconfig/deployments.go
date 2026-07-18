package appconfig

import (
	"fmt"
	"sort"
	"time"
)

// StartDeployment starts a deployment.
func (b *InMemoryBackend) StartDeployment(
	applicationID, environmentID, configProfileID, strategyID, configVersion, description string,
) (*Deployment, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	env, ok := b.environments.Get(environmentID)
	if !ok || env.ApplicationID != applicationID {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	profile, ok := b.configProfiles.Get(configProfileID)
	if !ok || profile.ApplicationID != applicationID {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			configProfileID,
		)
	}

	if !b.deploymentStrategies.Has(strategyID) {
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
	}

	if b.deploymentCounters[applicationID] == nil {
		b.deploymentCounters[applicationID] = make(map[string]int32)
	}

	b.deploymentCounters[applicationID][environmentID]++
	deploymentNumber := b.deploymentCounters[applicationID][environmentID]

	now := time.Now()
	deployment := &Deployment{
		ApplicationID:          applicationID,
		EnvironmentID:          environmentID,
		ConfigurationProfileID: configProfileID,
		DeploymentStrategyID:   strategyID,
		ConfigurationVersion:   configVersion,
		Description:            description,
		State:                  "COMPLETE",
		TriggeredBy:            "USER",
		PercentageComplete:     100.0, //nolint:mnd // 100% complete
		DeploymentNumber:       deploymentNumber,
		StartedAt:              now,
		CompletedAt:            now,
	}
	b.deployments.Put(deployment)
	cp := *deployment

	return &cp, nil
}

// GetDeployment retrieves a deployment.
func (b *InMemoryBackend) GetDeployment(
	applicationID, environmentID string,
	deploymentNumber int32,
) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.deployments.Get(deploymentKey(applicationID, environmentID, deploymentNumber))
	if !ok {
		return nil, fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	cp := *d

	return &cp, nil
}

// ListDeployments returns paginated deployments for an environment.
func (b *InMemoryBackend) ListDeployments(
	applicationID, environmentID, nextToken string,
	maxResults int,
) ([]Deployment, string, error) {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	// Single lookup — returns a clear error for app-not-found or env-not-found.
	env, ok := b.environments.Get(environmentID)
	if !ok || env.ApplicationID != applicationID {
		if !b.applications.Has(applicationID) {
			return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
		}

		return nil, "", fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	deploys := b.deploymentsByEnv.Get(appEnvKey(applicationID, environmentID))

	out := make([]Deployment, 0, len(deploys))
	for _, d := range deploys {
		out = append(out, *d)
	}

	sort.Slice(
		out,
		func(i, j int) bool { return out[i].DeploymentNumber < out[j].DeploymentNumber },
	)

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// stoppableDeploymentStates are the states from which a deployment can be stopped.
var stoppableDeploymentStates = map[string]bool{ //nolint:gochecknoglobals // compile-time constant map
	"BAKING":     true,
	"DEPLOYING":  true,
	"VALIDATING": true,
}

// StopDeployment stops an in-progress deployment.
func (b *InMemoryBackend) StopDeployment(
	applicationID, environmentID string,
	deploymentNumber int32,
) error {
	b.mu.Lock("StopDeployment")
	defer b.mu.Unlock()

	key := deploymentKey(applicationID, environmentID, deploymentNumber)

	d, ok := b.deployments.Get(key)
	if !ok {
		return fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	// Allow stopping from any non-terminal state to keep in-memory stub pragmatic.
	// (Real deployments complete instantly here so we still accept the request.)
	if d.State != "COMPLETE" && d.State != "ROLLED_BACK" && !stoppableDeploymentStates[d.State] {
		return fmt.Errorf("%w: cannot stop deployment in state %s", ErrBadRequest, d.State)
	}

	updated := *d
	updated.State = "ROLLED_BACK"
	updated.CompletedAt = time.Now()
	b.deployments.Put(&updated)

	return nil
}
