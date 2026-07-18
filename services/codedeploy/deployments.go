package codedeploy

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"time"
)

const (
	deployIDChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	deployIDLen   = 9
)

// simulatedDeployDuration is the simulated time for a deployment to complete.
const simulatedDeployDuration = 5 * time.Second

// generateDeploymentID produces an AWS-format deployment ID: d- followed by 9 uppercase alphanumeric chars.
func generateDeploymentID() string {
	b := make([]byte, deployIDLen)
	for i := range b {
		b[i] = deployIDChars[rand.IntN(len(deployIDChars))] //nolint:gosec // non-crypto ID for test mock
	}

	return "d-" + string(b)
}

// CreateDeployment creates a new deployment.
func (b *InMemoryBackend) CreateDeployment(appName, dgName string, opts DeploymentOptions) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dg, ok := b.deploymentGroups.Get(dgKey(appName, dgName))
	if !ok {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	if opts.Creator == "" {
		opts.Creator = "user"
	}

	deployID := generateDeploymentID()
	now := time.Now().UTC()
	completed := now.Add(simulatedDeployDuration)

	d := &Deployment{
		DeploymentID:                  deployID,
		ApplicationName:               appName,
		DeploymentGroupName:           dgName,
		DeploymentConfigName:          dg.DeploymentConfigName,
		Status:                        statusSucceeded,
		Creator:                       opts.Creator,
		Description:                   opts.Description,
		FileExistsBehavior:            opts.FileExistsBehavior,
		UpdateOutdatedInstancesOnly:   opts.UpdateOutdatedInstancesOnly,
		IgnoreApplicationStopFailures: opts.IgnoreApplicationStopFailures,
		Revision:                      opts.Revision,
		CreateTime:                    now,
		CompleteTime:                  &completed,
		AccountID:                     b.accountID,
		Region:                        b.region,
	}
	b.deployments.Put(d)

	cp := *d

	return &cp, nil
}

// GetDeployment returns a deployment by ID.
func (b *InMemoryBackend) GetDeployment(deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.deployments.Get(deploymentID)
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	cp := *d

	return &cp, nil
}

// ListDeployments returns deployment IDs in sorted order, filtered by the provided criteria.
func (b *InMemoryBackend) ListDeployments(filter DeploymentFilter) []string {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	statusSet := make(map[string]struct{}, len(filter.Statuses))
	for _, s := range filter.Statuses {
		statusSet[s] = struct{}{}
	}

	all := b.deployments.All()
	ids := make([]string, 0, len(all))

	for _, d := range all {
		if filter.ApplicationName != "" && d.ApplicationName != filter.ApplicationName {
			continue
		}

		if filter.DeploymentGroupName != "" && d.DeploymentGroupName != filter.DeploymentGroupName {
			continue
		}

		if len(statusSet) > 0 {
			if _, ok := statusSet[d.Status]; !ok {
				continue
			}
		}

		if filter.CreateTimeStart != nil && d.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}

		if filter.CreateTimeEnd != nil && d.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}

		ids = append(ids, d.DeploymentID)
	}

	sort.Strings(ids)

	return ids
}

// StopDeployment marks a deployment as Stopped.
func (b *InMemoryBackend) StopDeployment(deploymentID string) error {
	b.mu.Lock("StopDeployment")
	defer b.mu.Unlock()

	d, ok := b.deployments.Get(deploymentID)
	if !ok {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	d.Status = statusStopped

	return nil
}

// ContinueDeployment marks a blue/green deployment as continuing past the wait point.
func (b *InMemoryBackend) ContinueDeployment(deploymentID string) error {
	b.mu.Lock("ContinueDeployment")
	defer b.mu.Unlock()

	if !b.deployments.Has(deploymentID) {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	return nil
}

// BatchGetDeployments returns deployment structs for the given IDs.
// Deployment IDs that do not exist are silently omitted.
func (b *InMemoryBackend) BatchGetDeployments(deploymentIDs []string) []*Deployment {
	b.mu.RLock("BatchGetDeployments")
	defer b.mu.RUnlock()

	result := make([]*Deployment, 0, len(deploymentIDs))

	for _, id := range deploymentIDs {
		d, ok := b.deployments.Get(id)
		if !ok {
			continue
		}

		cp := *d
		result = append(result, &cp)
	}

	return result
}

// AddDeploymentInternal adds a deployment directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentInternal(d *Deployment) {
	b.mu.Lock("AddDeploymentInternal")
	defer b.mu.Unlock()

	if d.DeploymentID == "" {
		d.DeploymentID = generateDeploymentID()
	}

	if d.CreateTime.IsZero() {
		d.CreateTime = time.Now().UTC()
	}

	b.deployments.Put(d)
}
