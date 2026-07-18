package appconfig

import (
	"fmt"
	"sort"
	"time"
)

// CreateEnvironment creates a new environment within an application.
func (b *InMemoryBackend) CreateEnvironment(
	applicationID, name, description string,
	monitors []Monitor,
) (*Environment, error) {
	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if len(b.environmentsByAppName.Get(appNameKey(applicationID, name))) > 0 {
		return nil, fmt.Errorf(
			"%w: environment with name %q already exists in application %s",
			ErrConflict,
			name,
			applicationID,
		)
	}

	now := time.Now()
	env := &Environment{
		ID:            newResourceID(),
		ApplicationID: applicationID,
		Name:          name,
		Description:   description,
		State:         "READY_FOR_DEPLOYMENT",
		Monitors:      monitors,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.environments.Put(env)
	cp := *env

	return &cp, nil
}

// GetEnvironment retrieves an environment by application and environment ID.
func (b *InMemoryBackend) GetEnvironment(
	applicationID, environmentID string,
) (*Environment, error) {
	b.mu.RLock("GetEnvironment")
	defer b.mu.RUnlock()

	env, ok := b.environments.Get(environmentID)
	if !ok || env.ApplicationID != applicationID {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	cp := *env

	return &cp, nil
}

// ListEnvironments returns paginated environments for an application.
func (b *InMemoryBackend) ListEnvironments(
	applicationID, nextToken string,
	maxResults int,
) ([]Environment, string, error) {
	b.mu.RLock("ListEnvironments")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationID) {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	envs := b.environmentsByApp.Get(applicationID)
	out := make([]Environment, 0, len(envs))

	for _, e := range envs {
		out = append(out, *e)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// UpdateEnvironment updates an environment's name, description, and
// monitors. A nil name/description/monitors means the request omitted that
// field, and AWS AppConfig leaves an omitted field unchanged rather than
// clearing it.
func (b *InMemoryBackend) UpdateEnvironment(
	applicationID, environmentID string,
	name, description *string,
	monitors *[]Monitor,
) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	existing, ok := b.environments.Get(environmentID)
	if !ok || existing.ApplicationID != applicationID {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	updated := *existing
	if name != nil && *name != "" {
		updated.Name = *name
	}

	if description != nil {
		updated.Description = *description
	}

	if monitors != nil {
		updated.Monitors = *monitors
	}

	updated.UpdatedAt = time.Now()
	b.environments.Put(&updated)
	cp := updated

	return &cp, nil
}

// DeleteEnvironment deletes an environment.
func (b *InMemoryBackend) DeleteEnvironment(applicationID, environmentID string) error {
	b.mu.Lock("DeleteEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments.Get(environmentID)
	if !ok || env.ApplicationID != applicationID {
		return fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	b.environments.Delete(environmentID)
	delete(b.tags, b.appconfigARN("application/"+applicationID+"/environment/"+environmentID))

	return nil
}
