package appconfig

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"
)

// CreateApplication creates a new AppConfig application. See
// CreateExperimentDefinition's doc comment for why tags are applied
// directly to b.tags rather than via TagResource.
func (b *InMemoryBackend) CreateApplication(name, description string, tags map[string]string) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if len(b.applicationsByName.Get(name)) > 0 {
		return nil, fmt.Errorf("%w: application with name %q already exists", ErrConflict, name)
	}

	now := time.Now()
	app := &Application{
		ID:          newResourceID(),
		Name:        name,
		Description: description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	b.applications.Put(app)

	if len(tags) > 0 {
		// Applied directly rather than via TagResource (which takes its own
		// lock and would deadlock re-entered here) -- see
		// CreateExperimentDefinition for the same pattern and the
		// bd gopherstack-lcan history it references.
		b.tags[b.appconfigARN("application/"+app.ID)] = maps.Clone(tags)
	}

	cp := *app

	return &cp, nil
}

// GetApplication retrieves an application by ID.
func (b *InMemoryBackend) GetApplication(applicationID string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications.Get(applicationID)
	if !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	cp := *app

	return &cp, nil
}

// ListApplications returns paginated applications.
func (b *InMemoryBackend) ListApplications(
	nextToken string,
	maxResults int,
) ([]Application, string) {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	all := b.applications.All()
	out := make([]Application, 0, len(all))
	for _, app := range all {
		out = append(out, *app)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// UpdateApplication updates an application's name and description. A nil
// name/description means the request omitted that field, and AWS AppConfig
// leaves an omitted field unchanged rather than clearing it (only an
// explicit, present value -- including "" -- overwrites).
func (b *InMemoryBackend) UpdateApplication(
	applicationID string, name, description *string,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	existing, ok := b.applications.Get(applicationID)
	if !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	updated := *existing
	if name != nil && *name != "" {
		updated.Name = *name
	}

	if description != nil {
		updated.Description = *description
	}

	updated.UpdatedAt = time.Now()
	b.applications.Put(&updated)
	cp := updated

	return &cp, nil
}

// DeleteApplication deletes an application by ID.
func (b *InMemoryBackend) DeleteApplication(applicationID string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationID) {
		return fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	// Clean up tags and extension associations for the application and all
	// its child resources.
	appArn := b.appconfigARN("application/" + applicationID)
	delete(b.tags, appArn)
	b.deleteExtensionAssociationsForResourceLocked(appArn)

	for _, env := range slices.Clone(b.environmentsByApp.Get(applicationID)) {
		envArn := b.appconfigARN("application/" + applicationID + "/environment/" + env.ID)
		delete(b.tags, envArn)
		b.deleteExtensionAssociationsForResourceLocked(envArn)
		b.environments.Delete(env.ID)
	}

	for _, profile := range slices.Clone(b.configProfilesByApp.Get(applicationID)) {
		profileArn := b.appconfigARN("application/" + applicationID + "/configurationprofile/" + profile.ID)
		delete(b.tags, profileArn)
		b.deleteExtensionAssociationsForResourceLocked(profileArn)
		b.configProfiles.Delete(profile.ID)
	}

	for _, v := range slices.Clone(b.hostedConfigVersionsByApp.Get(applicationID)) {
		b.hostedConfigVersions.Delete(hostedConfigVersionKeyFn(v))
	}

	for _, d := range slices.Clone(b.deploymentsByApp.Get(applicationID)) {
		b.deployments.Delete(deploymentKeyFn(d))
	}

	b.deleteDeployedConfigsLocked(func(appID, _, _ string) bool { return appID == applicationID })
	b.deleteExperimentDefinitionsForAppLocked(applicationID)

	b.applications.Delete(applicationID)
	delete(b.versionCounters, applicationID)
	delete(b.deploymentCounters, applicationID)

	return nil
}

// applicationOutput is the real API response shape for an application --
// types.Application (aws-sdk-go-v2/service/appconfig@v1.48.4 types/types.go,
// checked 2026-08-13) has Description/Id/Name only, no CreatedAt/UpdatedAt.
// Application itself keeps those two fields (with JSON tags) for
// Snapshot/Restore; this converter is what strips them for the wire.
type applicationOutput struct {
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
}

func applicationToOutput(a Application) applicationOutput {
	return applicationOutput{
		ID:          a.ID,
		Name:        a.Name,
		Description: a.Description,
	}
}
