package opensearch

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateApplication creates an OpenSearch UI application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
) (*Application, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if len(b.applicationsByName.Get(name)) > 0 {
		return nil, fmt.Errorf(
			"%w: application %s already exists",
			ErrApplicationAlreadyExists,
			name,
		)
	}

	b.appIDCounter++
	id := fmt.Sprintf("app-%d", b.appIDCounter)
	appARN := arn.Build("opensearch", b.region, b.accountID, "application/"+id)

	if appConfigs == nil {
		appConfigs = []AppConfig{}
	}

	if dataSources == nil {
		dataSources = []AppDataSource{}
	}

	app := &Application{
		ID:          id,
		Name:        name,
		ARN:         appARN,
		AppConfigs:  appConfigs,
		DataSources: dataSources,
	}
	b.applications.Put(app)

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// GetApplication returns an application by ID.
func (b *InMemoryBackend) GetApplication(id string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, exists := b.applications.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// ListApplications returns all applications.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]*Application, 0, b.applications.Len())
	for _, app := range b.applications.All() {
		cp := *app
		cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
		copy(cp.AppConfigs, app.AppConfigs)
		cp.DataSources = make([]AppDataSource, len(app.DataSources))
		copy(cp.DataSources, app.DataSources)
		out = append(out, &cp)
	}

	return out
}

// UpdateApplication updates an application's configs and data sources.
func (b *InMemoryBackend) UpdateApplication(
	id string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, exists := b.applications.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	if appConfigs != nil {
		app.AppConfigs = appConfigs
	}

	if dataSources != nil {
		app.DataSources = dataSources
	}

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// DeleteApplication removes an application by ID.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(id) {
		return fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	b.applications.Delete(id)

	return nil
}
