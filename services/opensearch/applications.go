package opensearch

import (
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateApplication creates an OpenSearch UI application.
func (b *InMemoryBackend) CreateApplication(
	name string,
	appConfigs []AppConfig,
	dataSources []AppDataSource,
	tagMap map[string]string,
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

	now := float64(b.clock().Unix())

	app := &Application{
		ID:            id,
		Name:          name,
		ARN:           appARN,
		AppConfigs:    appConfigs,
		DataSources:   dataSources,
		CreatedAt:     now,
		LastUpdatedAt: now,
		Tags:          tags.New("opensearch." + id + ".tags"),
	}
	if len(tagMap) > 0 {
		app.Tags.Merge(tagMap)
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

	app.LastUpdatedAt = float64(b.clock().Unix())

	cp := *app
	cp.AppConfigs = make([]AppConfig, len(app.AppConfigs))
	copy(cp.AppConfigs, app.AppConfigs)
	cp.DataSources = make([]AppDataSource, len(app.DataSources))
	copy(cp.DataSources, app.DataSources)

	return &cp, nil
}

// DeleteApplication removes an application by ID, cascading the removal of
// every resource scoped to it: data source attachments, capabilities,
// migration jobs, and workspaces (all four families added by
// AttachDataSource/RegisterCapability/StartMigration and friends), matching
// the cascade-cleanup precedent DeleteDomain already established for
// domain-scoped resources.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(id) {
		return fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, id)
	}

	b.applications.Delete(id)

	// Index.Get results are cloned before deleting from the underlying
	// table, matching removeDomainLocked's pattern (a concurrent Delete
	// could otherwise invalidate the index-owned slice mid-range).
	for _, att := range slices.Clone(b.dataSourceAttachmentsByApp.Get(id)) {
		b.dataSourceAttachments.Delete(dataSourceAttachmentKey(att.ApplicationID, att.DataSourceArn))
	}

	for _, cp := range b.capabilities.All() {
		if cp.ApplicationID == id {
			b.capabilities.Delete(capabilityKey(cp.ApplicationID, cp.CapabilityName))
		}
	}

	for _, m := range slices.Clone(b.migrationsByApp.Get(id)) {
		b.migrations.Delete(m.MigrationID)
	}

	for _, ws := range slices.Clone(b.workspacesByApp.Get(id)) {
		b.workspaces.Delete(ws.WorkspaceID)
	}

	return nil
}
