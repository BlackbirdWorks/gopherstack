package bedrockagent

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Data source CRUD
// ---------------------------------------------------------------------------

func dsKey(kbID, dsID string) string { return kbID + "/" + dsID }

// CreateDataSource creates a data source in a knowledge base.
func (b *InMemoryBackend) CreateDataSource(
	_ context.Context, kbID string, cfg DataSourceConfig,
) (*DataSource, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	id := b.nextID("ds", &b.dsCounter)
	now := time.Now().UTC()

	ds := &DataSource{
		DataSourceID:            id,
		KnowledgeBaseID:         kbID,
		Name:                    cfg.Name,
		DataSourceStatus:        dsStatusAvailable,
		Description:             cfg.Description,
		DataDeletionPolicy:      cfg.DataDeletionPolicy,
		DataSourceConfiguration: cfg.DataSourceConfiguration,
		VectorIngestionConfig:   cfg.VectorIngestionConfig,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	b.dataSources.Put(ds)

	return dsCopy(ds), nil
}

// GetDataSource returns a data source.
func (b *InMemoryBackend) GetDataSource(_ context.Context, kbID, dsID string) (*DataSource, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ds, ok := b.dataSources.Get(dsKey(kbID, dsID))
	if !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	return dsCopy(ds), nil
}

// UpdateDataSource updates a data source.
func (b *InMemoryBackend) UpdateDataSource(
	_ context.Context, kbID, dsID string, cfg DataSourceConfig,
) (*DataSource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ds, ok := b.dataSources.Get(dsKey(kbID, dsID))
	if !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	if cfg.Name != "" {
		ds.Name = cfg.Name
	}

	if cfg.Description != "" {
		ds.Description = cfg.Description
	}

	if cfg.DataDeletionPolicy != "" {
		ds.DataDeletionPolicy = cfg.DataDeletionPolicy
	}

	if cfg.DataSourceConfiguration != nil {
		ds.DataSourceConfiguration = cfg.DataSourceConfiguration
	}

	if cfg.VectorIngestionConfig != nil {
		ds.VectorIngestionConfig = cfg.VectorIngestionConfig
	}

	ds.UpdatedAt = time.Now().UTC()

	return dsCopy(ds), nil
}

// DeleteDataSource deletes a data source.
func (b *InMemoryBackend) DeleteDataSource(_ context.Context, kbID, dsID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := dsKey(kbID, dsID)
	if !b.dataSources.Has(key) {
		return fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	b.dataSources.Delete(key)

	return nil
}

// ListDataSources returns paginated data source summaries.
func (b *InMemoryBackend) ListDataSources(
	_ context.Context, kbID string, maxResults int, nextToken string,
) ([]*DataSourceSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.dataSourcesByKB.Get(kbID)
	ids := tableIDs(group, func(ds *DataSource) string { return ds.DataSourceID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*DataSourceSummary, 0, len(ids))

	for _, id := range ids {
		ds, _ := b.dataSources.Get(dsKey(kbID, id))
		out = append(out, &DataSourceSummary{
			DataSourceID:     ds.DataSourceID,
			KnowledgeBaseID:  ds.KnowledgeBaseID,
			Name:             ds.Name,
			DataSourceStatus: ds.DataSourceStatus,
			Description:      ds.Description,
			UpdatedAt:        ds.UpdatedAt,
		})
	}

	return out, outToken, nil
}

func dsCopy(ds *DataSource) *DataSource {
	cp := *ds

	return &cp
}
