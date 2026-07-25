package athena

import (
	"fmt"
	"maps"
	"sort"
)

// CreateDataCatalog creates a new data catalog and returns a copy of the
// created record. The real CreateDataCatalogOutput carries an optional
// DataCatalog field with the newly created catalog; the handler wires the
// returned pointer straight into that response field.
func (b *InMemoryBackend) CreateDataCatalog(
	name, catalogType, description, connectionType string,
	params, tags map[string]string,
) (*DataCatalog, error) {
	switch {
	case name == "":
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	case catalogType == "":
		return nil, fmt.Errorf("%w: Type is required", ErrValidation)
	}

	if !isValidDataCatalogType(catalogType) {
		return nil, fmt.Errorf(
			"%w: Type %q is invalid; must be one of LAMBDA, GLUE, HIVE, FEDERATED",
			ErrValidation,
			catalogType,
		)
	}

	b.mu.Lock("CreateDataCatalog")
	defer b.mu.Unlock()

	if b.dataCatalogs.Has(name) {
		return nil, fmt.Errorf("%w: data catalog %q already exists", ErrAlreadyExists, name)
	}

	status := "CREATE_COMPLETE"
	if catalogType == "FEDERATED" {
		status = "CREATE_IN_PROGRESS"
	}

	dc := &DataCatalog{
		Name:           name,
		Type:           catalogType,
		Description:    description,
		ConnectionType: connectionType,
		Parameters:     maps.Clone(params),
		Status:         status,
	}
	b.dataCatalogs.Put(dc)

	arn := b.dataCatalogARN(name)
	if len(tags) > 0 {
		b.resourceTags[arn] = copyTags(tags)
	}

	cp := *dc
	cp.Parameters = maps.Clone(dc.Parameters)

	return &cp, nil
}

// GetDataCatalog retrieves a data catalog by name.
func (b *InMemoryBackend) GetDataCatalog(name string) (*DataCatalog, error) {
	b.mu.RLock("GetDataCatalog")
	defer b.mu.RUnlock()

	dc, ok := b.dataCatalogs.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	cp := *dc
	cp.Parameters = maps.Clone(dc.Parameters)

	return &cp, nil
}

// ListDataCatalogs returns summaries of all data catalogs with optional NextToken/MaxResults pagination.
func (b *InMemoryBackend) ListDataCatalogs(
	nextToken string,
	maxResults int,
) ([]*DataCatalogSummary, string, error) {
	b.mu.RLock("ListDataCatalogs")
	defer b.mu.RUnlock()

	all := make([]*DataCatalogSummary, 0, b.dataCatalogs.Len())
	for _, dc := range b.dataCatalogs.All() {
		all = append(all, &DataCatalogSummary{
			CatalogName:    dc.Name,
			Type:           dc.Type,
			ConnectionType: dc.ConnectionType,
			Status:         dc.Status,
			Error:          dc.Error,
		})
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].CatalogName < all[j].CatalogName
	})

	const defaultMaxResults = 50
	limit := defaultMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := paginationStart(len(all), nextToken, func(i int) string { return all[i].CatalogName })
	all = all[start:]

	outToken := ""
	if len(all) > limit {
		outToken = all[limit].CatalogName
		all = all[:limit]
	}

	return all, outToken, nil
}

// UpdateDataCatalog updates an existing data catalog.
func (b *InMemoryBackend) UpdateDataCatalog(
	name, catalogType, description, connectionType string,
	params map[string]string,
) error {
	b.mu.Lock("UpdateDataCatalog")
	defer b.mu.Unlock()

	dc, ok := b.dataCatalogs.Get(name)
	if !ok {
		return fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	if catalogType != "" && catalogType != dc.Type {
		return fmt.Errorf("%w: cannot change the Type of an existing data catalog", ErrValidation)
	}

	if description != "" {
		dc.Description = description
	}

	if connectionType != "" {
		dc.ConnectionType = connectionType
	}

	if params != nil {
		dc.Parameters = params
	}

	return nil
}

// DeleteDataCatalog removes a data catalog by name and returns a copy of the
// record as it existed immediately before deletion. The real
// DeleteDataCatalogOutput carries an optional DataCatalog field with the
// deleted catalog; the handler wires the returned pointer straight into that
// response field. The built-in AwsDataCatalog cannot be deleted.
func (b *InMemoryBackend) DeleteDataCatalog(name string) (*DataCatalog, error) {
	if name == awsDataCatalog {
		return nil, fmt.Errorf(
			"%w: cannot delete the built-in data catalog %s",
			ErrProtected,
			awsDataCatalog,
		)
	}

	b.mu.Lock("DeleteDataCatalog")
	defer b.mu.Unlock()

	dc, ok := b.dataCatalogs.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	cp := *dc
	cp.Parameters = maps.Clone(dc.Parameters)

	b.dataCatalogs.Delete(name)
	delete(b.resourceTags, b.dataCatalogARN(name))

	return &cp, nil
}
