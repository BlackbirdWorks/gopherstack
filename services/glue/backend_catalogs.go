package glue

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

func cloneCatalogEntry(c *CatalogEntry) *CatalogEntry {
	cp := *c
	if c.Parameters != nil {
		cp.Parameters = make(map[string]string, len(c.Parameters))
		maps.Copy(cp.Parameters, c.Parameters)
	}

	return &cp
}

func (b *InMemoryBackend) CreateCatalog(
	catalogID, name, description string,
	params map[string]string,
) error {
	b.mu.Lock("CreateCatalog")
	defer b.mu.Unlock()

	if b.catalogs.Has(catalogID) {
		return fmt.Errorf("catalog %q already exists: %w", catalogID, ErrAlreadyExists)
	}
	b.catalogs.Put(&CatalogEntry{
		CatalogID:   catalogID,
		Name:        name,
		Description: description,
		Parameters:  params,
		CreateTime:  float64(time.Now().Unix()),
	})

	return nil
}

func (b *InMemoryBackend) GetCatalog(catalogID string) (*CatalogEntry, error) {
	b.mu.RLock("GetCatalog")
	defer b.mu.RUnlock()

	c, ok := b.catalogs.Get(catalogID)
	if !ok {
		return nil, fmt.Errorf("catalog %q not found: %w", catalogID, ErrNotFound)
	}

	return cloneCatalogEntry(c), nil
}

func (b *InMemoryBackend) GetCatalogs() []*CatalogEntry {
	b.mu.RLock("GetCatalogs")
	defer b.mu.RUnlock()

	src := b.catalogs.All()
	out := make([]*CatalogEntry, 0, len(src))
	for _, c := range src {
		out = append(out, cloneCatalogEntry(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CatalogID < out[j].CatalogID })

	return out
}

func (b *InMemoryBackend) UpdateCatalog(
	catalogID, description string,
	params map[string]string,
) error {
	b.mu.Lock("UpdateCatalog")
	defer b.mu.Unlock()

	c, ok := b.catalogs.Get(catalogID)
	if !ok {
		return fmt.Errorf("catalog %q not found: %w", catalogID, ErrNotFound)
	}
	c.Description = description
	if params != nil {
		c.Parameters = params
	}

	return nil
}

func (b *InMemoryBackend) DeleteCatalog(catalogID string) error {
	b.mu.Lock("DeleteCatalog")
	defer b.mu.Unlock()

	if !b.catalogs.Has(catalogID) {
		return fmt.Errorf("catalog %q not found: %w", catalogID, ErrNotFound)
	}
	b.catalogs.Delete(catalogID)

	return nil
}

func (b *InMemoryBackend) PutDataCatalogEncryptionSettings(
	catalogID string,
	settings DataCatalogEncryptionSettings,
) error {
	b.mu.Lock("PutDataCatalogEncryptionSettings")
	defer b.mu.Unlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}
	b.catalogEncryptionSettings[key] = &settings

	return nil
}

func (b *InMemoryBackend) GetDataCatalogEncryptionSettings(
	catalogID string,
) (*DataCatalogEncryptionSettings, error) {
	b.mu.RLock("GetDataCatalogEncryptionSettings")
	defer b.mu.RUnlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}
	if s, ok := b.catalogEncryptionSettings[key]; ok {
		cp := *s

		return &cp, nil
	}
	// Return empty default settings (AWS returns defaults even if never set)
	return &DataCatalogEncryptionSettings{}, nil
}

// ImportCatalogToGlue marks the given catalog (or the account-level catalog
// when catalogID is empty) as imported from a Hive metastore.
func (b *InMemoryBackend) ImportCatalogToGlue(catalogID string) error {
	b.mu.Lock("ImportCatalogToGlue")
	defer b.mu.Unlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}

	b.catalogImports[key] = &CatalogImportStatus{
		ImportCompleted: true,
		ImportTime:      float64(time.Now().Unix()),
		ImportedBy:      glueServiceName,
	}

	return nil
}

// GetCatalogImportStatus returns the import status for the given catalog.
// When catalogID is empty, the account-level status is returned.
// Returns nil (no error) when no import has been triggered yet.
func (b *InMemoryBackend) GetCatalogImportStatus(catalogID string) *CatalogImportStatus {
	b.mu.RLock("GetCatalogImportStatus")
	defer b.mu.RUnlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}

	if s, ok := b.catalogImports[key]; ok {
		cp := *s

		return &cp
	}

	return &CatalogImportStatus{ImportCompleted: false}
}
