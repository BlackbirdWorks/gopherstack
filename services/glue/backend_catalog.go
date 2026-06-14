package glue

import (
	"time"
)

// glueServiceName is the canonical service name used in import attribution.
const glueServiceName = "Glue"

// CatalogImportStatus stores the import status for a data catalog.
type CatalogImportStatus struct {
	ImportedBy      string  `json:"ImportedBy,omitempty"`
	ImportTime      float64 `json:"ImportTime,omitempty"`
	ImportCompleted bool    `json:"ImportCompleted"`
}

// GetCatalogImportStatus returns the import status for the given catalog.
// If no import has occurred, it returns a default status with ImportCompleted=false.
func (b *InMemoryBackend) GetCatalogImportStatus(catalogID string) *CatalogImportStatus {
	b.mu.RLock("GetCatalogImportStatus")
	defer b.mu.RUnlock()

	if s, ok := b.catalogImportStatus[catalogID]; ok {
		cp := *s

		return &cp
	}

	return &CatalogImportStatus{ImportCompleted: false}
}

// ImportCatalogToGlue marks the catalog as imported.
func (b *InMemoryBackend) ImportCatalogToGlue(catalogID string) {
	b.mu.Lock("ImportCatalogToGlue")
	defer b.mu.Unlock()

	b.catalogImportStatus[catalogID] = &CatalogImportStatus{
		ImportCompleted: true,
		ImportTime:      float64(time.Now().Unix()),
		ImportedBy:      glueServiceName,
	}
}
