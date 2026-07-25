package inspector2

import "time"

// CreateSbomExport creates an async SBOM export.
func (b *InMemoryBackend) CreateSbomExport(
	destination, filterCriteria map[string]any, format string,
) (*SbomExport, error) {
	b.mu.Lock("CreateSbomExport")
	defer b.mu.Unlock()

	reportID := b.buildReportARN()
	export := &SbomExport{
		ReportID:       reportID,
		Status:         "SUCCEEDED",
		Destination:    destination,
		FilterCriteria: filterCriteria,
		Format:         format,
		CreatedAt:      time.Now().UTC(),
	}
	b.sbomExports.Put(export)

	return export, nil
}

// CancelSbomExport cancels an SBOM export.
func (b *InMemoryBackend) CancelSbomExport(reportID string) error {
	b.mu.Lock("CancelSbomExport")
	defer b.mu.Unlock()

	e, ok := b.sbomExports.Get(reportID)
	if !ok {
		return ErrSbomExportNotFound
	}

	e.Status = "CANCELLED"

	return nil
}

// GetSbomExport returns the status of an SBOM export.
func (b *InMemoryBackend) GetSbomExport(reportID string) (*SbomExport, error) {
	b.mu.RLock("GetSbomExport")
	defer b.mu.RUnlock()

	e, ok := b.sbomExports.Get(reportID)
	if !ok {
		return nil, ErrSbomExportNotFound
	}

	cp := *e

	return &cp, nil
}
