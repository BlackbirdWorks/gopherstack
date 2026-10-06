package cloudtrail

import (
	"fmt"
	"sort"
	"time"
)

const (
	importStatusInitializing = "INITIALIZING"
	importStatusStopped      = "STOPPED"
	importStatusFailed       = "FAILED"
)

// StartImport creates an import job; importSource may be nil for callers that only pass Destinations.
func (b *InMemoryBackend) StartImport(
	destinations []string, importSource *ImportSource, startEventTime, endEventTime *time.Time,
) (*Import, error) {
	b.mu.Lock("StartImport")
	defer b.mu.Unlock()

	b.importCounter++
	id := fmt.Sprintf("import-%06d", b.importCounter)
	now := time.Now().UTC()
	imp := &Import{
		ImportID:         id,
		Destinations:     destinations,
		ImportSource:     importSource,
		StartEventTime:   startEventTime,
		EndEventTime:     endEventTime,
		ImportStatus:     importStatusInitializing,
		CreatedTimestamp: now,
		UpdatedTimestamp: now,
	}
	b.imports.Put(imp)
	cp := *imp

	return &cp, nil
}

// RetryImport restarts a stopped or failed import by ID.
func (b *InMemoryBackend) RetryImport(importID string) (*Import, error) {
	b.mu.Lock("RetryImport")
	defer b.mu.Unlock()

	imp, ok := b.imports.Get(importID)
	if !ok {
		return nil, fmt.Errorf("%w: import %s not found", ErrImportNotFound, importID)
	}

	if imp.ImportStatus != importStatusStopped && imp.ImportStatus != importStatusFailed {
		return nil, fmt.Errorf("%w: import %s is %s and cannot be retried", ErrValidation, importID, imp.ImportStatus)
	}

	imp.ImportStatus = importStatusInitializing
	imp.UpdatedTimestamp = time.Now().UTC()
	cp := *imp

	return &cp, nil
}

// GetImport returns an import by ID.
func (b *InMemoryBackend) GetImport(importID string) (*Import, error) {
	b.mu.RLock("GetImport")
	defer b.mu.RUnlock()

	imp, ok := b.imports.Get(importID)
	if !ok {
		return nil, fmt.Errorf("%w: import %s not found", ErrImportNotFound, importID)
	}
	cp := *imp

	return &cp, nil
}

// ListImports returns all imports.
func (b *InMemoryBackend) ListImports() []*Import {
	b.mu.RLock("ListImports")
	defer b.mu.RUnlock()

	all := b.imports.All()
	list := make([]*Import, 0, len(all))
	for _, imp := range all {
		cp := *imp
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ImportID < list[j].ImportID })

	return list
}

// StopImport stops an in-progress import.
func (b *InMemoryBackend) StopImport(importID string) (*Import, error) {
	b.mu.Lock("StopImport")
	defer b.mu.Unlock()

	imp, ok := b.imports.Get(importID)
	if !ok {
		return nil, fmt.Errorf("%w: import %s not found", ErrImportNotFound, importID)
	}
	imp.ImportStatus = importStatusStopped
	imp.UpdatedTimestamp = time.Now().UTC()
	cp := *imp

	return &cp, nil
}

// ListImportFailures returns empty import failures (stub).
func (b *InMemoryBackend) ListImportFailures(_ string) []map[string]any {
	return []map[string]any{}
}
