package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CreateArchive creates a new event archive.
func (b *InMemoryBackend) CreateArchive(ctx context.Context, input CreateArchiveInput) (*Archive, error) {
	if input.ArchiveName == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	if len(input.ArchiveName) > maxArchiveNameLength {
		return nil, fmt.Errorf(
			"%w: ArchiveName must be %d characters or fewer",
			ErrInvalidParameter,
			maxArchiveNameLength,
		)
	}

	if input.EventSourceArn == "" {
		return nil, fmt.Errorf("%w: EventSourceArn is required", ErrInvalidParameter)
	}

	if input.RetentionDays < 0 {
		return nil, fmt.Errorf(
			"%w: RetentionDays must be 0 (indefinite) or a positive integer",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateArchive")
	defer b.mu.Unlock()

	if b.archivesTable(region).Has(input.ArchiveName) {
		return nil, fmt.Errorf("%w: archive %s already exists", ErrAlreadyExists, input.ArchiveName)
	}

	archive := &Archive{
		ArchiveName:    input.ArchiveName,
		ArchiveArn:     b.archiveARN(input.ArchiveName),
		CreationTime:   time.Now(),
		Description:    input.Description,
		EventPattern:   input.EventPattern,
		EventSourceArn: input.EventSourceArn,
		RetentionDays:  input.RetentionDays,
		State:          "ENABLED",
	}
	b.archivesTable(region).Put(archive)

	cp := *archive

	return &cp, nil
}

// DeleteArchive deletes an archive.
func (b *InMemoryBackend) DeleteArchive(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteArchive")
	defer b.mu.Unlock()

	store := b.archivesTable(region)
	if !store.Has(name) {
		return fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	store.Delete(name)
	delete(b.archivedEventsStore(region), name)

	return nil
}

// DescribeArchive returns a single archive by name.
func (b *InMemoryBackend) DescribeArchive(ctx context.Context, name string) (*Archive, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeArchive")
	defer b.mu.RUnlock()

	archive, exists := b.archivesTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	cp := *archive

	return &cp, nil
}

// ListArchives returns archives optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListArchives(ctx context.Context, namePrefix, nextToken string) ([]Archive, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListArchives")
	defer b.mu.RUnlock()

	store := b.archivesTable(region)
	all := make([]Archive, 0, store.Len())
	for _, a := range store.All() {
		if namePrefix == "" || strings.HasPrefix(a.ArchiveName, namePrefix) {
			all = append(all, *a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ArchiveName < all[j].ArchiveName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateArchive updates an existing archive.
func (b *InMemoryBackend) UpdateArchive(ctx context.Context, input UpdateArchiveInput) (*Archive, error) {
	if input.ArchiveName == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateArchive")
	defer b.mu.Unlock()

	archive, exists := b.archivesTable(region).Get(input.ArchiveName)
	if !exists {
		return nil, fmt.Errorf("%w: archive %s not found", ErrNotFound, input.ArchiveName)
	}

	if input.Description != "" {
		archive.Description = input.Description
	}
	if input.EventPattern != "" {
		archive.EventPattern = input.EventPattern
	}
	if input.RetentionDays >= 0 {
		archive.RetentionDays = input.RetentionDays
	}

	cp := *archive

	return &cp, nil
}

// captureEventInArchives stores the entry in any archive whose EventSourceArn
// matches the event bus ARN and whose EventPattern matches the event.
// Must be called with b.mu held for writing.
//
// Archive patterns are matched via the shared pattern cache
// (getOrCompilePattern) rather than recompiling the pattern regexes for every
// archive on every event, so a hot PutEvents path compiles each distinct
// pattern at most once instead of once per archive per event.
func (b *InMemoryBackend) captureEventInArchives(region string, entry EventEntry, busName string) {
	busARN := b.busARN(region, busName)
	envelope := buildEventEnvelope(entry)
	archivedEvents := b.archivedEventsStore(region)
	for _, archive := range b.archivesTable(region).All() {
		if archive.EventSourceArn != busARN {
			continue
		}
		if archive.EventPattern != "" {
			compiled, err := b.getOrCompilePattern(archive.EventPattern)
			if err != nil || !matchCompiledPattern(compiled, envelope) {
				continue
			}
		}
		archivedEvents[archive.ArchiveName] = append(
			archivedEvents[archive.ArchiveName],
			entry,
		)
		archive.EventCount++
	}
}

// AddArchiveInternal adds an archive directly for testing.
func (b *InMemoryBackend) AddArchiveInternal(archive *Archive) {
	b.mu.Lock("AddArchiveInternal")
	defer b.mu.Unlock()

	cp := *archive
	b.archivesTable(b.region).Put(&cp)
}
