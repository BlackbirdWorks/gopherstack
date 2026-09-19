package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	discovererStateStarted = "STARTED"
	discovererStateStopped = "STOPPED"

	// discoveredSchemaSkeletonContent mirrors GetDiscoveredSchema's own stub:
	// AWS generates a full OpenApi3 schema from the event payload, which is
	// out of scope for in-process emulation; a minimal valid skeleton lets
	// DescribeSchema/ListSchemas on a discovered schema round-trip honestly.
	discoveredSchemaSkeletonContent = `{"openapi":"3.0.0","info":{"title":"DiscoveredSchema","version":"1.0"},"paths":{}}`
)

// discovererIDFromSourceArn derives a DiscovererId from the event bus ARN's
// final "/"-delimited segment. AWS allows at most one discoverer per
// SourceArn, so keying the ID off the bus ARN keeps that invariant a natural
// consequence of DiscovererId uniqueness rather than a separate index.
func discovererIDFromSourceArn(sourceArn string) string {
	if idx := strings.LastIndex(sourceArn, "/"); idx >= 0 {
		return sourceArn[idx+1:]
	}

	return sourceArn
}

// CreateDiscoverer creates a discoverer watching an event bus.
func (b *InMemoryBackend) CreateDiscoverer(
	ctx context.Context, //nolint:revive // existing issue.
	input CreateDiscovererInput,
) (*Discoverer, error) {
	if input.SourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrInvalidParameter)
	}

	crossAccount := true
	if input.CrossAccount != nil {
		crossAccount = *input.CrossAccount
	}

	id := discovererIDFromSourceArn(input.SourceArn)

	b.mu.Lock("CreateDiscoverer")
	defer b.mu.Unlock()

	if b.discoverersTable().Has(id) {
		return nil, fmt.Errorf(
			"%w: a discoverer already exists for source %s",
			ErrAlreadyExists,
			input.SourceArn,
		)
	}

	d := &Discoverer{
		DiscovererArn: b.discovererARN(id),
		DiscovererID:  id,
		SourceArn:     input.SourceArn,
		CrossAccount:  crossAccount,
		Description:   input.Description,
		State:         discovererStateStarted,
		Tags:          input.Tags,
	}
	b.discoverersTable().Put(d)

	cp := *d

	return &cp, nil
}

// DescribeDiscoverer returns a single discoverer.
func (b *InMemoryBackend) DescribeDiscoverer(
	ctx context.Context, //nolint:revive // existing issue.
	discovererID string,
) (*Discoverer, error) {
	if discovererID == "" {
		return nil, fmt.Errorf("%w: DiscovererId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeDiscoverer")
	defer b.mu.RUnlock()

	d, ok := b.discoverersTable().Get(discovererID)
	if !ok {
		return nil, fmt.Errorf("%w: discoverer %s not found", ErrNotFound, discovererID)
	}

	cp := *d

	return &cp, nil
}

// ListDiscoverers returns discoverers, optionally filtered by DiscovererId
// prefix and/or SourceArn prefix.
func (b *InMemoryBackend) ListDiscoverers(
	ctx context.Context, //nolint:revive // existing issue.
	discovererIDPrefix, sourceArnPrefix, nextToken string,
	limit int,
) ([]Discoverer, string, error) {
	b.mu.RLock("ListDiscoverers")
	defer b.mu.RUnlock()

	all := make([]Discoverer, 0, b.discoverersTable().Len())
	for _, d := range b.discoverersTable().All() {
		if discovererIDPrefix != "" && !strings.HasPrefix(d.DiscovererID, discovererIDPrefix) {
			continue
		}

		if sourceArnPrefix != "" && !strings.HasPrefix(d.SourceArn, sourceArnPrefix) {
			continue
		}

		all = append(all, *d)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DiscovererID < all[j].DiscovererID })

	page, outToken := paginateN(all, nextToken, limit)

	return page, outToken, nil
}

// UpdateDiscoverer updates a discoverer's description and/or CrossAccount setting.
func (b *InMemoryBackend) UpdateDiscoverer(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdateDiscovererInput,
) (*Discoverer, error) {
	if input.DiscovererID == "" {
		return nil, fmt.Errorf("%w: DiscovererId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateDiscoverer")
	defer b.mu.Unlock()

	d, ok := b.discoverersTable().Get(input.DiscovererID)
	if !ok {
		return nil, fmt.Errorf("%w: discoverer %s not found", ErrNotFound, input.DiscovererID)
	}

	if input.Description != nil {
		d.Description = *input.Description
	}

	if input.CrossAccount != nil {
		d.CrossAccount = *input.CrossAccount
	}

	cp := *d

	return &cp, nil
}

// DeleteDiscoverer deletes a discoverer.
func (b *InMemoryBackend) DeleteDiscoverer(
	ctx context.Context, //nolint:revive // existing issue.
	discovererID string,
) error {
	if discovererID == "" {
		return fmt.Errorf("%w: DiscovererId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteDiscoverer")
	defer b.mu.Unlock()

	if !b.discoverersTable().Has(discovererID) {
		return fmt.Errorf("%w: discoverer %s not found", ErrNotFound, discovererID)
	}

	b.discoverersTable().Delete(discovererID)

	return nil
}

// StartDiscoverer transitions a discoverer to STARTED. Idempotent: AWS's own
// deserializeOpError switch for this op declares no
// ConflictException/InvalidStateException, so starting an already-started
// discoverer succeeds rather than erroring.
func (b *InMemoryBackend) StartDiscoverer(
	ctx context.Context, //nolint:revive // existing issue.
	discovererID string,
) (*Discoverer, error) {
	return b.setDiscovererState(discovererID, discovererStateStarted)
}

// StopDiscoverer transitions a discoverer to STOPPED. See StartDiscoverer's
// doc comment for why this is idempotent.
func (b *InMemoryBackend) StopDiscoverer(
	ctx context.Context, //nolint:revive // existing issue.
	discovererID string,
) (*Discoverer, error) {
	return b.setDiscovererState(discovererID, discovererStateStopped)
}

func (b *InMemoryBackend) setDiscovererState(discovererID, state string) (*Discoverer, error) {
	if discovererID == "" {
		return nil, fmt.Errorf("%w: DiscovererId is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetDiscovererState")
	defer b.mu.Unlock()

	d, ok := b.discoverersTable().Get(discovererID)
	if !ok {
		return nil, fmt.Errorf("%w: discoverer %s not found", ErrNotFound, discovererID)
	}

	d.State = state

	cp := *d

	return &cp, nil
}

// sanitizeSchemaNamePart replaces any character outside Schemas' allowed
// SchemaName charset ([a-zA-Z0-9._-]) with "-", so an event's arbitrary
// Source/DetailType text always yields a valid discovered SchemaName.
func sanitizeSchemaNamePart(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		default:
			return '-'
		}
	}, s)
}

// discoveredSchemaName builds the SchemaName a STARTED discoverer registers
// for an event on busName, from its Source and DetailType. Not itself part
// of any verified AWS wire shape (the discoverer's internal naming scheme is
// opaque to callers, who only ever read it back via ListSchemas/
// DescribeSchema, both of which round-trip whatever name was stored).
func discoveredSchemaName(busName, source, detailType string) string {
	return sanitizeSchemaNamePart(busName) + "@" +
		sanitizeSchemaNamePart(source) + "." + sanitizeSchemaNamePart(detailType)
}

// discoverSchemaLocked registers a schema for entry's source/detail-type in
// the discovered-schemas registry, if busName has a STARTED discoverer whose
// SourceArn matches. Caller must already hold b.mu (called from
// putEventsLocked). A no-op if the schema was already discovered.
func (b *InMemoryBackend) discoverSchemaLocked(region, busName string, entry EventEntry) {
	busArn := b.busARN(region, busName)

	var found *Discoverer
	for _, d := range b.discoverersTable().All() {
		if d.SourceArn == busArn {
			found = d

			break
		}
	}

	if found == nil || found.State != discovererStateStarted {
		return
	}

	if !b.registriesTable().Has(builtinRegistryDiscoveredSchemas) {
		b.registriesTable().Put(&SchemaRegistry{
			RegistryArn:  b.registryARN(builtinRegistryDiscoveredSchemas),
			RegistryName: builtinRegistryDiscoveredSchemas,
		})
	}

	schemaName := discoveredSchemaName(busName, entry.Source, entry.DetailType)
	schemaTable := b.schemasTableFor(builtinRegistryDiscoveredSchemas)

	if schemaTable.Has(schemaName) {
		return
	}

	now := time.Now()
	schema := &Schema{
		SchemaArn:          b.schemaARN(builtinRegistryDiscoveredSchemas, schemaName),
		SchemaName:         schemaName,
		SchemaVersion:      defaultSchemaVersion,
		RegistryName:       builtinRegistryDiscoveredSchemas,
		Type:               schemaTypeOpenAPI3,
		Content:            discoveredSchemaSkeletonContent,
		LastModified:       now,
		VersionCreatedDate: now,
	}
	schemaTable.Put(schema)

	versionKey := b.schemaVersionKey(builtinRegistryDiscoveredSchemas, schemaName)
	b.schemaVersions[versionKey] = []*SchemaVersion{{
		SchemaArn:     schema.SchemaArn,
		SchemaName:    schemaName,
		SchemaVersion: defaultSchemaVersion,
		RegistryName:  builtinRegistryDiscoveredSchemas,
		Type:          schemaTypeOpenAPI3,
		Content:       discoveredSchemaSkeletonContent,
		CreatedDate:   now,
	}}
}
