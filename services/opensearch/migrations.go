package opensearch

import (
	"fmt"
	"time"
)

func migrationKeyFn(v *Migration) string { return v.MigrationID }

// migrationInProgressWindows is how many processingDelay windows a migration
// spends between PENDING and its terminal state: one PENDING window, then
// one IN_PROGRESS window before settling.
const migrationInProgressWindows = 2

// resolveMigrationStatus settles a migration copy's status against the
// backend's clock, reusing the same processingDelay knob every other
// transient-window resource in this backend uses (domains, serverless
// collections, capabilities): with no configured delay the migration settles
// straight to SUCCEEDED (matching the "historical fast behaviour" documented
// on SetProcessingDelay), with a delay it is observably PENDING then
// IN_PROGRESS before settling. It never mutates stored state.
func resolveMigrationStatus(m *Migration, delay time.Duration, now time.Time) {
	elapsed := now.Sub(m.CreatedAt)

	switch {
	case delay > 0 && elapsed < delay:
		m.Status = migrationStatusPending
		m.UpdatedAt = m.CreatedAt
	case delay > 0 && elapsed < migrationInProgressWindows*delay:
		m.Status = migrationStatusInProgress
		m.UpdatedAt = m.CreatedAt.Add(delay)
	default:
		// Terminal. This emulator has no saved-object store to actually
		// migrate, so it always "succeeds" at migrating zero objects rather
		// than fabricating a non-zero result -- see the Migration type doc.
		m.Status = migrationStatusSucceeded
		m.UpdatedAt = m.CreatedAt.Add(migrationInProgressWindows * delay)
	}
}

// StartMigration starts a saved-object migration job from a real,
// backend-tracked data source (see resolveDataSourceRefLocked, shared with
// the data source attachment family) into an application workspace.
func (b *InMemoryBackend) StartMigration(applicationID, sourceArn string) (*Migration, error) {
	if applicationID == "" {
		return nil, fmt.Errorf("%w: ApplicationId is required", ErrInvalidParameter)
	}

	if sourceArn == "" {
		return nil, fmt.Errorf("%w: MigrationOptions.Source.DatasourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("StartMigration")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, applicationID)
	}

	if found, _ := b.resolveDataSourceRefLocked(sourceArn); !found {
		return nil, fmt.Errorf(
			"%w: datasourceArn %s does not reference a known domain or serverless collection",
			ErrDataSourceNotFound,
			sourceArn,
		)
	}

	b.migrationCounter++
	now := b.clock()
	m := &Migration{
		MigrationID:   fmt.Sprintf("migration-%d", b.migrationCounter),
		ApplicationID: applicationID,
		SourceArn:     sourceArn,
		Status:        migrationStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.migrations.Put(m)

	cp := *m
	resolveMigrationStatus(&cp, b.processingDelay, b.clock())

	return &cp, nil
}

// GetMigration returns a migration job's current status and progress.
func (b *InMemoryBackend) GetMigration(migrationID string) (*Migration, error) {
	b.mu.RLock("GetMigration")
	defer b.mu.RUnlock()

	m, ok := b.migrations.Get(migrationID)
	if !ok {
		return nil, fmt.Errorf("%w: migration %s not found", ErrMigrationNotFound, migrationID)
	}

	cp := *m
	resolveMigrationStatus(&cp, b.processingDelay, b.clock())

	return &cp, nil
}

// ListMigrations returns every migration job for the given application,
// optionally filtered by status.
func (b *InMemoryBackend) ListMigrations(applicationID, statusFilter string) ([]*Migration, error) {
	b.mu.RLock("ListMigrations")
	defer b.mu.RUnlock()

	if applicationID == "" {
		return nil, fmt.Errorf("%w: ApplicationId is required", ErrInvalidParameter)
	}

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, applicationID)
	}

	group := b.migrationsByApp.Get(applicationID)
	now := b.clock()
	out := make([]*Migration, 0, len(group))

	for _, m := range group {
		cp := *m
		resolveMigrationStatus(&cp, b.processingDelay, now)

		if statusFilter != "" && cp.Status != statusFilter {
			continue
		}

		out = append(out, &cp)
	}

	return out, nil
}
