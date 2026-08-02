package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family F (8 ops): CreateWave, UpdateWave, DeleteWave,
// ListWaves, ArchiveWave, UnarchiveWave, AssociateApplications,
// DisassociateApplications. See applications.go's doc comment for the
// confirmed Wave -> Application -> SourceServer grouping hierarchy.

func (b *InMemoryBackend) resolveWaveLocked(id string) (*Wave, bool) {
	return b.waves.Get(id)
}

// CreateWave creates a new Wave.
func (b *InMemoryBackend) CreateWave(name, description string, waveTags map[string]string) (*Wave, error) {
	b.mu.Lock("CreateWave")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if name == "" {
		return nil, validationError("name is required")
	}

	id := newWaveID()
	now := nowRFC3339()
	t := tags.New("mgn.wave." + id + ".tags")
	t.Merge(waveTags)

	wave := &Wave{
		WaveID:               id,
		Arn:                  b.waveARN(id),
		Name:                 name,
		Description:          description,
		Tags:                 t,
		CreationDateTime:     now,
		LastModifiedDateTime: now,
		AggregatedStatus: &WaveAggregatedStatus{
			HealthStatus:       HealthStatusHealthy,
			ProgressStatus:     ProgressStatusNotStarted,
			LastUpdateDateTime: now,
		},
	}
	b.waves.Put(wave)

	return wave.clone(), nil
}

// UpdateWave applies a partial update to a Wave.
func (b *InMemoryBackend) UpdateWave(id string, name, description *string) (*Wave, error) {
	b.mu.Lock("UpdateWave")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	wave, ok := b.resolveWaveLocked(id)
	if !ok {
		return nil, notFoundError(resourceWave, id)
	}

	if name != nil {
		wave.Name = *name
	}

	if description != nil {
		wave.Description = *description
	}

	wave.LastModifiedDateTime = nowRFC3339()

	return wave.clone(), nil
}

// DeleteWave deletes a Wave. Rejected (ConflictException) if it still has
// associated Applications.
func (b *InMemoryBackend) DeleteWave(id string) error {
	b.mu.Lock("DeleteWave")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	wave, ok := b.resolveWaveLocked(id)
	if !ok {
		return notFoundError(resourceWave, id)
	}

	if b.waveHasApplicationsLocked(id) {
		return conflictErrorWithResource(resourceWave, id, "wave still has associated applications: "+id)
	}

	if wave.Tags != nil {
		wave.Tags.Close()
	}

	b.waves.Delete(id)

	return nil
}

func (b *InMemoryBackend) waveHasApplicationsLocked(waveID string) bool {
	for _, a := range b.applications.All() {
		if a.WaveID == waveID {
			return true
		}
	}

	return false
}

// ListWavesFilters mirrors types.ListWavesRequestFilters.
type ListWavesFilters struct {
	IsArchived *bool
	WaveIDs    []string
}

func matchesWaveFilter(w *Wave, f ListWavesFilters) bool {
	if len(f.WaveIDs) > 0 && !containsStr(f.WaveIDs, w.WaveID) {
		return false
	}

	if f.IsArchived != nil && w.IsArchived != *f.IsArchived {
		return false
	}

	return true
}

// ListWaves returns a page of Waves matching f.
func (b *InMemoryBackend) ListWaves(f ListWavesFilters, token string, limit int) (page.Page[*Wave], error) {
	b.mu.RLock("ListWaves")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*Wave]{}, err
	}

	all := b.waves.Snapshot()
	filtered := make([]*Wave, 0, len(all))

	for _, w := range all {
		if matchesWaveFilter(w, f) {
			filtered = append(filtered, w.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// ArchiveWave sets a Wave's IsArchived flag true.
func (b *InMemoryBackend) ArchiveWave(id string) (*Wave, error) { return b.setWaveArchived(id, true) }

// UnarchiveWave sets a Wave's IsArchived flag false.
func (b *InMemoryBackend) UnarchiveWave(id string) (*Wave, error) {
	return b.setWaveArchived(id, false)
}

func (b *InMemoryBackend) setWaveArchived(id string, archived bool) (*Wave, error) {
	b.mu.Lock("setWaveArchived")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	wave, ok := b.resolveWaveLocked(id)
	if !ok {
		return nil, notFoundError(resourceWave, id)
	}

	wave.IsArchived = archived
	wave.LastModifiedDateTime = nowRFC3339()

	return wave.clone(), nil
}

// AssociateApplications associates applicationIDs with waveID (each
// Application.WaveID reverse pointer is set) and recomputes the Wave's
// rollup status.
func (b *InMemoryBackend) AssociateApplications(waveID string, applicationIDs []string) error {
	b.mu.Lock("AssociateApplications")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	wave, ok := b.resolveWaveLocked(waveID)
	if !ok {
		return notFoundError(resourceWave, waveID)
	}

	for _, id := range applicationIDs {
		app, found := b.resolveApplicationLocked(id)
		if !found {
			return notFoundError(resourceApplication, id)
		}

		app.WaveID = waveID
	}

	b.recomputeWaveStatusLocked(wave)

	return nil
}

// DisassociateApplications removes the association between waveID and
// applicationIDs.
func (b *InMemoryBackend) DisassociateApplications(waveID string, applicationIDs []string) error {
	b.mu.Lock("DisassociateApplications")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	wave, ok := b.resolveWaveLocked(waveID)
	if !ok {
		return notFoundError(resourceWave, waveID)
	}

	for _, id := range applicationIDs {
		app, found := b.resolveApplicationLocked(id)
		if !found {
			return notFoundError(resourceApplication, id)
		}

		if app.WaveID == waveID {
			app.WaveID = ""
		}
	}

	b.recomputeWaveStatusLocked(wave)

	return nil
}

// recomputeWaveStatusLocked recomputes wave's AggregatedStatus rollup from
// its currently-associated Applications' own (already-recomputed) rollup
// statuses -- a one-level-up rollup, using the same documented,
// invented aggregation rule applications.go's recomputeApplicationStatusLocked
// applies at the SourceServer level (not SDK-specified, PARITY.md).
// ReplicationStartedDateTime is set the first time any member Application
// leaves NOT_STARTED, and never cleared. Callers must hold b.mu.
func (b *InMemoryBackend) recomputeWaveStatusLocked(wave *Wave) {
	members := b.waveMembersLocked(wave.WaveID)

	if wave.AggregatedStatus == nil {
		wave.AggregatedStatus = &WaveAggregatedStatus{}
	}

	wave.AggregatedStatus.TotalApplications = int64(len(members))
	wave.AggregatedStatus.HealthStatus = rollupWaveHealthStatus(members)
	wave.AggregatedStatus.ProgressStatus = rollupWaveProgressStatus(members)
	wave.AggregatedStatus.LastUpdateDateTime = nowRFC3339()

	if wave.AggregatedStatus.ReplicationStartedDateTime == "" &&
		wave.AggregatedStatus.ProgressStatus != ProgressStatusNotStarted {
		wave.AggregatedStatus.ReplicationStartedDateTime = nowRFC3339()
	}
}

func (b *InMemoryBackend) waveMembersLocked(waveID string) []*Application {
	var members []*Application

	for _, a := range b.applications.All() {
		if a.WaveID == waveID {
			members = append(members, a)
		}
	}

	return members
}

func rollupWaveHealthStatus(members []*Application) string {
	lagging := false

	for _, a := range members {
		if a.AggregatedStatus == nil {
			continue
		}

		switch a.AggregatedStatus.HealthStatus {
		case HealthStatusError:
			return HealthStatusError
		case HealthStatusLagging:
			lagging = true
		}
	}

	if lagging {
		return HealthStatusLagging
	}

	return HealthStatusHealthy
}

func rollupWaveProgressStatus(members []*Application) string {
	if len(members) == 0 {
		return ProgressStatusNotStarted
	}

	allCompleted := true
	anyStarted := false

	for _, a := range members {
		status := ProgressStatusNotStarted
		if a.AggregatedStatus != nil {
			status = a.AggregatedStatus.ProgressStatus
		}

		if status != ProgressStatusCompleted {
			allCompleted = false
		}

		if status != ProgressStatusNotStarted {
			anyStarted = true
		}
	}

	switch {
	case allCompleted:
		return ProgressStatusCompleted
	case anyStarted:
		return ProgressStatusInProgress
	default:
		return ProgressStatusNotStarted
	}
}
