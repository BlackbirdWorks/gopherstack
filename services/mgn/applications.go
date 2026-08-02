package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family E (8 ops): CreateApplication, UpdateApplication,
// DeleteApplication, ListApplications, ArchiveApplication,
// UnarchiveApplication, AssociateSourceServers, DisassociateSourceServers.
//
// Wave contains Application (waves.go's Associate/DisassociateApplications);
// Application contains SourceServer (this file's Associate/
// DisassociateSourceServers) -- SourceServer.ApplicationID is the reverse
// pointer, but there is no direct SourceServer<->Wave association at all
// (PARITY.md's confirmed grouping hierarchy).

func (b *InMemoryBackend) resolveApplicationLocked(id string) (*Application, bool) {
	return b.applications.Get(id)
}

// CreateApplication creates a new Application.
func (b *InMemoryBackend) CreateApplication(name, description string, appTags map[string]string) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if name == "" {
		return nil, validationError("name is required")
	}

	id := newApplicationID()
	now := nowRFC3339()
	t := tags.New("mgn.application." + id + ".tags")
	t.Merge(appTags)

	app := &Application{
		ApplicationID:        id,
		Arn:                  b.applicationARN(id),
		Name:                 name,
		Description:          description,
		Tags:                 t,
		CreationDateTime:     now,
		LastModifiedDateTime: now,
		AggregatedStatus: &ApplicationAggregatedStatus{
			HealthStatus:       HealthStatusHealthy,
			ProgressStatus:     ProgressStatusNotStarted,
			LastUpdateDateTime: now,
		},
	}
	b.applications.Put(app)

	return app.clone(), nil
}

// UpdateApplication applies a partial update to an Application.
func (b *InMemoryBackend) UpdateApplication(id string, name, description *string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	app, ok := b.resolveApplicationLocked(id)
	if !ok {
		return nil, notFoundError(resourceApplication, id)
	}

	if name != nil {
		app.Name = *name
	}

	if description != nil {
		app.Description = *description
	}

	app.LastModifiedDateTime = nowRFC3339()

	return app.clone(), nil
}

// DeleteApplication deletes an Application. Rejected (ConflictException) if
// it still has associated SourceServers.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	app, ok := b.resolveApplicationLocked(id)
	if !ok {
		return notFoundError(resourceApplication, id)
	}

	if b.applicationHasServersLocked(id) {
		return conflictErrorWithResource(
			resourceApplication,
			id,
			"application still has associated source servers: "+id,
		)
	}

	if app.Tags != nil {
		app.Tags.Close()
	}

	b.applications.Delete(id)

	return nil
}

func (b *InMemoryBackend) applicationHasServersLocked(applicationID string) bool {
	for _, s := range b.sourceServers.All() {
		if s.ApplicationID == applicationID {
			return true
		}
	}

	return false
}

// ListApplicationsFilters mirrors types.ListApplicationsRequestFilters.
type ListApplicationsFilters struct {
	IsArchived     *bool
	ApplicationIDs []string
	WaveIDs        []string
}

func matchesApplicationFilter(a *Application, f ListApplicationsFilters) bool {
	if len(f.ApplicationIDs) > 0 && !containsStr(f.ApplicationIDs, a.ApplicationID) {
		return false
	}

	if len(f.WaveIDs) > 0 && !containsStr(f.WaveIDs, a.WaveID) {
		return false
	}

	if f.IsArchived != nil && a.IsArchived != *f.IsArchived {
		return false
	}

	return true
}

// ListApplications returns a page of Applications matching f.
func (b *InMemoryBackend) ListApplications(
	f ListApplicationsFilters,
	token string,
	limit int,
) (page.Page[*Application], error) {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*Application]{}, err
	}

	all := b.applications.Snapshot()
	filtered := make([]*Application, 0, len(all))

	for _, a := range all {
		if matchesApplicationFilter(a, f) {
			filtered = append(filtered, a.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// ArchiveApplication sets an Application's IsArchived flag true.
func (b *InMemoryBackend) ArchiveApplication(id string) (*Application, error) {
	return b.setApplicationArchived(id, true)
}

// UnarchiveApplication sets an Application's IsArchived flag false.
func (b *InMemoryBackend) UnarchiveApplication(id string) (*Application, error) {
	return b.setApplicationArchived(id, false)
}

func (b *InMemoryBackend) setApplicationArchived(id string, archived bool) (*Application, error) {
	b.mu.Lock("setApplicationArchived")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	app, ok := b.resolveApplicationLocked(id)
	if !ok {
		return nil, notFoundError(resourceApplication, id)
	}

	app.IsArchived = archived
	app.LastModifiedDateTime = nowRFC3339()

	return app.clone(), nil
}

// AssociateSourceServers associates sourceServerIDs with applicationID
// (each SourceServer.ApplicationID reverse pointer is set) and recomputes
// the Application's rollup status.
func (b *InMemoryBackend) AssociateSourceServers(applicationID string, sourceServerIDs []string) error {
	b.mu.Lock("AssociateSourceServers")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	app, ok := b.resolveApplicationLocked(applicationID)
	if !ok {
		return notFoundError(resourceApplication, applicationID)
	}

	for _, id := range sourceServerIDs {
		s, found := b.resolveSourceServerLocked(id)
		if !found {
			return notFoundError(resourceSourceServer, id)
		}

		s.ApplicationID = applicationID
	}

	b.recomputeApplicationStatusLocked(app)

	return nil
}

// DisassociateSourceServers removes the association between applicationID
// and sourceServerIDs.
func (b *InMemoryBackend) DisassociateSourceServers(applicationID string, sourceServerIDs []string) error {
	b.mu.Lock("DisassociateSourceServers")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	app, ok := b.resolveApplicationLocked(applicationID)
	if !ok {
		return notFoundError(resourceApplication, applicationID)
	}

	for _, id := range sourceServerIDs {
		s, found := b.resolveSourceServerLocked(id)
		if !found {
			return notFoundError(resourceSourceServer, id)
		}

		if s.ApplicationID == applicationID {
			s.ApplicationID = ""
		}
	}

	b.recomputeApplicationStatusLocked(app)

	return nil
}

// recomputeApplicationStatusLocked recomputes app's AggregatedStatus rollup
// from its currently-associated SourceServers. The exact aggregation rule is
// not SDK-specified (PARITY.md) -- this backend's documented, invented rule:
// HealthStatus is ERROR if any member SourceServer's DataReplicationState is
// STALLED/DISCONNECTED, LAGGING if any is BACKLOG/RESCAN, else HEALTHY;
// ProgressStatus is COMPLETED if every member has reached CONTINUOUS,
// IN_PROGRESS if at least one has started replicating, else NOT_STARTED.
// Callers must hold b.mu.
func (b *InMemoryBackend) recomputeApplicationStatusLocked(app *Application) {
	members := b.applicationMembersLocked(app.ApplicationID)

	if app.AggregatedStatus == nil {
		app.AggregatedStatus = &ApplicationAggregatedStatus{}
	}

	app.AggregatedStatus.TotalSourceServers = int64(len(members))
	app.AggregatedStatus.HealthStatus = rollupHealthStatus(members)
	app.AggregatedStatus.ProgressStatus = rollupProgressStatus(members)
	app.AggregatedStatus.LastUpdateDateTime = nowRFC3339()
}

func (b *InMemoryBackend) applicationMembersLocked(applicationID string) []*SourceServer {
	var members []*SourceServer

	for _, s := range b.sourceServers.All() {
		if s.ApplicationID == applicationID {
			members = append(members, s)
		}
	}

	return members
}

// rollupHealthStatus implements this backend's documented HealthStatus
// aggregation rule -- see recomputeApplicationStatusLocked's doc comment.
func rollupHealthStatus(members []*SourceServer) string {
	lagging := false

	for _, s := range members {
		if s.DataReplicationInfo == nil {
			continue
		}

		switch s.DataReplicationInfo.DataReplicationState {
		case DataReplicationStateStalled, DataReplicationStateDisconnected:
			return HealthStatusError
		case DataReplicationStateBacklog, DataReplicationStateRescan:
			lagging = true
		}
	}

	if lagging {
		return HealthStatusLagging
	}

	return HealthStatusHealthy
}

// rollupProgressStatus implements this backend's documented ProgressStatus
// aggregation rule -- see recomputeApplicationStatusLocked's doc comment.
func rollupProgressStatus(members []*SourceServer) string {
	if len(members) == 0 {
		return ProgressStatusNotStarted
	}

	allContinuous := true
	anyStarted := false

	for _, s := range members {
		state := ""
		if s.DataReplicationInfo != nil {
			state = s.DataReplicationInfo.DataReplicationState
		}

		if state != DataReplicationStateContinuous {
			allContinuous = false
		}

		if state != "" && state != DataReplicationStateStopped {
			anyStarted = true
		}
	}

	switch {
	case allContinuous:
		return ProgressStatusCompleted
	case anyStarted:
		return ProgressStatusInProgress
	default:
		return ProgressStatusNotStarted
	}
}
