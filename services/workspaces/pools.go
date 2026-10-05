package workspaces

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// poolsRunningModeAlwaysOn is the default running mode for a newly created
// pool when the caller doesn't specify RunningMode (real CreateWorkspacesPoolInput
// makes RunningMode optional).
const poolsRunningModeAlwaysOn = "ALWAYS_ON"

// poolStateStopped is the WorkspacesPoolState value StopWorkspacesPool sets
// and the only state UpdateWorkspacesPool may change RunningMode in.
const poolStateStopped = "STOPPED"

// poolsPageSize is this backend's default page size for
// DescribeWorkspacesPools; real AWS doesn't document an exact default, so
// this is chosen generously (larger than any realistic per-account pool
// count) so pagination only activates when a caller explicitly requests a
// smaller Limit.
const poolsPageSize = 100

// poolSessionsPageSize is DescribeWorkspacesPoolSessions' default page size.
// Unlike poolsPageSize, real AWS documents this one exactly: "The default
// value is 20 and the maximum value is 50" (DescribeWorkspacesPoolSessionsInput.Limit).
const poolSessionsPageSize = 20

// CreateWorkspacesPool creates a new workspace pool.
func (b *InMemoryBackend) CreateWorkspacesPool(
	poolName, bundleID, directoryID, description, runningMode string,
	desiredUserSessions int32,
	tags map[string]string,
	settings PoolSettings,
) (*storedPool, error) {
	if err := validatePoolSettings(settings); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateWorkspacesPool")
	defer b.mu.Unlock()

	if runningMode == "" {
		runningMode = poolsRunningModeAlwaysOn
	}

	id := b.nextID("wsp-")
	arn := fmt.Sprintf(
		"arn:aws:workspaces:%s:%s:workspacespool/%s",
		b.region, b.accountID, id,
	)

	stored := cloneTags(tags)
	pool := &storedPool{
		PoolID:              id,
		PoolArn:             arn,
		PoolName:            poolName,
		BundleID:            bundleID,
		DirectoryID:         directoryID,
		Description:         description,
		State:               "RUNNING",
		RunningMode:         runningMode,
		DesiredUserSessions: desiredUserSessions,
		CreatedAt:           time.Now().UTC(),
		Tags:                stored,
	}
	applyPoolSettings(pool, settings)
	b.pools.Put(pool)
	b.tags[id] = stored

	return pool, nil
}

// DescribeWorkspacesPools returns pools, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeWorkspacesPools(
	poolIDs []string, limit int32, nextToken string,
) ([]*storedPool, string, error) {
	return b.DescribeWorkspacesPoolsFiltered(poolIDs, nil, limit, nextToken)
}

// DescribeWorkspacesPoolsFiltered is DescribeWorkspacesPools narrowed by PoolName filter conditions.
func (b *InMemoryBackend) DescribeWorkspacesPoolsFiltered(
	poolIDs []string, filters []PoolFilter, limit int32, nextToken string,
) ([]*storedPool, string, error) {
	if err := validatePoolFilters(filters); err != nil {
		return nil, "", err
	}

	b.mu.RLock("DescribeWorkspacesPools")
	defer b.mu.RUnlock()

	filter := buildFilter(poolIDs)
	all := b.pools.All()

	sort.Slice(all, func(i, j int) bool { return all[i].PoolID < all[j].PoolID })

	result := make([]*storedPool, 0, len(all))

	for _, p := range all {
		if !matchesFilter(filter, p.PoolID) || !poolMatchesFilters(p, filters) {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	pg := page.New(result, nextToken, int(limit), poolsPageSize)

	return pg.Data, pg.Next, nil
}

// StartWorkspacesPool transitions a pool to RUNNING.
func (b *InMemoryBackend) StartWorkspacesPool(poolID string) error {
	b.mu.Lock("StartWorkspacesPool")
	defer b.mu.Unlock()

	p, ok := b.pools.Get(poolID)
	if !ok {
		return errPoolNotFound
	}

	p.State = "RUNNING"

	return nil
}

// StopWorkspacesPool transitions a pool to STOPPED.
func (b *InMemoryBackend) StopWorkspacesPool(poolID string) error {
	b.mu.Lock("StopWorkspacesPool")
	defer b.mu.Unlock()

	p, ok := b.pools.Get(poolID)
	if !ok {
		return errPoolNotFound
	}

	p.State = poolStateStopped

	return nil
}

// TerminateWorkspacesPool removes a pool.
func (b *InMemoryBackend) TerminateWorkspacesPool(poolID string) error {
	b.mu.Lock("TerminateWorkspacesPool")
	defer b.mu.Unlock()

	if !b.pools.Has(poolID) {
		return errPoolNotFound
	}

	b.pools.Delete(poolID)

	return nil
}

// UpdateWorkspacesPool updates pool fields. Fields left at their zero value
// (empty string / zero int) are left unchanged, matching the real API's
// "only specified fields are updated" partial-update semantics.
func (b *InMemoryBackend) UpdateWorkspacesPool(
	poolID, description, bundleID, directoryID, runningMode string,
	desiredUserSessions int32,
	settings PoolSettings,
) (*storedPool, error) {
	if err := validatePoolSettings(settings); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateWorkspacesPool")
	defer b.mu.Unlock()

	p, ok := b.pools.Get(poolID)
	if !ok {
		return nil, errPoolNotFound
	}

	if runningMode != "" && p.State != poolStateStopped {
		return nil, errPoolRunningModeRequiresStopped
	}

	if description != "" {
		p.Description = description
	}

	if bundleID != "" {
		p.BundleID = bundleID
	}

	if directoryID != "" {
		p.DirectoryID = directoryID
	}

	if runningMode != "" {
		p.RunningMode = runningMode
	}

	if desiredUserSessions != 0 {
		p.DesiredUserSessions = desiredUserSessions
	}

	applyPoolSettings(p, settings)

	cp := *p

	return &cp, nil
}

// DescribeWorkspacesPoolSessions returns sessions for a pool.
func (b *InMemoryBackend) DescribeWorkspacesPoolSessions(
	poolID, _ /*userID*/ string, limit int32, nextToken string,
) ([]*storedPoolSession, string, error) {
	b.mu.RLock("DescribeWorkspacesPoolSessions")
	defer b.mu.RUnlock()

	all := b.poolSessions.All()

	sort.Slice(all, func(i, j int) bool { return all[i].SessionID < all[j].SessionID })

	result := make([]*storedPoolSession, 0, len(all))

	for _, s := range all {
		if s.PoolID != poolID {
			continue
		}

		cp := *s
		result = append(result, &cp)
	}

	pg := page.New(result, nextToken, int(limit), poolSessionsPageSize)

	return pg.Data, pg.Next, nil
}

// TerminateWorkspacesPoolSession removes a pool session.
func (b *InMemoryBackend) TerminateWorkspacesPoolSession(sessionID string) error {
	b.mu.Lock("TerminateWorkspacesPoolSession")
	defer b.mu.Unlock()

	if !b.poolSessions.Has(sessionID) {
		return errPoolSessionNotFound
	}

	b.poolSessions.Delete(sessionID)

	return nil
}

func validatePoolSettings(s PoolSettings) error {
	if a := s.ApplicationSettings; a != nil && a.Status != "ENABLED" && a.Status != "DISABLED" {
		return awserr.Newf("invalid ApplicationSettings.Status: %q", awserr.ErrInvalidParameter, a.Status)
	}

	return nil
}

// applyPoolSettings merges the set members into p; unset members keep their value.
func applyPoolSettings(p *storedPool, s PoolSettings) {
	if s.ApplicationSettings != nil {
		a := *s.ApplicationSettings
		p.ApplicationSettings = &a
	}

	if t := s.TimeoutSettings; t != nil {
		merged := PoolTimeoutSettings{}
		if p.TimeoutSettings != nil {
			merged = *p.TimeoutSettings
		}

		if t.DisconnectTimeoutInSeconds != nil {
			merged.DisconnectTimeoutInSeconds = t.DisconnectTimeoutInSeconds
		}

		if t.IdleDisconnectTimeoutInSeconds != nil {
			merged.IdleDisconnectTimeoutInSeconds = t.IdleDisconnectTimeoutInSeconds
		}

		if t.MaxUserDurationInSeconds != nil {
			merged.MaxUserDurationInSeconds = t.MaxUserDurationInSeconds
		}

		p.TimeoutSettings = &merged
	}
}
