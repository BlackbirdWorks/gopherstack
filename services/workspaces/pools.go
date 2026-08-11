package workspaces

import (
	"fmt"
	"time"
)

// poolsRunningModeAlwaysOn is the default running mode for a newly created
// pool when the caller doesn't specify RunningMode (real CreateWorkspacesPoolInput
// makes RunningMode optional).
const poolsRunningModeAlwaysOn = "ALWAYS_ON"

// poolStateStopped is the WorkspacesPoolState value StopWorkspacesPool sets
// and the only state UpdateWorkspacesPool may change RunningMode in.
const poolStateStopped = "STOPPED"

// CreateWorkspacesPool creates a new workspace pool.
func (b *InMemoryBackend) CreateWorkspacesPool(
	poolName, bundleID, directoryID, description, runningMode string,
	desiredUserSessions int32,
	tags map[string]string,
) (*storedPool, error) {
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
	b.pools.Put(pool)
	b.tags[id] = stored

	return pool, nil
}

// DescribeWorkspacesPools returns pools, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeWorkspacesPools(
	poolIDs []string, _ int32, _ string,
) ([]*storedPool, string, error) {
	b.mu.RLock("DescribeWorkspacesPools")
	defer b.mu.RUnlock()

	filter := buildFilter(poolIDs)
	var result []*storedPool

	for _, p := range b.pools.All() {
		if !matchesFilter(filter, p.PoolID) {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedPool{}
	}

	return result, "", nil
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
) (*storedPool, error) {
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

	cp := *p

	return &cp, nil
}

// DescribeWorkspacesPoolSessions returns sessions for a pool.
func (b *InMemoryBackend) DescribeWorkspacesPoolSessions(
	poolID, _ /*userID*/ string, _ int32, _ string,
) ([]*storedPoolSession, string, error) {
	b.mu.RLock("DescribeWorkspacesPoolSessions")
	defer b.mu.RUnlock()

	var result []*storedPoolSession

	for _, s := range b.poolSessions.All() {
		if s.PoolID != poolID {
			continue
		}

		cp := *s
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedPoolSession{}
	}

	return result, "", nil
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
