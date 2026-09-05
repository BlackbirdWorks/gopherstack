package elasticache

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) appendUpdateActionsLocked(actions ...*UpdateAction) {
	b.updateActions = append(b.updateActions, actions...)
	const maxUpdateActions = 1000
	if len(b.updateActions) > maxUpdateActions {
		b.updateActions = b.updateActions[len(b.updateActions)-maxUpdateActions:]
	}
}

// AppendUpdateActions appends new update action records.
func (b *InMemoryBackend) AppendUpdateActions(actions []*UpdateAction) {
	b.mu.Lock("AppendUpdateActions")
	defer b.mu.Unlock()
	b.appendUpdateActionsLocked(actions...)
}

// ListUpdateActionsByServiceUpdate returns update actions filtered by service update name.
func (b *InMemoryBackend) ListUpdateActionsByServiceUpdate(serviceUpdateName string) []*UpdateAction {
	b.mu.RLock("ListUpdateActionsByServiceUpdate")
	defer b.mu.RUnlock()

	if serviceUpdateName == "" {
		result := make([]*UpdateAction, len(b.updateActions))
		copy(result, b.updateActions)

		return result
	}

	var result []*UpdateAction

	for _, a := range b.updateActions {
		if a.ServiceUpdateName == serviceUpdateName {
			result = append(result, a)
		}
	}

	return result
}

// ----------------------------------------
// Seeded service updates
// ----------------------------------------

// BuiltinServiceUpdates returns a slice of pre-seeded service updates for testing realism.
func BuiltinServiceUpdates() []ServiceUpdate {
	return []ServiceUpdate{
		{ServiceUpdateName: "20240101-001-security-patch", Status: "available"},
		{ServiceUpdateName: "20240201-002-engine-upgrade", Status: "available"},
		{ServiceUpdateName: "20231101-003-security-hotfix", Status: "expired"},
	}
}

// DescribeServiceUpdatesFull returns service updates, filtered by name and/or status list.
func (b *InMemoryBackend) DescribeServiceUpdatesFull(
	serviceUpdateName string,
	status []string,
	marker string,
	maxRecords int,
) ([]ServiceUpdate, string, error) {
	b.mu.RLock("DescribeServiceUpdatesFull")
	defer b.mu.RUnlock()

	all := BuiltinServiceUpdates()
	statusSet := make(map[string]bool, len(status))
	for _, s := range status {
		statusSet[s] = true
	}

	filtered := make([]ServiceUpdate, 0, len(all))
	for _, su := range all {
		if serviceUpdateName != "" && su.ServiceUpdateName != serviceUpdateName {
			continue
		}

		if len(statusSet) > 0 && !statusSet[su.Status] {
			continue
		}

		filtered = append(filtered, su)
	}

	p := newServiceUpdatePage(filtered, marker, maxRecords)

	return p.data, p.next, nil
}

// serviceUpdatePageResult is a simple pagination result for service updates.
type serviceUpdatePageResult struct {
	next string
	data []ServiceUpdate
}

func newServiceUpdatePage(items []ServiceUpdate, marker string, maxRecords int) serviceUpdatePageResult {
	if maxRecords <= 0 {
		maxRecords = elasticacheDefaultMaxRecords
	}

	start := 0
	if marker != "" {
		for i, su := range items {
			if su.ServiceUpdateName == marker {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxRecords, len(items))
	page := items[start:end]
	next := ""

	if end < len(items) {
		next = items[end-1].ServiceUpdateName
	}

	return serviceUpdatePageResult{data: page, next: next}
}

// ----------------------------------------
// DescribeUpdateActionsFull — returns tracked update actions
// ----------------------------------------

// updateActionFilter holds the DescribeUpdateActions constraining
// parameters as membership sets, built once per call by
// newUpdateActionFilter.
type updateActionFilter struct {
	clusterIDs        map[string]bool
	replicationGroups map[string]bool
	statuses          map[string]bool
	serviceUpdateName string
}

func newUpdateActionFilter(
	serviceUpdateName string,
	cacheClusterIDs, replicationGroupIDs, status []string,
) updateActionFilter {
	clusterFilter := make(map[string]bool, len(cacheClusterIDs))
	for _, id := range cacheClusterIDs {
		clusterFilter[id] = true
	}

	rgFilter := make(map[string]bool, len(replicationGroupIDs))
	for _, id := range replicationGroupIDs {
		rgFilter[id] = true
	}

	statusFilter := make(map[string]bool, len(status))
	for _, s := range status {
		statusFilter[s] = true
	}

	return updateActionFilter{
		serviceUpdateName: serviceUpdateName,
		clusterIDs:        clusterFilter,
		replicationGroups: rgFilter,
		statuses:          statusFilter,
	}
}

func (f updateActionFilter) matches(a *UpdateAction) bool {
	if f.serviceUpdateName != "" && a.ServiceUpdateName != f.serviceUpdateName {
		return false
	}
	if len(f.clusterIDs) > 0 && !f.clusterIDs[a.CacheClusterID] {
		return false
	}
	if len(f.replicationGroups) > 0 && !f.replicationGroups[a.ReplicationGroupID] {
		return false
	}
	if len(f.statuses) > 0 && !f.statuses[a.UpdateActionStatus] {
		return false
	}

	return true
}

// DescribeUpdateActionsFull returns update actions filtered by service update
// name, cache cluster/replication group IDs, and update action status, with
// pagination (elasticache@v1.56.4 api_op_DescribeUpdateActions.go).
func (b *InMemoryBackend) DescribeUpdateActionsFull(
	serviceUpdateName, marker string,
	maxRecords int,
	cacheClusterIDs, replicationGroupIDs, updateActionStatus []string,
) ([]UpdateAction, string, error) {
	b.mu.RLock("DescribeUpdateActionsFull")
	defer b.mu.RUnlock()

	if maxRecords <= 0 {
		maxRecords = elasticacheDefaultMaxRecords
	}

	filter := newUpdateActionFilter(serviceUpdateName, cacheClusterIDs, replicationGroupIDs, updateActionStatus)

	all := make([]UpdateAction, 0, len(b.updateActions))
	for _, a := range b.updateActions {
		if filter.matches(a) {
			all = append(all, *a)
		}
	}

	start := 0
	if marker != "" {
		for i, ua := range all {
			key := ua.ReplicationGroupID + ua.CacheClusterID + ua.ServiceUpdateName
			if key == marker {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxRecords, len(all))
	page := all[start:end]
	next := ""

	if end < len(all) {
		ua := all[end-1]
		next = ua.ReplicationGroupID + ua.CacheClusterID + ua.ServiceUpdateName
	}

	return page, next, nil
}

// batchUpdateActions processes a list of replication groups and clusters for a service update,
// returning processed (found) and unprocessed (not found) update action results.
func (b *InMemoryBackend) batchUpdateActions(
	replicationGroupIDs, cacheClusterIDs []string,
	serviceUpdateName, matchedStatus string,
) *BatchUpdateResult {
	result := &BatchUpdateResult{
		ProcessedUpdateActions:   []UpdateActionResult{},
		UnprocessedUpdateActions: []UpdateActionResult{},
	}

	for _, rgID := range replicationGroupIDs {
		found := false
		for _, regionRGs := range b.replicationGroups {
			if _, ok := regionRGs.Get(rgID); ok {
				found = true

				break
			}
		}
		if found {
			result.ProcessedUpdateActions = append(result.ProcessedUpdateActions, UpdateActionResult{
				ReplicationGroupID: rgID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: matchedStatus,
			})
		} else {
			result.UnprocessedUpdateActions = append(result.UnprocessedUpdateActions, UpdateActionResult{
				ReplicationGroupID: rgID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: "not-applicable",
			})
		}
	}

	for _, clusterID := range cacheClusterIDs {
		found := false
		for _, regionClusters := range b.clusters {
			if _, ok := regionClusters.Get(clusterID); ok {
				found = true

				break
			}
		}
		if found {
			result.ProcessedUpdateActions = append(result.ProcessedUpdateActions, UpdateActionResult{
				CacheClusterID:     clusterID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: matchedStatus,
			})
		} else {
			result.UnprocessedUpdateActions = append(result.UnprocessedUpdateActions, UpdateActionResult{
				CacheClusterID:     clusterID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: "not-applicable",
			})
		}
	}

	sort.Slice(result.ProcessedUpdateActions, func(i, j int) bool {
		ki := result.ProcessedUpdateActions[i].ReplicationGroupID + result.ProcessedUpdateActions[i].CacheClusterID
		kj := result.ProcessedUpdateActions[j].ReplicationGroupID + result.ProcessedUpdateActions[j].CacheClusterID

		return ki < kj
	})

	return result
}

// ----------------------------------------
// BatchApplyUpdateAction
// ----------------------------------------

// BatchApplyUpdateAction schedules a service update for the given replication groups and clusters.
func (b *InMemoryBackend) BatchApplyUpdateAction(
	_ context.Context,
	replicationGroupIDs, cacheClusterIDs []string,
	serviceUpdateName string,
) (*BatchUpdateResult, error) {
	b.mu.Lock("BatchApplyUpdateAction")
	defer b.mu.Unlock()

	result := b.batchUpdateActions(replicationGroupIDs, cacheClusterIDs, serviceUpdateName, "scheduling")

	for _, ua := range result.ProcessedUpdateActions {
		action := &UpdateAction{
			ReplicationGroupID: ua.ReplicationGroupID,
			CacheClusterID:     ua.CacheClusterID,
			ServiceUpdateName:  ua.ServiceUpdateName,
			UpdateActionStatus: ua.UpdateActionStatus,
		}
		b.updateActions = append(b.updateActions, action)
	}

	return result, nil
}

// ----------------------------------------
// BatchStopUpdateAction
// ----------------------------------------

// BatchStopUpdateAction stops a pending service update for the given replication groups and clusters.
func (b *InMemoryBackend) BatchStopUpdateAction(
	_ context.Context,
	replicationGroupIDs, cacheClusterIDs []string,
	serviceUpdateName string,
) (*BatchUpdateResult, error) {
	b.mu.Lock("BatchStopUpdateAction")
	defer b.mu.Unlock()

	result := b.batchUpdateActions(replicationGroupIDs, cacheClusterIDs, serviceUpdateName, "stopped")

	// Without this, a stopped action's UpdateActionStatus stayed "scheduling"
	// forever in DescribeUpdateActions -- batchUpdateActions above only
	// computes the response, it never touches the tracked records.
	for _, processed := range result.ProcessedUpdateActions {
		for _, action := range b.updateActions {
			if action.ServiceUpdateName == serviceUpdateName &&
				action.ReplicationGroupID == processed.ReplicationGroupID &&
				action.CacheClusterID == processed.CacheClusterID {
				action.UpdateActionStatus = "stopped"
			}
		}
	}

	return result, nil
}

// ----------------------------------------
// CompleteMigration
// ----------------------------------------

// DescribeServiceUpdates returns service updates, filtered by name and status.
func (b *InMemoryBackend) DescribeServiceUpdates(
	_ context.Context,
	serviceUpdateName string,
	marker string,
	maxRecords int,
	status []string,
) (page.Page[ServiceUpdate], error) {
	data, next, err := b.DescribeServiceUpdatesFull(serviceUpdateName, status, marker, maxRecords)
	if err != nil {
		return page.Page[ServiceUpdate]{}, err
	}

	return page.Page[ServiceUpdate]{Data: data, Next: next}, nil
}

// DescribeUpdateActions returns update actions, filtered by service update name.
func (b *InMemoryBackend) DescribeUpdateActions(
	_ context.Context,
	serviceUpdateName string,
	marker string,
	maxRecords int,
	cacheClusterIDs, replicationGroupIDs, updateActionStatus []string,
) (page.Page[UpdateAction], error) {
	data, next, err := b.DescribeUpdateActionsFull(
		serviceUpdateName, marker, maxRecords, cacheClusterIDs, replicationGroupIDs, updateActionStatus,
	)
	if err != nil {
		return page.Page[UpdateAction]{}, err
	}

	return page.Page[UpdateAction]{Data: data, Next: next}, nil
}
