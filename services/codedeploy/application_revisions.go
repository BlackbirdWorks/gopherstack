package codedeploy

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// maxBatchRevisions is the maximum number of revisions accepted by BatchGetApplicationRevisions.
const maxBatchRevisions = 25

// RegisterApplicationRevision registers (or re-registers) a revision for an
// application. Re-registering an already-known revision refreshes its
// description (when a non-empty one is supplied) but preserves the original
// RegisterTime, matching real CodeDeploy's "registering an already-registered
// revision is a no-op besides the description" behavior.
func (b *InMemoryBackend) RegisterApplicationRevision(
	appName string, revision RevisionLocation, description string,
) error {
	b.mu.Lock("RegisterApplicationRevision")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	key := applicationRevisionKey(appName, revision)

	if existing, ok := b.applicationRevisions.Get(key); ok {
		if description != "" {
			existing.Description = description
		}

		return nil
	}

	b.applicationRevisions.Put(&ApplicationRevision{
		ApplicationName: appName,
		Revision:        revision,
		Description:     description,
		RegisterTime:    time.Now().UTC(),
	})

	return nil
}

// touchApplicationRevisionForDeployment records that revision was just used
// to create a deployment against dgName: it auto-registers the revision if
// unseen (real CodeDeploy auto-registers revisions supplied directly to
// CreateDeployment), stamps FirstUsedTime/LastUsedTime, and updates which
// deployment group currently targets it (a deployment group targets exactly
// one revision at a time, so dgName is removed from every other revision of
// the same application). Callers must already hold b.mu.Lock.
func (b *InMemoryBackend) touchApplicationRevisionForDeployment(appName, dgName string, revision *RevisionLocation) {
	if revision == nil {
		return
	}

	now := time.Now().UTC()
	key := applicationRevisionKey(appName, *revision)

	rev, ok := b.applicationRevisions.Get(key)
	if !ok {
		rev = &ApplicationRevision{
			ApplicationName: appName,
			Revision:        *revision,
			RegisterTime:    now,
		}
		b.applicationRevisions.Put(rev)
	}

	if rev.FirstUsedTime == nil {
		first := now
		rev.FirstUsedTime = &first
	}

	last := now
	rev.LastUsedTime = &last

	if !slices.Contains(rev.DeploymentGroups, dgName) {
		rev.DeploymentGroups = append(rev.DeploymentGroups, dgName)
	}

	for _, other := range b.applicationRevisionsByApp.Get(appName) {
		if other == rev {
			continue
		}

		other.DeploymentGroups = slices.DeleteFunc(other.DeploymentGroups, func(g string) bool { return g == dgName })
	}
}

// GetApplicationRevision returns a registered application revision by its
// (appName, revision) identity.
func (b *InMemoryBackend) GetApplicationRevision(
	appName string, revision RevisionLocation,
) (*ApplicationRevision, error) {
	b.mu.RLock("GetApplicationRevision")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	rev, ok := b.applicationRevisions.Get(applicationRevisionKey(appName, revision))
	if !ok {
		return nil, fmt.Errorf("%w: revision not registered for application %s", ErrRevisionNotFound, appName)
	}

	cp := *rev

	return &cp, nil
}

// ListApplicationRevisions returns registered revisions for an application,
// filtered and sorted per filter.
func (b *InMemoryBackend) ListApplicationRevisions(
	appName string, filter RevisionListFilter,
) ([]*ApplicationRevision, error) {
	b.mu.RLock("ListApplicationRevisions")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	entries := b.applicationRevisionsByApp.Get(appName)
	out := make([]*ApplicationRevision, 0, len(entries))

	for _, rev := range entries {
		if !revisionMatchesFilter(rev, filter) {
			continue
		}

		cp := *rev
		out = append(out, &cp)
	}

	sortApplicationRevisions(out, filter.SortBy, filter.SortOrder)

	return out, nil
}

// revisionMatchesFilter reports whether rev satisfies the Deployed/S3Bucket/S3KeyPrefix
// filters of a ListApplicationRevisions request.
func revisionMatchesFilter(rev *ApplicationRevision, filter RevisionListFilter) bool {
	switch filter.Deployed {
	case "include":
		if len(rev.DeploymentGroups) == 0 {
			return false
		}
	case "exclude":
		if len(rev.DeploymentGroups) > 0 {
			return false
		}
	}

	if filter.S3Bucket != "" || filter.S3KeyPrefix != "" {
		if rev.Revision.S3Location == nil {
			return false
		}
		if filter.S3Bucket != "" && rev.Revision.S3Location.Bucket != filter.S3Bucket {
			return false
		}
		if filter.S3KeyPrefix != "" && !strings.HasPrefix(rev.Revision.S3Location.Key, filter.S3KeyPrefix) {
			return false
		}
	}

	return true
}

// revisionSortKey extracts the time.Time field named by sortBy, defaulting to
// RegisterTime for an unrecognized or empty sortBy.
func revisionSortKey(rev *ApplicationRevision, sortBy string) time.Time {
	switch sortBy {
	case "firstUsedTime":
		if rev.FirstUsedTime != nil {
			return *rev.FirstUsedTime
		}

		return time.Time{}
	case "lastUsedTime":
		if rev.LastUsedTime != nil {
			return *rev.LastUsedTime
		}

		return time.Time{}
	default:
		return rev.RegisterTime
	}
}

// sortApplicationRevisions orders revs in place by sortBy (registerTime |
// firstUsedTime | lastUsedTime, default registerTime) and sortOrder
// (ascending | descending, default ascending), breaking ties on the
// canonical revision key for determinism.
func sortApplicationRevisions(revs []*ApplicationRevision, sortBy, sortOrder string) {
	sort.SliceStable(revs, func(i, j int) bool {
		ti := revisionSortKey(revs[i], sortBy)
		tj := revisionSortKey(revs[j], sortBy)

		if !ti.Equal(tj) {
			if sortOrder == "descending" {
				return ti.After(tj)
			}

			return ti.Before(tj)
		}

		ki := applicationRevisionKey(revs[i].ApplicationName, revs[i].Revision)
		kj := applicationRevisionKey(revs[j].ApplicationName, revs[j].Revision)

		return ki < kj
	})
}

// BatchGetApplicationRevisions validates that the application exists and
// returns the registered ApplicationRevision for each requested location that
// is actually registered (revisions never registered are silently omitted
// from the map, matching the codebase-wide "not found -> omitted" Batch*
// convention -- see BatchGetApplications/BatchGetDeploymentGroups).
func (b *InMemoryBackend) BatchGetApplicationRevisions(
	appName string,
	revisions []RevisionLocation,
) (map[string]*ApplicationRevision, error) {
	b.mu.RLock("BatchGetApplicationRevisions")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	if len(revisions) > maxBatchRevisions {
		return nil, fmt.Errorf("%w: at most %d revisions can be requested at once, got %d",
			ErrValidation, maxBatchRevisions, len(revisions))
	}

	found := make(map[string]*ApplicationRevision, len(revisions))

	for _, r := range revisions {
		rev, ok := b.applicationRevisions.Get(applicationRevisionKey(appName, r))
		if !ok {
			continue
		}

		cp := *rev
		found[applicationRevisionKey(appName, r)] = &cp
	}

	return found, nil
}

// deleteApplicationRevisions removes every revision registered for appName.
// Callers must already hold b.mu.Lock. Factored out of DeleteApplication to
// keep that function's own complexity low.
func (b *InMemoryBackend) deleteApplicationRevisions(appName string) {
	for _, rev := range slices.Clone(b.applicationRevisionsByApp.Get(appName)) {
		b.applicationRevisions.Delete(applicationRevisionKey(rev.ApplicationName, rev.Revision))
	}
}

// renameApplicationRevisions moves every revision registered for oldName to
// newName, preserving their Revision identity (only ApplicationName, and
// therefore the composite table key, changes). Callers must already hold
// b.mu.Lock. Factored out of UpdateApplication to keep that function's own
// complexity low.
func (b *InMemoryBackend) renameApplicationRevisions(oldName, newName string) {
	for _, rev := range slices.Clone(b.applicationRevisionsByApp.Get(oldName)) {
		b.applicationRevisions.Delete(applicationRevisionKey(oldName, rev.Revision))
		rev.ApplicationName = newName
		b.applicationRevisions.Put(rev)
	}
}
