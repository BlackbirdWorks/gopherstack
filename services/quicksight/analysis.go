package quicksight

import (
	"fmt"
	"maps"
	"math"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---- Analyses ----

func (b *InMemoryBackend) CreateAnalysis(
	accountID, analysisID, name, themeArn string,
	definition map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Analysis, error) {
	if analysisID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAnalysis")
	defer b.mu.Unlock()

	b.pruneDeletedAnalysesLocked(time.Now().UTC())

	key := analysisKey(accountID, analysisID)
	if b.analyses.Has(key) {
		return nil, ErrAnalysisAlreadyExists
	}

	now := time.Now().UTC()
	a := &storedAnalysis{
		CreatedTime:     now,
		LastUpdatedTime: now,
		AnalysisID:      analysisID,
		Arn:             arn.Build("quicksight", b.region, accountID, fmt.Sprintf("analysis/%s", analysisID)),
		Name:            name,
		ThemeArn:        themeArn,
		Status:          statusCreationSuccessful,
		Definition:      definition,
		Permissions:     clonePermissions(permissions),
	}
	b.analyses.Put(a)

	if len(tags) > 0 {
		b.tags[a.Arn] = maps.Clone(tags)
	}

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) DescribeAnalysis(accountID, analysisID string) (*Analysis, error) {
	b.mu.RLock("DescribeAnalysis")
	defer b.mu.RUnlock()

	a, ok := b.analyses.Get(analysisKey(accountID, analysisID))
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) UpdateAnalysis(
	accountID, analysisID, name, themeArn string,
	definition map[string]any,
) (*Analysis, error) {
	b.mu.Lock("UpdateAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses.Get(key)
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	if name != "" {
		a.Name = name
	}
	if themeArn != "" {
		a.ThemeArn = themeArn
	}
	if definition != nil {
		a.Definition = definition
	}
	a.LastUpdatedTime = time.Now().UTC()
	a.Status = statusUpdateSuccessful

	return a.toAnalysis(), nil
}

// defaultAnalysisRecoveryWindowDays is DeleteAnalysisInput.RecoveryWindowInDays'
// documented default ("The default value is 30.").
const defaultAnalysisRecoveryWindowDays = 30

// pruneDeletedAnalysesLocked evicts analyses past their PermanentDeletionAt
// deadline, matching real AWS: DeleteAnalysisOutput's DeletionTime is when
// the analysis "will be permanently deleted." Previously this deadline was
// computed and returned to the caller but never stored, so nothing ever
// evicted the row -- an unbounded-memory-growth leak in the same class
// ec2/ecs/medialive/ram/acmpca fixed for their own delete-waiter tombstones.
// Caller must hold the write lock.
func (b *InMemoryBackend) pruneDeletedAnalysesLocked(now time.Time) {
	for _, a := range b.analyses.All() {
		if a.Status == statusDeleted && !a.PermanentDeletionAt.IsZero() && !now.Before(a.PermanentDeletionAt) {
			b.analyses.Delete(analysisKey(b.accountID, a.AnalysisID))
		}
	}
}

// DeleteAnalysis soft-deletes analysisID (marking it statusDeleted) unless
// forceDeleteWithoutRecovery, which purges it outright. It returns the time
// the analysis is scheduled for permanent deletion -- DeleteAnalysisOutput's
// real DeletionTime member, computed from recoveryWindowInDays (0 defaults
// to 30, matching the documented default) -- or the zero time when force-
// deleted, since there is no scheduled deletion in that case. The same
// deadline is stored on the row so pruneDeletedAnalysesLocked can evict it
// once that time passes, rather than keeping it forever.
func (b *InMemoryBackend) DeleteAnalysis(
	accountID, analysisID string, forceDeleteWithoutRecovery bool, recoveryWindowInDays int64,
) (time.Time, error) {
	b.mu.Lock("DeleteAnalysis")
	defer b.mu.Unlock()

	b.pruneDeletedAnalysesLocked(time.Now().UTC())

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses.Get(key)
	if !ok {
		return time.Time{}, ErrAnalysisNotFound
	}

	if forceDeleteWithoutRecovery {
		delete(b.tags, a.Arn)
		b.analyses.Delete(key)

		return time.Time{}, nil
	}

	days := recoveryWindowInDays
	if days <= 0 {
		days = defaultAnalysisRecoveryWindowDays
	}

	if days > math.MaxInt32 {
		return time.Time{}, ErrValidation
	}

	deletionTime := time.Now().UTC().AddDate(0, 0, int(days))

	a.Status = statusDeleted
	a.PermanentDeletionAt = deletionTime

	return deletionTime, nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListAnalyses(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*Analysis, string, error) {
	b.mu.RLock("ListAnalyses")
	defer b.mu.RUnlock()

	all := b.analyses.All()
	sort.Slice(all, func(i, j int) bool { return all[i].AnalysisID < all[j].AnalysisID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(all)
		for i, a := range all {
			if a.AnalysisID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].AnalysisID
	} else {
		end = len(all)
	}

	result := make([]*Analysis, 0, end-start)
	for _, a := range all[start:end] {
		result = append(result, a.toAnalysis())
	}

	return result, next, nil
}

func (b *InMemoryBackend) RestoreAnalysis(accountID, analysisID string) (*Analysis, error) {
	b.mu.Lock("RestoreAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses.Get(key)
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	a.Status = statusCreationSuccessful
	a.LastUpdatedTime = time.Now().UTC()

	return a.toAnalysis(), nil
}

// SearchAnalyses searches analyses by name (filter Name == filterAnalysisName);
// any other filter Name is an ownership-related filter that this in-memory
// backend doesn't track and is treated as a pass-through match.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchAnalyses(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Analysis, string, error) {
	b.mu.RLock("SearchAnalyses")
	defer b.mu.RUnlock()

	var filtered []*storedAnalysis
	for _, a := range b.analyses.All() {
		if matchesAllNameFilters(a.Name, filters, filterAnalysisName) {
			filtered = append(filtered, a)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].AnalysisID < filtered[j].AnalysisID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range filtered {
			if a.AnalysisID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].AnalysisID
	} else {
		end = len(filtered)
	}

	result := make([]*Analysis, 0, end-start)
	for _, a := range filtered[start:end] {
		result = append(result, a.toAnalysis())
	}

	return result, next, nil
}

// ---- Analysis permissions ----

func (b *InMemoryBackend) DescribeAnalysisPermissions(
	accountID, analysisID string,
) (*Analysis, []ResourcePermission, error) {
	b.mu.RLock("DescribeAnalysisPermissions")
	defer b.mu.RUnlock()

	a, ok := b.analyses.Get(analysisKey(accountID, analysisID))
	if !ok {
		return nil, nil, ErrAnalysisNotFound
	}

	return a.toAnalysis(), clonePermissions(a.Permissions), nil
}

func (b *InMemoryBackend) UpdateAnalysisPermissions(
	accountID, analysisID string,
	grant, revoke []ResourcePermission,
) (*Analysis, []ResourcePermission, error) {
	b.mu.Lock("UpdateAnalysisPermissions")
	defer b.mu.Unlock()

	a, ok := b.analyses.Get(analysisKey(accountID, analysisID))
	if !ok {
		return nil, nil, ErrAnalysisNotFound
	}

	a.Permissions = applyGrantRevoke(a.Permissions, grant, revoke)
	a.LastUpdatedTime = time.Now().UTC()

	return a.toAnalysis(), clonePermissions(a.Permissions), nil
}
