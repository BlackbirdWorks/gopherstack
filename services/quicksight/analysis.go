package quicksight

import (
	"fmt"
	"maps"
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

func (b *InMemoryBackend) DeleteAnalysis(accountID, analysisID string, forceDeleteWithoutRecovery bool) error {
	b.mu.Lock("DeleteAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses.Get(key)
	if !ok {
		return ErrAnalysisNotFound
	}

	if forceDeleteWithoutRecovery {
		delete(b.tags, a.Arn)
		b.analyses.Delete(key)
	} else {
		a.Status = statusDeleted
	}

	return nil
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
