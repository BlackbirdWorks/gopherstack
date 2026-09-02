package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateNetworkInsightsPath(
	sourceID, destinationID, protocol string,
	destinationPort int,
) (*NetworkInsightsPath, error) {
	b.mu.Lock("CreateNetworkInsightsPath")
	defer b.mu.Unlock()

	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceId is required", ErrInvalidParameter)
	}

	id := "nip-" + uuid.New().String()[:8]
	p := &NetworkInsightsPath{
		NetworkInsightsPathID:  id,
		NetworkInsightsPathArn: "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":network-insights-path/" + id,
		SourceID:               sourceID,
		DestinationID:          destinationID,
		Protocol:               protocol,
		DestinationPort:        destinationPort,
	}
	b.networkInsightsPaths.Put(p)

	cp := *p

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsPath(id string) error {
	b.mu.Lock("DeleteNetworkInsightsPath")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsPaths.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsPathNotFound, id)
	}
	b.networkInsightsPaths.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsPaths(ids []string) []*NetworkInsightsPath {
	b.mu.RLock("DescribeNetworkInsightsPaths")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsPath

	for _, p := range b.networkInsightsPaths.All() {
		if len(ids) > 0 && !slices.Contains(ids, p.NetworkInsightsPathID) {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsPathID < result[j].NetworkInsightsPathID
	})

	return result
}

func (b *InMemoryBackend) StartNetworkInsightsAnalysis(pathID string) (*NetworkInsightsAnalysis, error) {
	b.mu.Lock("StartNetworkInsightsAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsPaths.Get(pathID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInsightsPathNotFound, pathID)
	}

	id := "nia-" + uuid.New().String()[:8]
	a := &NetworkInsightsAnalysis{
		NetworkInsightsAnalysisID: id,
		NetworkInsightsPathID:     pathID,
		Status:                    stateAnalysisSucceeded,
		NetworkPathFound:          true,
	}
	b.networkInsightsAnalyses.Put(a)

	cp := *a

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAnalysis(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAnalyses.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAnalysisNotFound, id)
	}
	b.networkInsightsAnalyses.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAnalyses(ids []string, pathID string) []*NetworkInsightsAnalysis {
	b.mu.RLock("DescribeNetworkInsightsAnalyses")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAnalysis

	for _, a := range b.networkInsightsAnalyses.All() {
		if len(ids) > 0 && !slices.Contains(ids, a.NetworkInsightsAnalysisID) {
			continue
		}

		if pathID != "" && a.NetworkInsightsPathID != pathID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsAnalysisID < result[j].NetworkInsightsAnalysisID
	})

	return result
}

func (b *InMemoryBackend) CreateNetworkInsightsAccessScope() (*NetworkInsightsAccessScope, error) {
	b.mu.Lock("CreateNetworkInsightsAccessScope")
	defer b.mu.Unlock()

	id := "nias-" + uuid.New().String()[:8]
	s := &NetworkInsightsAccessScope{
		NetworkInsightsAccessScopeID:  id,
		NetworkInsightsAccessScopeArn: "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":network-insights-access-scope/" + id,
	}
	b.networkInsightsAccessScopes.Put(s)

	cp := *s

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAccessScope(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAccessScope")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopes.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeNF, id)
	}
	b.networkInsightsAccessScopes.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAccessScopes(ids []string) []*NetworkInsightsAccessScope {
	b.mu.RLock("DescribeNetworkInsightsAccessScopes")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAccessScope

	for _, s := range b.networkInsightsAccessScopes.All() {
		if len(ids) > 0 && !slices.Contains(ids, s.NetworkInsightsAccessScopeID) {
			continue
		}

		cp := *s
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsAccessScopeID < result[j].NetworkInsightsAccessScopeID
	})

	return result
}

func (b *InMemoryBackend) StartNetworkInsightsAccessScopeAnalysis(
	scopeID string,
) (*NetworkInsightsAccessScopeAnalysis, error) {
	b.mu.Lock("StartNetworkInsightsAccessScopeAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopes.Get(scopeID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeNF, scopeID)
	}

	id := "niasa-" + uuid.New().String()[:8]
	a := &NetworkInsightsAccessScopeAnalysis{
		NetworkInsightsAccessScopeAnalysisID: id,
		NetworkInsightsAccessScopeID:         scopeID,
		Status:                               stateAnalysisSucceeded,
		AnalyzedEniCount:                     0,
	}
	b.networkInsightsAccessScopeAnalyses.Put(a)

	cp := *a

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAccessScopeAnalysis(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAccessScopeAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopeAnalyses.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeAnaNF, id)
	}
	b.networkInsightsAccessScopeAnalyses.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAccessScopeAnalyses(
	ids []string,
	scopeID string,
) []*NetworkInsightsAccessScopeAnalysis {
	b.mu.RLock("DescribeNetworkInsightsAccessScopeAnalyses")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAccessScopeAnalysis

	for _, a := range b.networkInsightsAccessScopeAnalyses.All() {
		if len(ids) > 0 && !slices.Contains(ids, a.NetworkInsightsAccessScopeAnalysisID) {
			continue
		}

		if scopeID != "" && a.NetworkInsightsAccessScopeID != scopeID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsAccessScopeAnalysisID < result[j].NetworkInsightsAccessScopeAnalysisID
	})

	return result
}

// ---- BYOIP backend methods ----

// EnableReachabilityAnalyzerOrganizationSharing sets the per-account flag
// enabling Reachability Analyzer resource sharing across an organization.
func (b *InMemoryBackend) EnableReachabilityAnalyzerOrganizationSharing() bool {
	b.mu.Lock("EnableReachabilityAnalyzerOrganizationSharing")
	defer b.mu.Unlock()

	b.reachabilityAnalyzerOrgSharing = true

	return true
}
