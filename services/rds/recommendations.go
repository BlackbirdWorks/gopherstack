package rds

import (
	"fmt"
	"net/url"
	"slices"
)

// AddDBRecommendation adds a recommendation to the backend. Used by tests and internal
// workflows to seed recommendations so that ModifyDBRecommendation and
// DescribeDBRecommendations can exercise real state transitions.
func (b *InMemoryBackend) AddDBRecommendation(rec DBRecommendation) {
	b.mu.Lock("AddDBRecommendation")
	defer b.mu.Unlock()

	cp := rec
	b.recommendations.Put(&cp)
}

// ModifyDBRecommendation modifies the status of a DB recommendation.
func (b *InMemoryBackend) ModifyDBRecommendation(recID, status string) (*DBRecommendation, error) {
	b.mu.Lock("ModifyDBRecommendation")
	defer b.mu.Unlock()
	rec, ok := b.recommendations.Get(recID)
	if !ok {
		return nil, fmt.Errorf("%w: recommendation %s not found", ErrInvalidParameter, recID)
	}
	rec.Status = status
	cp := *rec

	return &cp, nil
}

// DescribeDBRecommendations returns DB recommendations filtered by optional parameters.
func (b *InMemoryBackend) DescribeDBRecommendations(recID, status string) []DBRecommendation {
	b.mu.RLock("DescribeDBRecommendations")
	defer b.mu.RUnlock()
	result := make([]DBRecommendation, 0, b.recommendations.Len())
	for _, rec := range b.recommendations.All() {
		if recID != "" && rec.RecommendationID != recID {
			continue
		}
		if status != "" && rec.Status != status {
			continue
		}
		result = append(result, *rec)
	}
	slices.SortFunc(result, func(a, b DBRecommendation) int {
		if a.RecommendationID < b.RecommendationID {
			return -1
		}
		if a.RecommendationID > b.RecommendationID {
			return 1
		}

		return 0
	})

	return result
}

// isKnownDBRecommendationFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for DescribeDBRecommendations
// (rds@v1.124.1 api_op_DescribeDBRecommendations.go:33-87).
// "dbi-resource-id", "cluster-resource-id", "pg-arn", and "cluster-pg-arn"
// are accepted (to avoid rejecting an otherwise-valid client request) but
// DBRecommendation carries only a generic ResourceARN, not fields typed to
// any one of those four resource kinds, so they are not implemented as match
// predicates, matching the existing DescribeDBInstances "domain" precedent
// (db_instances.go).
func isKnownDBRecommendationFilterName(name string) bool {
	switch name {
	case filterNameRecommendationID, filterNameStatus, filterNameSeverity, filterNameTypeID,
		filterNameDbiResourceID, filterNameClusterResourceID, filterNamePgArn, filterNameClusterPgArn:
		return true
	default:
		return false
	}
}

// applyDBRecommendationFilters narrows recs per the AWS
// DescribeDBRecommendations Filters contract: each filter ANDs together, and
// a filter's Values list is OR-matched against the corresponding
// recommendation field. An unrecognized filter name returns
// InvalidParameterValue, matching real AWS.
func applyDBRecommendationFilters(vals url.Values, recs []DBRecommendation) ([]DBRecommendation, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return recs, nil
	}

	for name := range filters {
		if !isKnownDBRecommendationFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]DBRecommendation, 0, len(recs))
	for _, rec := range recs {
		if matchesAllDBRecommendationFilters(rec, filters) {
			filtered = append(filtered, rec)
		}
	}

	return filtered, nil
}

func matchesAllDBRecommendationFilters(rec DBRecommendation, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameRecommendationID:
			if !slices.Contains(values, rec.RecommendationID) {
				return false
			}
		case filterNameStatus:
			if !slices.Contains(values, rec.Status) {
				return false
			}
		case filterNameSeverity:
			if !slices.Contains(values, rec.Severity) {
				return false
			}
		case filterNameTypeID:
			if !slices.Contains(values, rec.TypeID) {
				return false
			}
		case filterNameDbiResourceID, filterNameClusterResourceID, filterNamePgArn, filterNameClusterPgArn:
			// Not modeled; accept unconditionally.
		}
	}

	return true
}
