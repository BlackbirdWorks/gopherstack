package detective

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// validDatasourcePackages is the real DatasourcePackage enum (botocore
// detective/2018-10-26 service-2.json shapes.DatasourcePackage) --
// UpdateDatasourcePackages documents ValidationException in its error set,
// so a package name outside this set must be rejected, not silently
// persisted as if it were a real package.
var validDatasourcePackages = map[string]bool{ //nolint:gochecknoglobals // static enum lookup table, never mutated.
	"DETECTIVE_CORE":           true,
	"EKS_AUDIT":                true,
	"ASFF_SECURITYHUB_FINDING": true,
}

// ListDatasourcePackages returns datasource package ingest details for a graph.
func (b *InMemoryBackend) ListDatasourcePackages(
	graphARN string,
	maxResults int32,
	nextToken string,
) (map[string]DatasourcePackageIngestDetail, string, error) {
	b.mu.RLock("ListDatasourcePackages")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, "", ErrGraphNotFound
	}

	pkgMap := b.datasources[graphARN]
	keys := collections.SortedKeys(pkgMap)

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(keys) {
		start = len(keys)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxDatasourcesPerPage {
		limit = maxDatasourcesPerPage
	}

	end := min(start+limit, len(keys))

	result := make(map[string]DatasourcePackageIngestDetail, end-start)
	for _, k := range keys[start:end] {
		result[k] = DatasourcePackageIngestDetail{IngestState: pkgMap[k]}
	}

	var outToken string
	if end < len(keys) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// UpdateDatasourcePackages enables datasource packages on a graph.
func (b *InMemoryBackend) UpdateDatasourcePackages(graphARN string, packages []string) error {
	for _, pkg := range packages {
		if !validDatasourcePackages[pkg] {
			return fmt.Errorf("%w: invalid DatasourcePackage %q", ErrValidation, pkg)
		}
	}

	b.mu.Lock("UpdateDatasourcePackages")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	if b.datasources[graphARN] == nil {
		b.datasources[graphARN] = make(map[string]string)
	}

	if b.datasourceChangedAt[graphARN] == nil {
		b.datasourceChangedAt[graphARN] = make(map[string]time.Time)
	}

	now := time.Now().UTC()
	for _, pkg := range packages {
		b.datasources[graphARN][pkg] = datasourceIngestStateStarted
		b.datasourceChangedAt[graphARN][pkg] = now
	}

	return nil
}

// ingestHistoryLocked builds the DatasourcePackageIngestHistory shape (package
// -> current ingest state -> the time that state began) for graphARN. Callers
// must hold b.mu (either lock).
func (b *InMemoryBackend) ingestHistoryLocked(graphARN string) map[string]map[string]time.Time {
	history := make(map[string]map[string]time.Time)

	for pkg, state := range b.datasources[graphARN] {
		changedAt := b.datasourceChangedAt[graphARN][pkg]
		history[pkg] = map[string]time.Time{state: changedAt}
	}

	return history
}

// BatchGetGraphMemberDatasources returns datasource package info for member accounts of a graph.
func (b *InMemoryBackend) BatchGetGraphMemberDatasources(
	graphARN string,
	accountIDs []string,
) ([]MembershipDatasources, []UnprocessedAccount, error) {
	b.mu.RLock("BatchGetGraphMemberDatasources")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	var results []MembershipDatasources
	var unprocessed []UnprocessedAccount

	for _, id := range accountIDs {
		if !b.members.Has(memberKey(graphARN, id)) {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    reasonMemberNotFoundInGraph,
			})

			continue
		}

		results = append(results, MembershipDatasources{
			AccountID:     id,
			GraphARN:      graphARN,
			IngestHistory: b.ingestHistoryLocked(graphARN),
		})
	}

	return results, unprocessed, nil
}

// BatchGetMembershipDatasources returns datasource history for the account across graphs.
func (b *InMemoryBackend) BatchGetMembershipDatasources(
	graphARNs []string,
) ([]MembershipDatasources, []UnprocessedGraph, error) {
	b.mu.RLock("BatchGetMembershipDatasources")
	defer b.mu.RUnlock()

	var results []MembershipDatasources
	var unprocessed []UnprocessedGraph

	for _, graphARN := range graphARNs {
		if !b.graphs.Has(graphARN) {
			unprocessed = append(unprocessed, UnprocessedGraph{
				GraphArn: graphARN,
				Reason:   "Graph not found",
			})

			continue
		}

		results = append(results, MembershipDatasources{
			AccountID:     b.accountID,
			GraphARN:      graphARN,
			IngestHistory: b.ingestHistoryLocked(graphARN),
		})
	}

	return results, unprocessed, nil
}
