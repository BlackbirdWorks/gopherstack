package detective

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

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
	b.mu.Lock("UpdateDatasourcePackages")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	if b.datasources[graphARN] == nil {
		b.datasources[graphARN] = make(map[string]string)
	}

	for _, pkg := range packages {
		b.datasources[graphARN][pkg] = datasourceIngestStateStarted
	}

	return nil
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

		pkgStates := make(map[string]string)
		if pkgMap := b.datasources[graphARN]; pkgMap != nil {
			maps.Copy(pkgStates, pkgMap)
		}

		results = append(results, MembershipDatasources{
			AccountID:                     id,
			GraphARN:                      graphARN,
			DatasourcePackageIngestStates: pkgStates,
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

		pkgStates := make(map[string]string)
		if pkgMap := b.datasources[graphARN]; pkgMap != nil {
			maps.Copy(pkgStates, pkgMap)
		}

		results = append(results, MembershipDatasources{
			AccountID:                     b.accountID,
			GraphARN:                      graphARN,
			DatasourcePackageIngestStates: pkgStates,
		})
	}

	return results, unprocessed, nil
}
