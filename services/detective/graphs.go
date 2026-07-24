package detective

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// createGraphLocked creates and stores a new behavior graph. Callers must
// hold b.mu for writing. Shared by CreateGraph and EnableOrganizationAdminAccount,
// which both need to materialize a graph the first time an account uses Detective.
func (b *InMemoryBackend) createGraphLocked(tags map[string]string) *storedGraph {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	arn := b.graphARN(id)
	now := time.Now().UTC()

	graphTags := make(map[string]string)
	maps.Copy(graphTags, tags)

	g := &storedGraph{
		Arn:         arn,
		CreatedTime: now,
		Tags:        graphTags,
	}
	b.graphs.Put(g)

	if len(graphTags) > 0 {
		b.tags[arn] = make(map[string]string)
		maps.Copy(b.tags[arn], graphTags)
	}

	return g
}

// CreateGraph creates a new behavior graph. Returns existing one if already created (idempotent).
func (b *InMemoryBackend) CreateGraph(tags map[string]string) (*Graph, error) {
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateGraph")
	defer b.mu.Unlock()

	if existing := b.graphs.All(); len(existing) > 0 {
		cp := existing[0].toGraph()

		return &cp, nil
	}

	cp := b.createGraphLocked(tags).toGraph()

	return &cp, nil
}

// deleteGraphLocked deletes graphARN and cascades cleanup of every map keyed
// by it (members, investigations, tags, datasources, orgConfigs). Callers
// must hold b.mu for writing. Shared by DeleteGraph and
// DisableOrganizationAdminAccount, which both destroy a behavior graph.
// Returns false if graphARN does not exist (no-op).
func (b *InMemoryBackend) deleteGraphLocked(graphARN string) bool {
	if !b.graphs.Has(graphARN) {
		return false
	}

	b.graphs.Delete(graphARN)

	for _, m := range slices.Clone(b.membersByGraph.Get(graphARN)) {
		b.members.Delete(memberKey(m.GraphARN, m.AccountID))
	}

	for _, inv := range slices.Clone(b.investigationsByGraph.Get(graphARN)) {
		b.investigations.Delete(investigationKey(inv.GraphARN, inv.InvestigationID))
	}

	delete(b.tags, graphARN)
	delete(b.datasources, graphARN)
	delete(b.orgConfigs, graphARN)

	return true
}

// DeleteGraph deletes a behavior graph.
func (b *InMemoryBackend) DeleteGraph(graphARN string) error {
	b.mu.Lock("DeleteGraph")
	defer b.mu.Unlock()

	if !b.deleteGraphLocked(graphARN) {
		return ErrGraphNotFound
	}

	return nil
}

// ListGraphs returns behavior graphs for the admin account.
func (b *InMemoryBackend) ListGraphs(maxResults int32, nextToken string) ([]*Graph, string, error) {
	b.mu.RLock("ListGraphs")
	defer b.mu.RUnlock()

	graphs := b.graphs.Snapshot()

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(graphs) {
		start = len(graphs)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxGraphsPerPage {
		limit = maxGraphsPerPage
	}

	end := min(start+limit, len(graphs))

	result := make([]*Graph, 0, end-start)
	for _, g := range graphs[start:end] {
		cp := g.toGraph()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(graphs) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}
