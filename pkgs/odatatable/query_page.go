package odatatable

import "sort"

// MaxPageSize is the most entities Azure Table Storage returns from one Query Entities call.
const MaxPageSize = 1000

// EntityPage is one page of query results. NextPartitionKey and NextRowKey are set when more entities match.
type EntityPage struct {
	NextPartitionKey string
	NextRowKey       string
	Entities         []EntityInfo
	HasMore          bool
}

// QueryEntitiesPage returns up to limit (capped at MaxPageSize; <= 0 means MaxPageSize) matching entities starting
// at the first key >= (startPK, startRK), plus the continuation key of the next match, if any.
func (b *InMemoryBackend) QueryEntitiesPage(
	table string, filter Node, limit int, startPK, startRK string,
) (EntityPage, error) {
	b.mu.RLock("QueryEntitiesPage")
	defer b.mu.RUnlock()

	t, ok := b.tables[table]
	if !ok {
		return EntityPage{}, ErrTableNotFound
	}

	if limit <= 0 || limit > MaxPageSize {
		limit = MaxPageSize
	}

	entities := t.sortedEntities()
	from := sort.Search(len(entities), func(i int) bool {
		e := entities[i]

		return e.PartitionKey > startPK || (e.PartitionKey == startPK && e.RowKey >= startRK)
	})

	page := EntityPage{Entities: make([]EntityInfo, 0, min(limit, defaultQueryCap))}

	for _, e := range entities[from:] {
		if filter != nil && !EvaluateFilter(filter, EntityInfo{
			PartitionKey: e.PartitionKey, RowKey: e.RowKey, Timestamp: e.Timestamp, Properties: e.Properties,
		}) {
			continue
		}

		if len(page.Entities) >= limit {
			page.HasMore, page.NextPartitionKey, page.NextRowKey = true, e.PartitionKey, e.RowKey

			break
		}

		page.Entities = append(page.Entities, b.info(e))
	}

	return page, nil
}
