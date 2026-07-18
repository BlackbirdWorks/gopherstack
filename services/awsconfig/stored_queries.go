package awsconfig

import "fmt"

// PutStoredQuery stores a query by name.
func (b *InMemoryBackend) PutStoredQuery(name string) error {
	if name == "" {
		return nil
	}

	b.mu.Lock("PutStoredQuery")
	defer b.mu.Unlock()

	b.storedQueries.Put(&StoredQuery{QueryName: name})

	return nil
}

// ListStoredQueries returns metadata for all stored queries.
func (b *InMemoryBackend) ListStoredQueries() []StoredQueryMetadata {
	b.mu.RLock("ListStoredQueries")
	defer b.mu.RUnlock()

	all := b.storedQueries.All()
	out := make([]StoredQueryMetadata, 0, len(all))

	for _, q := range all {
		arn := fmt.Sprintf(
			"arn:aws:config:%s:%s:stored-query/%s",
			b.region, b.accountID, q.QueryName,
		)
		out = append(out, StoredQueryMetadata{
			QueryArn:  arn,
			QueryID:   q.QueryID,
			QueryName: q.QueryName,
		})
	}

	return out
}

// GetStoredQuery returns the stored query with the given name, or nil if not found.
func (b *InMemoryBackend) GetStoredQuery(name string) *StoredQuery {
	b.mu.RLock("GetStoredQuery")
	defer b.mu.RUnlock()

	q, ok := b.storedQueries.Get(name)
	if !ok {
		return nil
	}

	cp := *q

	return &cp
}

// DeleteStoredQuery removes the stored query with the given name.
func (b *InMemoryBackend) DeleteStoredQuery(name string) error {
	b.mu.Lock("DeleteStoredQuery")
	defer b.mu.Unlock()

	b.storedQueries.Delete(name)

	return nil
}
