package awsconfig

import (
	"fmt"

	"github.com/google/uuid"
)

// storedQueryArn returns the ARN for the stored query named name.
func (b *InMemoryBackend) storedQueryArn(name string) string {
	return fmt.Sprintf("arn:aws:config:%s:%s:stored-query/%s", b.region, b.accountID, name)
}

// PutStoredQuery creates or updates a stored query by name. A pre-existing
// query keeps its QueryId across updates, matching real AWS Config Put
// (create-or-update) semantics.
func (b *InMemoryBackend) PutStoredQuery(name, description, expression string, tags []Tag) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: QueryName is required", ErrValidation)
	}

	b.mu.Lock("PutStoredQuery")
	defer b.mu.Unlock()

	queryID := uuid.NewString()
	if existing, ok := b.storedQueries.Get(name); ok {
		queryID = existing.QueryID
	}

	arn := b.storedQueryArn(name)

	b.storedQueries.Put(&StoredQuery{
		QueryName:   name,
		QueryID:     queryID,
		QueryArn:    arn,
		Description: description,
		Expression:  expression,
	})
	b.setResourceTagsLocked(arn, tags)

	return arn, nil
}

// ListStoredQueries returns metadata for all stored queries.
func (b *InMemoryBackend) ListStoredQueries() []StoredQueryMetadata {
	b.mu.RLock("ListStoredQueries")
	defer b.mu.RUnlock()

	all := b.storedQueries.All()
	out := make([]StoredQueryMetadata, 0, len(all))

	for _, q := range all {
		out = append(out, StoredQueryMetadata{
			QueryArn:    q.QueryArn,
			QueryID:     q.QueryID,
			QueryName:   q.QueryName,
			Description: q.Description,
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
