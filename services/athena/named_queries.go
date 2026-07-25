package athena

import (
	"fmt"
	"sort"
)

// CreateNamedQuery creates a new named query and returns its ID.
func (b *InMemoryBackend) CreateNamedQuery(
	name, description, database, queryString, workGroup string,
) (string, error) {
	switch {
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	case database == "":
		return "", fmt.Errorf("%w: Database is required", ErrValidation)
	case queryString == "":
		return "", fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	if workGroup == "" {
		workGroup = defaultWorkGroup
	}

	b.mu.Lock("CreateNamedQuery")
	defer b.mu.Unlock()

	if !b.workGroups.Has(workGroup) {
		return "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	id := randomID()
	b.namedQueries.Put(&NamedQuery{
		NamedQueryID: id,
		Name:         name,
		Description:  description,
		Database:     database,
		QueryString:  queryString,
		WorkGroup:    workGroup,
	})

	return id, nil
}

// GetNamedQuery retrieves a named query by ID.
func (b *InMemoryBackend) GetNamedQuery(id string) (*NamedQuery, error) {
	b.mu.RLock("GetNamedQuery")
	defer b.mu.RUnlock()

	q, ok := b.namedQueries.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	cp := *q

	return &cp, nil
}

// ListNamedQueries returns named query IDs, optionally filtered by workgroup, with pagination.
func (b *InMemoryBackend) ListNamedQueries(
	workGroup, nextToken string,
	maxResults int,
) ([]string, string, error) {
	b.mu.RLock("ListNamedQueries")
	defer b.mu.RUnlock()

	var matches []*NamedQuery
	if workGroup == "" {
		matches = b.namedQueries.All()
	} else {
		matches = b.namedQueriesByWorkGroup.Get(workGroup)
	}

	ids := make([]string, 0, len(matches))
	for _, q := range matches {
		ids = append(ids, q.NamedQueryID)
	}

	sort.Strings(ids)

	const defaultMaxResults = 50
	limit := defaultMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := paginationStart(len(ids), nextToken, func(i int) string { return ids[i] })
	ids = ids[start:]

	outToken := ""
	if len(ids) > limit {
		outToken = ids[limit]
		ids = ids[:limit]
	}

	return ids, outToken, nil
}

// BatchGetNamedQuery retrieves multiple named queries by ID.
func (b *InMemoryBackend) BatchGetNamedQuery(
	ids []string,
) ([]NamedQuery, []UnprocessedNamedQueryID) {
	b.mu.RLock("BatchGetNamedQuery")
	defer b.mu.RUnlock()

	found := make([]NamedQuery, 0, len(ids))
	unprocessed := make([]UnprocessedNamedQueryID, 0, len(ids))

	for _, id := range ids {
		q, ok := b.namedQueries.Get(id)
		if ok {
			found = append(found, *q)
		} else {
			unprocessed = append(unprocessed, UnprocessedNamedQueryID{
				NamedQueryID: id,
				ErrorCode:    "InvalidRequestException",
				ErrorMessage: fmt.Sprintf("named query %q not found", id),
			})
		}
	}

	return found, unprocessed
}

// DeleteNamedQuery removes a named query by ID.
func (b *InMemoryBackend) DeleteNamedQuery(id string) error {
	b.mu.Lock("DeleteNamedQuery")
	defer b.mu.Unlock()

	if !b.namedQueries.Has(id) {
		return fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	b.namedQueries.Delete(id)

	return nil
}

// UpdateNamedQuery updates an existing named query's name, description, or query string.
func (b *InMemoryBackend) UpdateNamedQuery(id, name, description, queryString string) error {
	if id == "" {
		return fmt.Errorf("%w: NamedQueryId is required", ErrValidation)
	}

	b.mu.Lock("UpdateNamedQuery")
	defer b.mu.Unlock()

	q, ok := b.namedQueries.Get(id)
	if !ok {
		return fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	if name != "" {
		q.Name = name
	}

	if description != "" {
		q.Description = description
	}

	if queryString != "" {
		q.QueryString = queryString
	}

	return nil
}
