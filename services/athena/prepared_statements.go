package athena

import (
	"fmt"
	"sort"
	"time"
)

// preparedStatementKey returns the map key for a prepared statement.
func preparedStatementKey(workGroup, name string) string {
	return workGroup + "/" + name
}

// maxListPreparedStatements is the AWS-documented maximum page size for ListPreparedStatements.
const maxListPreparedStatements = 256

// CreatePreparedStatement creates a new prepared statement in a workgroup.
func (b *InMemoryBackend) CreatePreparedStatement(
	name, description, workGroup, queryStatement string,
) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: StatementName is required", ErrValidation)
	case workGroup == "":
		return fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case queryStatement == "":
		return fmt.Errorf("%w: QueryStatement is required", ErrValidation)
	}

	b.mu.Lock("CreatePreparedStatement")
	defer b.mu.Unlock()

	key := preparedStatementKey(workGroup, name)
	if b.preparedStatements.Has(key) {
		return fmt.Errorf(
			"%w: prepared statement %q already exists in workgroup %q",
			ErrAlreadyExists,
			name,
			workGroup,
		)
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.preparedStatements.Put(&PreparedStatement{
		StatementName:    name,
		WorkGroupName:    workGroup,
		QueryStatement:   queryStatement,
		Description:      description,
		LastModifiedTime: now,
	})

	return nil
}

// GetPreparedStatement retrieves a prepared statement by name and workgroup.
func (b *InMemoryBackend) GetPreparedStatement(name, workGroup string) (*PreparedStatement, error) {
	b.mu.RLock("GetPreparedStatement")
	defer b.mu.RUnlock()

	key := preparedStatementKey(workGroup, name)
	ps, ok := b.preparedStatements.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: prepared statement %q not found in workgroup %q",
			ErrResourceNotFound,
			name,
			workGroup,
		)
	}

	cp := *ps

	return &cp, nil
}

// ListPreparedStatements returns summary views of prepared statements in a workgroup, sorted by name,
// with optional NextToken/MaxResults pagination.
func (b *InMemoryBackend) ListPreparedStatements(
	workGroup, nextToken string,
	maxResults int,
) ([]PreparedStatementSummary, string, error) {
	b.mu.RLock("ListPreparedStatements")
	defer b.mu.RUnlock()

	matches := b.preparedStatementsByWorkGroup.Get(workGroup)
	result := make([]PreparedStatementSummary, 0, len(matches))

	for _, ps := range matches {
		result = append(result, PreparedStatementSummary{
			StatementName:    ps.StatementName,
			LastModifiedTime: ps.LastModifiedTime,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StatementName < result[j].StatementName
	})

	limit := maxListPreparedStatements
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := paginationStart(len(result), nextToken, func(i int) string { return result[i].StatementName })
	result = result[start:]

	outToken := ""
	if len(result) > limit {
		outToken = result[limit].StatementName
		result = result[:limit]
	}

	return result, outToken, nil
}

// BatchGetPreparedStatement retrieves multiple prepared statements by name within a workgroup.
func (b *InMemoryBackend) BatchGetPreparedStatement(
	workGroup string,
	names []string,
) ([]PreparedStatement, []UnprocessedPreparedStatementName) {
	b.mu.RLock("BatchGetPreparedStatement")
	defer b.mu.RUnlock()

	found := make([]PreparedStatement, 0, len(names))
	unprocessed := make([]UnprocessedPreparedStatementName, 0, len(names))

	for _, name := range names {
		key := preparedStatementKey(workGroup, name)
		ps, ok := b.preparedStatements.Get(key)
		if ok {
			found = append(found, *ps)
		} else {
			unprocessed = append(unprocessed, UnprocessedPreparedStatementName{
				StatementName: name,
				ErrorMessage:  fmt.Sprintf("prepared statement %q not found in workgroup %q", name, workGroup),
			})
		}
	}

	return found, unprocessed
}

// DeletePreparedStatement removes a prepared statement by name and workgroup.
func (b *InMemoryBackend) DeletePreparedStatement(name, workGroup string) error {
	b.mu.Lock("DeletePreparedStatement")
	defer b.mu.Unlock()

	key := preparedStatementKey(workGroup, name)
	if !b.preparedStatements.Has(key) {
		return fmt.Errorf(
			"%w: prepared statement %q not found in workgroup %q",
			ErrResourceNotFound,
			name,
			workGroup,
		)
	}

	b.preparedStatements.Delete(key)

	return nil
}

// UpdatePreparedStatement updates an existing prepared statement.
func (b *InMemoryBackend) UpdatePreparedStatement(name, workGroup, queryStatement, description string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: StatementName is required", ErrValidation)
	case workGroup == "":
		return fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case queryStatement == "":
		return fmt.Errorf("%w: QueryStatement is required", ErrValidation)
	}

	b.mu.Lock("UpdatePreparedStatement")
	defer b.mu.Unlock()

	key := preparedStatementKey(workGroup, name)

	ps, ok := b.preparedStatements.Get(key)
	if !ok {
		return fmt.Errorf("%w: prepared statement %q not found in workgroup %q", ErrResourceNotFound, name, workGroup)
	}

	ps.QueryStatement = queryStatement

	if description != "" {
		ps.Description = description
	}

	ps.LastModifiedTime = nowSeconds()

	return nil
}
