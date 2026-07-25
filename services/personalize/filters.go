package personalize

import (
	"fmt"
	"time"
)

// --- Filter ---

// CreateFilter creates a new filter.
func (b *InMemoryBackend) CreateFilter(
	name, datasetGroupArn, filterExpression string,
	tags map[string]string,
) (*Filter, error) {
	b.mu.Lock("CreateFilter")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.filters.Has(name) {
		return nil, fmt.Errorf("%w: filter %q already exists", ErrAlreadyExists, name)
	}
	if b.findDatasetGroup(datasetGroupArn) == nil {
		return nil, fmt.Errorf("%w: dataset group %q not found", ErrNotFound, datasetGroupArn)
	}

	now := time.Now().UTC()
	f := &Filter{
		FilterArn:           b.personalizeARN("filter", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		FilterExpression:    filterExpression,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.filters.Put(f)
	if len(tags) > 0 {
		b.tags[f.FilterArn] = copyStringMap(tags)
	}

	return f, nil
}

// DescribeFilter returns a filter by name or ARN.
func (b *InMemoryBackend) DescribeFilter(nameOrArn string) (*Filter, error) {
	b.mu.RLock("DescribeFilter")
	defer b.mu.RUnlock()

	if f := b.findFilter(nameOrArn); f != nil {
		return f, nil
	}

	return nil, fmt.Errorf("%w: filter %q not found", ErrNotFound, nameOrArn)
}

// DeleteFilter removes a filter.
func (b *InMemoryBackend) DeleteFilter(nameOrArn string) error {
	b.mu.Lock("DeleteFilter")
	defer b.mu.Unlock()

	f := b.findFilter(nameOrArn)
	if f == nil {
		return fmt.Errorf("%w: filter %q not found", ErrNotFound, nameOrArn)
	}
	b.filters.Delete(f.Name)
	delete(b.tags, f.FilterArn)

	return nil
}

// ListFilters returns filters, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListFilters(datasetGroupArn string, maxResults int, nextToken string) ([]*Filter, string) {
	b.mu.RLock("ListFilters")
	defer b.mu.RUnlock()

	all := b.filters.Snapshot()
	filtered := make([]*Filter, 0, len(all))
	for _, f := range all {
		if datasetGroupArn == "" || f.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, f)
		}
	}

	return paginateItems(filtered, filterKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findFilter(nameOrArn string) *Filter {
	if f, ok := b.filters.Get(nameOrArn); ok {
		return f
	}
	for _, f := range b.filters.All() {
		if f.FilterArn == nameOrArn {
			return f
		}
	}

	return nil
}
