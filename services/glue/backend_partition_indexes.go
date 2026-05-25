package glue

import (
	"fmt"
	"sort"
	"strings"
)

// PartitionIndex describes an index over table partition keys.
type PartitionIndex struct {
	IndexName   string   `json:"IndexName"`
	IndexStatus string   `json:"IndexStatus"`
	Keys        []string `json:"Keys"`
}

func partitionIndexKey(dbName, tableName, indexName string) string {
	return dbName + "|" + tableName + "|" + indexName
}

func clonePartitionIndex(index *PartitionIndex) *PartitionIndex {
	cp := *index
	cp.Keys = append([]string(nil), index.Keys...)

	return &cp
}

// CreatePartitionIndex creates an index for an existing table's partition keys.
func (b *InMemoryBackend) CreatePartitionIndex(dbName, tableName string, input PartitionIndex) error {
	b.mu.Lock("CreatePartitionIndex")
	defer b.mu.Unlock()

	table, ok := b.tables[tableKey(dbName, tableName)]
	if !ok {
		return fmt.Errorf("table %s.%s not found: %w", dbName, tableName, ErrNotFound)
	}
	if err := validatePartitionIndex(table, input); err != nil {
		return err
	}

	key := partitionIndexKey(dbName, tableName, input.IndexName)
	if _, exists := b.partitionIndexes[key]; exists {
		return fmt.Errorf("partition index %q already exists: %w", input.IndexName, ErrAlreadyExists)
	}

	input.IndexStatus = stateActive
	b.partitionIndexes[key] = clonePartitionIndex(&input)

	return nil
}

func validatePartitionIndex(table *Table, input PartitionIndex) error {
	if input.IndexName == "" || len(input.Keys) == 0 {
		return fmt.Errorf("IndexName and Keys are required: %w", ErrValidation)
	}

	partitionKeys := make(map[string]struct{}, len(table.PartitionKeys))
	for _, key := range table.PartitionKeys {
		partitionKeys[key.Name] = struct{}{}
	}

	seen := make(map[string]struct{}, len(input.Keys))
	for _, key := range input.Keys {
		if _, ok := partitionKeys[key]; !ok {
			return fmt.Errorf("index key %q is not a partition key: %w", key, ErrValidation)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("index key %q appears more than once: %w", key, ErrValidation)
		}
		seen[key] = struct{}{}
	}

	return nil
}

// DeletePartitionIndex deletes an index from an existing table.
func (b *InMemoryBackend) DeletePartitionIndex(dbName, tableName, indexName string) error {
	b.mu.Lock("DeletePartitionIndex")
	defer b.mu.Unlock()

	key := partitionIndexKey(dbName, tableName, indexName)
	if _, ok := b.partitionIndexes[key]; !ok {
		return fmt.Errorf("partition index %q not found: %w", indexName, ErrNotFound)
	}

	delete(b.partitionIndexes, key)

	return nil
}

// GetPartitionIndexes lists partition indexes for an existing table.
func (b *InMemoryBackend) GetPartitionIndexes(dbName, tableName string) ([]*PartitionIndex, error) {
	b.mu.RLock("GetPartitionIndexes")
	defer b.mu.RUnlock()

	if _, ok := b.tables[tableKey(dbName, tableName)]; !ok {
		return nil, fmt.Errorf("table %s.%s not found: %w", dbName, tableName, ErrNotFound)
	}

	prefix := dbName + "|" + tableName + "|"
	indexes := make([]*PartitionIndex, 0)
	for key, index := range b.partitionIndexes {
		if strings.HasPrefix(key, prefix) {
			indexes = append(indexes, clonePartitionIndex(index))
		}
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].IndexName < indexes[j].IndexName
	})

	return indexes, nil
}
