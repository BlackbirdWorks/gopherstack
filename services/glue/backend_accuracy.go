package glue

import (
	"maps"
	"strings"
	"time"
)

// GetPartition retrieves a single partition by its values.
func (b *InMemoryBackend) GetPartition(dbName, tableName string, values []string) (*Partition, error) {
	b.mu.RLock("GetPartition")
	defer b.mu.RUnlock()

	key := partitionKey(dbName, tableName, values)
	p, ok := b.partitions[key]
	if !ok {
		return nil, ErrNotFound
	}

	cp := clonePartition(p)

	return cp, nil
}

// GetPartitions returns all partitions for a table, sorted by key.
func (b *InMemoryBackend) GetPartitions(dbName, tableName string) ([]*Partition, error) {
	b.mu.RLock("GetPartitions")
	defer b.mu.RUnlock()

	if _, ok := b.tables[tableKey(dbName, tableName)]; !ok {
		return nil, ErrNotFound
	}

	prefix := dbName + "|" + tableName + "|"
	out := make([]*Partition, 0)

	for _, k := range sortedKeys(b.partitions) {
		if strings.HasPrefix(k, prefix) {
			out = append(out, clonePartition(b.partitions[k]))
		}
	}

	return out, nil
}

// UpdatePartition updates an existing partition's storage descriptor and optionally renames it.
func (b *InMemoryBackend) UpdatePartition(
	dbName, tableName string,
	partitionValues []string,
	input PartitionInput,
) error {
	b.mu.Lock("UpdatePartition")
	defer b.mu.Unlock()

	return b.updatePartitionLocked(dbName, tableName, partitionValues, input)
}

// updatePartitionLocked performs the update while the caller holds the write lock.
func (b *InMemoryBackend) updatePartitionLocked(
	dbName, tableName string,
	partitionValues []string,
	input PartitionInput,
) error {
	oldKey := partitionKey(dbName, tableName, partitionValues)
	p, ok := b.partitions[oldKey]
	if !ok {
		return ErrNotFound
	}

	if len(input.Values) > 0 && !stringSliceEqual(input.Values, partitionValues) {
		return b.renamePartitionLocked(p, dbName, tableName, oldKey, input)
	}

	p.StorageDescriptor = input.StorageDescriptor
	p.Parameters = maps.Clone(input.Parameters)

	return nil
}

// renamePartitionLocked moves a partition to a new key based on new values.
func (b *InMemoryBackend) renamePartitionLocked(
	p *Partition, dbName, tableName, oldKey string, input PartitionInput,
) error {
	newKey := partitionKey(dbName, tableName, input.Values)
	if _, exists := b.partitions[newKey]; exists {
		return ErrAlreadyExists
	}

	delete(b.partitions, oldKey)
	p.Values = append([]string(nil), input.Values...)
	p.StorageDescriptor = input.StorageDescriptor
	p.Parameters = maps.Clone(input.Parameters)
	b.partitions[newKey] = p

	return nil
}

// SearchTables returns tables matching a case-insensitive substring of the table name.
// An empty searchText returns all tables.
func (b *InMemoryBackend) SearchTables(searchText string) []*Table {
	b.mu.RLock("SearchTables")
	defer b.mu.RUnlock()

	lower := strings.ToLower(searchText)
	out := make([]*Table, 0)

	for _, k := range sortedKeys(b.tables) {
		t := b.tables[k]
		if lower == "" || strings.Contains(strings.ToLower(t.Name), lower) {
			out = append(out, cloneTable(t))
		}
	}

	return out
}

// UpdateConnection updates an existing connection's type and properties.
func (b *InMemoryBackend) UpdateConnection(name string, connType string, props map[string]string) error {
	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	c, ok := b.connections[name]
	if !ok {
		return ErrNotFound
	}

	c.ConnectionType = connType
	c.ConnectionProperties = maps.Clone(props)
	c.LastUpdatedTime = float64(time.Now().Unix())

	return nil
}

// clonePartition returns a deep copy of a Partition.
func clonePartition(p *Partition) *Partition {
	cp := *p
	cp.Values = append([]string(nil), p.Values...)
	cp.StorageDescriptor = cloneStorageDescriptor(p.StorageDescriptor)
	cp.Parameters = maps.Clone(p.Parameters)

	return &cp
}

// stringSliceEqual reports whether two string slices are equal.
func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
