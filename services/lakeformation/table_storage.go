package lakeformation

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// tableKey generates a unique key for a table.
func tableKey(catalogID, db, table string) string {
	return catalogID + "|" + db + "|" + table
}

// GetTableObjects returns a paginated list of governed table objects.
func (b *InMemoryBackend) GetTableObjects(
	catalogID, databaseName, tableName, _ string,
	maxResults int, nextToken string,
) ([]PartitionedTableObjectsList, string) {
	b.mu.RLock("GetTableObjects")
	defer b.mu.RUnlock()
	key := tableKey(catalogID, databaseName, tableName)
	objects := b.tableObjects[key]

	return paginate(objects, maxResults, nextToken, defaultMaxResults)
}

// UpdateTableObjects validates the transaction and records the write operations.
func (b *InMemoryBackend) UpdateTableObjects(
	catalogID, databaseName, tableName, transactionID string,
	writes []WriteOperation,
) error {
	if strings.TrimSpace(transactionID) == "" {
		return nil
	}
	b.mu.Lock("UpdateTableObjects")
	defer b.mu.Unlock()
	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if info.Type == transactionTypeReadOnly {
		return fmt.Errorf("cannot write to READ_ONLY transaction: %w", ErrValidation)
	}

	key := tableKey(catalogID, databaseName, tableName)

	// Create a new partitioned list to hold the added objects
	list := PartitionedTableObjectsList{
		Objects: make([]TableObject, 0),
	}

	for _, w := range writes {
		if w.AddObject != nil {
			list.Objects = append(list.Objects, *w.AddObject)
		}
	}

	if len(list.Objects) > 0 {
		b.tableObjects[key] = append(b.tableObjects[key], list)
	}

	return nil
}

// tableStorageKey returns a composite key for table storage optimizer lookups.
func tableStorageKey(catalogID, databaseName, tableName string) string {
	return catalogID + "|" + databaseName + "|" + tableName
}

// ListTableStorageOptimizers returns the storage optimizers for a table, filtered by type if specified.
func (b *InMemoryBackend) ListTableStorageOptimizers(
	catalogID, databaseName, tableName, storageOptimizerType string,
) []StorageOptimizer {
	b.mu.RLock("ListTableStorageOptimizers")
	defer b.mu.RUnlock()
	key := tableStorageKey(catalogID, databaseName, tableName)
	opts := b.tableStorageOptimizers[key]
	result := make([]StorageOptimizer, 0, len(opts))

	for _, o := range opts {
		if storageOptimizerType == "" || o.StorageOptimizerType == storageOptimizerType {
			result = append(result, o)
		}
	}

	return result
}

// UpdateTableStorageOptimizer replaces the storage optimizer config for a table.
func (b *InMemoryBackend) UpdateTableStorageOptimizer(
	catalogID, databaseName, tableName string, config map[string]map[string]string,
) string {
	b.mu.Lock("UpdateTableStorageOptimizer")
	defer b.mu.Unlock()
	key := tableStorageKey(catalogID, databaseName, tableName)
	opts := make([]StorageOptimizer, 0, len(config))
	for optimizerType, cfg := range config {
		opts = append(opts, StorageOptimizer{StorageOptimizerType: optimizerType, Config: cfg})
	}
	b.tableStorageOptimizers[key] = opts

	return "Optimizer updated successfully"
}
