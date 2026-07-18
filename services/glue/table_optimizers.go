package glue

import (
	"fmt"
	"time"
)

func (b *InMemoryBackend) tableOptimizerKey(dbName, tableName, optimizerType string) string {
	return dbName + "|" + tableName + "|" + optimizerType
}

func cloneTableOptimizer(to *TableOptimizer) *TableOptimizer {
	cp := *to
	if to.LastRun != nil {
		lastRun := *to.LastRun
		cp.LastRun = &lastRun
	}

	return &cp
}

// CreateTableOptimizer registers a table optimizer and, mirroring the automatic
// compaction run AWS kicks off shortly after an optimizer is enabled, seeds an
// initial completed run so ListTableOptimizerRuns has real history to return.
func (b *InMemoryBackend) CreateTableOptimizer(
	catalogID, dbName, tableName, optimizerType string,
	config TableOptimizerConfiguration,
) error {
	b.mu.Lock("CreateTableOptimizer")
	defer b.mu.Unlock()

	key := b.tableOptimizerKey(dbName, tableName, optimizerType)
	if b.tableOptimizers.Has(key) {
		return fmt.Errorf(
			"table optimizer type %q already exists for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrAlreadyExists,
		)
	}

	now := float64(time.Now().Unix())
	b.tableOptimizers.Put(&TableOptimizer{
		CatalogID:     catalogID,
		DatabaseName:  dbName,
		TableName:     tableName,
		Type:          optimizerType,
		Configuration: config,
		LastRun: &TableOptimizerRun{
			EventType: "completed",
			StartedAt: now,
			EndedAt:   now,
		},
	})

	return nil
}

func (b *InMemoryBackend) GetTableOptimizer(
	dbName, tableName, optimizerType string,
) (*TableOptimizer, error) {
	b.mu.RLock("GetTableOptimizer")
	defer b.mu.RUnlock()

	to, ok := b.tableOptimizers.Get(b.tableOptimizerKey(dbName, tableName, optimizerType))
	if !ok {
		return nil, fmt.Errorf(
			"table optimizer %q not found for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrNotFound,
		)
	}

	return cloneTableOptimizer(to), nil
}

func (b *InMemoryBackend) UpdateTableOptimizer(
	dbName, tableName, optimizerType string,
	config TableOptimizerConfiguration,
) error {
	b.mu.Lock("UpdateTableOptimizer")
	defer b.mu.Unlock()

	key := b.tableOptimizerKey(dbName, tableName, optimizerType)
	to, ok := b.tableOptimizers.Get(key)
	if !ok {
		return fmt.Errorf(
			"table optimizer %q not found for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrNotFound,
		)
	}
	to.Configuration = config

	return nil
}

func (b *InMemoryBackend) DeleteTableOptimizer(dbName, tableName, optimizerType string) error {
	b.mu.Lock("DeleteTableOptimizer")
	defer b.mu.Unlock()

	key := b.tableOptimizerKey(dbName, tableName, optimizerType)
	if !b.tableOptimizers.Has(key) {
		return fmt.Errorf(
			"table optimizer %q not found for %s.%s: %w",
			optimizerType,
			dbName,
			tableName,
			ErrNotFound,
		)
	}
	b.tableOptimizers.Delete(key)

	return nil
}

func (b *InMemoryBackend) BatchGetTableOptimizer(
	entries []BatchGetTableOptimizerEntry,
) ([]*TableOptimizer, []BatchGetTableOptimizerError) {
	b.mu.RLock("BatchGetTableOptimizer")
	defer b.mu.RUnlock()

	var found []*TableOptimizer
	var errs []BatchGetTableOptimizerError
	for _, e := range entries {
		key := b.tableOptimizerKey(e.DatabaseName, e.TableName, e.Type)
		if to, ok := b.tableOptimizers.Get(key); ok {
			found = append(found, cloneTableOptimizer(to))
		} else {
			errs = append(errs, BatchGetTableOptimizerError{
				CatalogID:    e.CatalogID,
				DatabaseName: e.DatabaseName,
				TableName:    e.TableName,
				Type:         e.Type,
				Error:        ErrorDetail{ErrorCode: errEntityNotFoundCode, ErrorMessage: "table optimizer not found"},
			})
		}
	}

	return found, errs
}
