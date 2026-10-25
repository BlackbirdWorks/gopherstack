package odatatable

import (
	"fmt"
	"slices"
	"time"
)

// BatchOpKind is the kind of one entity operation in an atomic batch.
type BatchOpKind int

const (
	// BatchInsert creates an entity and fails if it exists.
	BatchInsert BatchOpKind = iota
	// BatchReplace replaces an entity; an empty IfMatch makes it an upsert.
	BatchReplace
	// BatchMerge merges properties into an entity; an empty IfMatch makes it an upsert.
	BatchMerge
	// BatchDelete deletes an existing entity.
	BatchDelete
	// BatchRetrieve reads one entity; it is only valid as the sole operation of a batch, outside any changeset.
	BatchRetrieve
)

// BatchOp is one operation of an atomic batch against a single table.
type BatchOp struct {
	Props        map[string]EntityProperty
	PartitionKey string
	RowKey       string
	IfMatch      string
	Kind         BatchOpKind
}

// BatchError reports which operation of a batch failed; Err is one of the package sentinels.
type BatchError struct {
	Err   error
	Index int
}

func (e *BatchError) Error() string { return fmt.Sprintf("batch operation %d: %v", e.Index, e.Err) }

func (e *BatchError) Unwrap() error { return e.Err }

type batchUndo struct {
	prev *storedEntity
	key  entityCompositeKey
}

// ExecuteBatch applies ops to table under one lock: either every operation commits or none does. It returns one
// EntityInfo per operation (zero for a delete), or a *BatchError naming the failing operation.
func (b *InMemoryBackend) ExecuteBatch(table string, ops []BatchOp) ([]EntityInfo, error) {
	b.mu.Lock("ExecuteBatch")
	defer b.mu.Unlock()

	t, ok := b.tables[table]
	if !ok {
		return nil, &BatchError{Index: 0, Err: ErrTableNotFound}
	}

	var undo []batchUndo

	out := make([]EntityInfo, 0, len(ops))

	for i, op := range ops {
		key := entityKey(op.PartitionKey, op.RowKey)
		prev := t.Entities[key]

		info, err := b.applyBatchOp(t, key, prev, op)
		if err != nil {
			revertBatch(t, undo)

			return nil, &BatchError{Index: i, Err: err}
		}

		undo = append(undo, batchUndo{key: key, prev: prev})
		out = append(out, info)
	}

	return out, nil
}

func revertBatch(t *storedTable, undo []batchUndo) {
	for _, u := range slices.Backward(undo) {
		if u.prev == nil {
			delete(t.Entities, u.key)
		} else {
			t.Entities[u.key] = u.prev
		}
	}

	t.sorted.Store(nil)
}

// applyBatchOp performs one operation, replacing (never mutating) the stored entity so the caller can revert.
func (b *InMemoryBackend) applyBatchOp(
	t *storedTable, key entityCompositeKey, prev *storedEntity, op BatchOp,
) (EntityInfo, error) {
	exists := prev != nil

	if op.Kind == BatchInsert && exists {
		return EntityInfo{}, ErrEntityAlreadyExists
	}

	if op.Kind == BatchDelete && !exists {
		return EntityInfo{}, ErrEntityNotFound
	}

	if err := b.checkIfMatch(prev, exists, op.IfMatch); err != nil {
		return EntityInfo{}, err
	}

	if op.Kind == BatchDelete {
		delete(t.Entities, key)
		t.sorted.Store(nil)

		return EntityInfo{}, nil
	}

	next := &storedEntity{PartitionKey: op.PartitionKey, RowKey: op.RowKey, Properties: cloneProps(op.Props)}
	next.Timestamp = b.bumpTimestamp(&storedEntity{Timestamp: timestampOf(prev)}, exists)

	if op.Kind == BatchMerge && exists {
		merged := cloneProps(prev.Properties)
		for name, p := range op.Props {
			merged[name] = cloneProp(p)
		}

		next.Properties = merged
	}

	t.Entities[key] = next
	t.sorted.Store(nil)

	return b.info(next), nil
}

func timestampOf(e *storedEntity) time.Time {
	if e == nil {
		return time.Time{}
	}

	return e.Timestamp
}
