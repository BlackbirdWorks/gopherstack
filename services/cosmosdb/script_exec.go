package cosmosdb

import (
	"maps"
	"slices"

	"github.com/dop251/goja"
)

// ScriptOutput is the outcome of a stored procedure run.
type ScriptOutput struct {
	Body    any
	Logs    []string
	Writes  int
	HasBody bool
}

// sprocOptimisticTries bounds the lock-free attempts before a stored procedure runs under the write lock.
const sprocOptimisticTries = 4

// ExecuteSproc runs a stored procedure against one partition. Everything it writes commits together or, on any
// failure, not at all. The script runs on a private copy of the container without holding the backend lock and is
// committed only if the container was not written meanwhile; after repeated conflicts it runs under the lock.
func (b *InMemoryBackend) ExecuteSproc(dbID, collID, id, partitionKey string, args []any) (ScriptOutput, error) {
	if b.cur.Load() != nil {
		return b.executeSprocLocked(dbID, collID, id, partitionKey, args)
	}

	for range sprocOptimisticTries {
		out, done, err := b.trySprocOptimistic(dbID, collID, id, partitionKey, args)
		if done {
			return out, err
		}
	}

	return b.executeSprocLocked(dbID, collID, id, partitionKey, args)
}

func (b *InMemoryBackend) trySprocOptimistic(
	dbID, collID, id, partitionKey string, args []any,
) (ScriptOutput, bool, error) {
	b.mu.RLock("ExecuteSproc")

	tx, err := b.beginTxAt(dbID, collID, false)
	if err != nil {
		b.mu.RUnlock()

		return ScriptOutput{}, true, err
	}

	if pkErr := tx.c.checkPartitionKey(partitionKey); pkErr != nil {
		b.mu.RUnlock()

		return ScriptOutput{}, true, pkErr
	}

	s, ok := tx.c.scriptMap(kindSproc, false)[id]
	if !ok {
		b.mu.RUnlock()

		return ScriptOutput{}, true, ErrScriptNotFound
	}

	script := *s
	live := tx.c
	shadow := tx.c.shadowCopy()
	shadow.keep = b.changeRetention()
	b.mu.RUnlock()

	stx := &containerTx{b: b, c: shadow, dbID: dbID, collID: collID, now: tx.now, seq0: tx.seq0}
	out, runErr := stx.runSproc(&script, partitionKey, args)

	b.mu.Lock("ExecuteSproc")
	defer b.mu.Unlock()

	if cur, findErr := b.findDataContainer(
		dbID,
		collID,
		false,
	); findErr != nil || cur != live ||
		live.ChangeSeq != tx.seq0 {
		return ScriptOutput{}, false, nil
	}

	if runErr != nil {
		return ScriptOutput{}, true, runErr
	}

	live.Documents, live.ChangeLog, live.ChangeSeq = shadow.Documents, shadow.ChangeLog, shadow.ChangeSeq
	live.ChangeFloor = shadow.ChangeFloor
	live.sorted.Store(nil)

	return out, true, nil
}

// shadowCopy is a private copy of the document state a script may change; configuration is shared read-only.
func (c *storedContainer) shadowCopy() *storedContainer {
	return &storedContainer{
		Modified: c.Modified, Offer: c.Offer, Triggers: c.Triggers, Sprocs: c.Sprocs, UDFs: c.UDFs,
		Documents: maps.Clone(c.Documents), DefaultTTL: c.DefaultTTL, ID: c.ID,
		PartitionKeyPath: c.PartitionKeyPath, ConflictResolutionPolicy: c.ConflictResolutionPolicy,
		IndexingPolicy: c.IndexingPolicy, PartitionKeyPaths: c.PartitionKeyPaths,
		UniqueKeyPolicy: c.UniqueKeyPolicy, GeospatialConfig: c.GeospatialConfig,
		VectorEmbeddingPolicy: c.VectorEmbeddingPolicy, FullTextPolicy: c.FullTextPolicy,
		ComputedProperties: c.ComputedProperties, ChangeLog: slices.Clip(c.ChangeLog), ChangeSeq: c.ChangeSeq,
		ChangeFloor: c.ChangeFloor, keep: c.keep,
	}
}

func (b *InMemoryBackend) executeSprocLocked(
	dbID, collID, id, partitionKey string, args []any,
) (ScriptOutput, error) {
	b.mu.Lock("ExecuteSproc")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, collID)
	if err != nil {
		return ScriptOutput{}, err
	}

	if pkErr := tx.c.checkPartitionKey(partitionKey); pkErr != nil {
		return ScriptOutput{}, pkErr
	}

	s, ok := tx.c.scriptMap(kindSproc, false)[id]
	if !ok {
		return ScriptOutput{}, ErrScriptNotFound
	}

	out, err := tx.runSproc(s, partitionKey, args)
	if err != nil {
		tx.rollback()

		return ScriptOutput{}, err
	}

	return out, nil
}

func (tx *containerTx) runSproc(s *storedScript, partitionKey string, args []any) (ScriptOutput, error) {
	vm, stop := tx.b.newSandbox()
	defer stop()

	env := newScriptEnv(vm, tx, partitionKey)

	fn, err := loadFunction(vm, s.Body)
	if err != nil {
		return ScriptOutput{}, err
	}

	jsArgs := make([]goja.Value, len(args))
	for i, a := range args {
		jsArgs[i] = toJS(vm, a)
	}

	if _, callErr := fn(goja.Undefined(), jsArgs...); callErr != nil {
		return ScriptOutput{}, scriptError(callErr)
	}

	out := ScriptOutput{Logs: env.logs, Writes: len(tx.undo), HasBody: env.respSet}
	if env.respSet {
		out.Body = jsonValue(env.respObj)
	}

	return out, nil
}

// runTrigger runs a trigger body with no arguments.
func (e *scriptEnv) runTrigger(s *storedScript) error {
	fn, err := loadFunction(e.vm, s.Body)
	if err != nil {
		return err
	}

	if _, callErr := fn(goja.Undefined()); callErr != nil {
		return scriptError(callErr)
	}

	return nil
}
