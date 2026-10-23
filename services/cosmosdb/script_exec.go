package cosmosdb

import (
	"github.com/dop251/goja"
)

// ScriptOutput is the outcome of a stored procedure run.
type ScriptOutput struct {
	Body    any
	Logs    []string
	Writes  int
	HasBody bool
}

// ExecuteSproc runs a stored procedure against one partition. Everything it writes commits together or, on any
// failure, not at all.
func (b *InMemoryBackend) ExecuteSproc(dbID, collID, id, partitionKey string, args []any) (ScriptOutput, error) {
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
