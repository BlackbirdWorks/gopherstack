package stepfunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// distributedMapChildRunner adapts InMemoryBackend to asl.DistributedMapRunner:
// it is the seam a DISTRIBUTED Map state's ItemProcessor calls into instead
// of running inline, letting the backend spawn a real child Execution per
// item/batch.
type distributedMapChildRunner struct {
	backend *InMemoryBackend
}

func (d *distributedMapChildRunner) RunDistributedMapItem(
	ctx context.Context,
	executionARN, mapRunARN, _ string,
	iterator *asl.StateMachine,
	_ int,
	item any,
) (any, error) {
	return d.backend.runDistributedMapChild(ctx, executionARN, mapRunARN, iterator, item)
}

// distributedMapChildContext is the identity a Distributed Map child
// execution inherits from its parent: AWS attributes child executions to the
// SAME state machine resource the Map state lives in (ItemProcessor is
// nested ASL, not a separately registered state machine), so the child's own
// StateMachineArn/RoleArn/Name/definition all mirror the parent's.
type distributedMapChildContext struct {
	smArn      string
	smName     string
	roleArn    string
	definition string
}

// distributedMapChildContextLocked resolves parentExecARN's identity for a
// child execution. Caller must hold at least b.mu's read lock.
func (b *InMemoryBackend) distributedMapChildContextLocked(parentExecARN string) (distributedMapChildContext, bool) {
	exec, ok := b.executions.Get(parentExecARN)
	if !ok {
		return distributedMapChildContext{}, false
	}

	cc := distributedMapChildContext{
		smArn:      exec.StateMachineArn,
		definition: b.executionDefinitions[parentExecARN],
	}

	if sm, smOK := b.stateMachines.Get(cc.smArn); smOK {
		cc.smName = sm.Name
		cc.roleArn = sm.RoleArn
	}

	return cc, true
}

// distributedMapItemCount reports how many Map items a Distributed Map
// child's input represents: 1 for a plain item, or len(Items) when
// ItemBatcher wrapped several items into this child's input shape
// ({"Items": [...], "BatchInput": ...} -- see wrapItemBatcherBatches in
// asl/executor.go). Matches AWS's ExecutionListItem.itemCount semantics
// (sfn@v1.49.0 types.go:308-313).
func distributedMapItemCount(item any) int {
	batch, ok := item.(map[string]any)
	if !ok {
		return 1
	}

	items, ok := batch["Items"].([]any)
	if !ok {
		return 1
	}

	return len(items)
}

// marshalDistributedMapInput mirrors asl.marshalInput's nil-safe encoding
// (unexported in that package) so a child execution's persisted Input field
// matches exactly what an INLINE iteration would have passed its
// sub-executor.
func marshalDistributedMapInput(item any) string {
	if item == nil {
		return "{}"
	}

	b, err := json.Marshal(item)
	if err != nil {
		return "{}"
	}

	return string(b)
}

// runDistributedMapChild spawns a real child Execution for one Distributed
// Map item (or ItemBatcher batch) and runs it through the same
// create-record/interpret/finalize-record machinery StartExecution uses
// (initializeExecutionRecord / finalizeExecutionRecordLocked), blocking
// until the child reaches a terminal state -- exactly like
// runParsedExecution's async path, just awaited in place instead of
// backgrounded, since the Map state needs the child's result to build its
// own output. Implements the callback asl.DistributedMapRunner declares.
func (b *InMemoryBackend) runDistributedMapChild(
	ctx context.Context,
	parentExecARN, mapRunARN string,
	iterator *asl.StateMachine,
	item any,
) (any, error) {
	b.mu.RLock("runDistributedMapChild.context")
	cc, ok := b.distributedMapChildContextLocked(parentExecARN)
	integrations := b.snapshotIntegrationsLocked()
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, parentExecARN)
	}

	input := marshalDistributedMapInput(item)
	itemCount := distributedMapItemCount(item)

	const millisPerSecond = 1000.0
	now := float64(time.Now().UnixMilli()) / millisPerSecond

	b.mu.Lock("runDistributedMapChild.create")
	b.mapChildSeq++
	childName := fmt.Sprintf("distmap-%d", b.mapChildSeq)
	childExecARN := b.execARN(cc.smArn, cc.smName, childName)
	childExec := b.initializeExecutionRecord(
		cc.smArn, childName, childExecARN, input, cc.definition, now, "", "",
	)
	childExec.MapRunArn = mapRunARN
	childExec.ItemCount = itemCount
	// Re-file into executionsByMapRun now that MapRunArn is set --
	// initializeExecutionRecord's own Put ran before this assignment. See
	// store.Index's doc comment on this exact "mutate then re-Put" pattern.
	b.executions.Put(childExec)

	childCtx, cancel := context.WithCancel(ctx)
	b.cancelFns[childExecARN] = cancel
	b.mu.Unlock()

	defer cancel()

	childSM := &asl.StateMachine{StartAt: iterator.StartAt, States: iterator.States}
	rec := &historyRecorder{backend: b}
	executor := asl.NewExecutor(childSM, integrations.lambdaInvoker, rec)
	applyIntegrations(executor, integrations)
	executor.SetActivityInvoker(b)
	executor.SetTaskTokenCallbackInvoker(b)
	executor.SetMapRunNotifier(b)
	executor.SetDistributedMapRunner(&distributedMapChildRunner{backend: b})
	executor.SetExecutionContext(
		childExecARN, childName, cc.roleArn,
		time.Unix(int64(now), 0).UTC().Format(time.RFC3339),
		cc.smArn, cc.smName,
	)

	result, execErr := executor.Execute(childCtx, childExecARN, input)

	b.mu.Lock("runDistributedMapChild.finalize")
	delete(b.cancelFns, childExecARN)

	if b.deletedExecs[childExecARN] {
		delete(b.deletedExecs, childExecARN)
	} else if childExec.Status == statusRunning {
		b.finalizeExecutionRecordLocked(childExec, childExecARN, result, execErr)
	}
	b.mu.Unlock()

	if execErr != nil {
		return nil, execErr
	}

	if result.Failed {
		return nil, &asl.FailError{ErrCode: result.Error, Cause: result.Cause}
	}

	return result.Output, nil
}
