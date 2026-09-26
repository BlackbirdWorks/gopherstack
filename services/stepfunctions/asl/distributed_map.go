package asl

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// processorModeDistributed is ItemProcessor.ProcessorConfig.Mode's
// DISTRIBUTED value (AWS Step Functions ASL reference, Map state ->
// ItemProcessor -> ProcessorConfig: "Configure a Distributed Map state",
// docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html).
// Mode defaults to INLINE when ProcessorConfig is omitted or Mode is "".
const processorModeDistributed = "DISTRIBUTED"

// DistributedMapItemResult is what running one Distributed Map item (or
// ItemBatcher batch) as a real child execution contributes back: its
// output, plus enough of the child's own identity for ResultWriter's
// Transformation: NONE metadata (ExecutionArn, Name, StartDate, StopDate --
// AWS docs: input-output-resultwriter.html). Populated regardless of
// whether the child succeeded or failed, so a FAILED item's record can
// still carry its own ExecutionArn/Name/dates.
type DistributedMapItemResult struct {
	Output       any
	ExecutionArn string
	Name         string
	StartDate    float64
	StopDate     float64
}

// DistributedMapRunner spawns a real child state-machine execution for one
// Distributed Map item (or ItemBatcher batch) and blocks until it reaches a
// terminal state, returning its output the way an INLINE iteration's
// in-process sub-executor result would. Implemented by the backend, which
// owns execution storage; wired in via SetDistributedMapRunner. When unset,
// a DISTRIBUTED Map state falls back to running its ItemProcessor inline --
// the same behavior as before this feature existed.
type DistributedMapRunner interface {
	RunDistributedMapItem(
		ctx context.Context,
		executionARN, mapRunARN, stateName string,
		iterator *StateMachine,
		idx int,
		item any,
	) (DistributedMapItemResult, error)
}

// SetDistributedMapRunner configures the backend hook that spawns real child
// executions for a DISTRIBUTED Map state's ItemProcessor.
func (e *Executor) SetDistributedMapRunner(r DistributedMapRunner) {
	e.distributedMapRunner = r
}

// isDistributedMapIterator reports whether iterator (the Map state's
// resolved Iterator/ItemProcessor) declares ProcessorConfig.Mode ==
// DISTRIBUTED.
func isDistributedMapIterator(iterator *StateMachine) bool {
	return iterator != nil &&
		iterator.ProcessorConfig != nil &&
		iterator.ProcessorConfig.Mode == processorModeDistributed
}

// runDistributedMapTasks runs items through e.distributedMapRunner at the
// resolved concurrency, mirroring runMapTasks's semaphore-bounded fan-out for
// INLINE Map states but spawning a real child execution per item/batch
// instead of an in-process sub-executor.
func (e *Executor) runDistributedMapTasks(
	ctx context.Context,
	executionARN, mapRunARN, stateName string,
	iterator *StateMachine,
	items []any,
	results []any,
	errs []error,
	meta []DistributedMapItemResult,
	concurrency int,
) {
	sem := semaphore.NewWeighted(int64(concurrency))
	var wg sync.WaitGroup

	for i, item := range items {
		if ctx.Err() != nil {
			break
		}

		e.spawnDistributedMapTask(
			ctx, executionARN, mapRunARN, stateName, iterator, i, item, results, errs, meta, sem, &wg,
		)
	}

	wg.Wait()
}

func (e *Executor) spawnDistributedMapTask(
	ctx context.Context,
	executionARN, mapRunARN, stateName string,
	iterator *StateMachine,
	idx int,
	item any,
	results []any,
	errs []error,
	meta []DistributedMapItemResult,
	sem *semaphore.Weighted,
	wg *sync.WaitGroup,
) {
	wg.Go(func() {
		if err := sem.Acquire(ctx, 1); err != nil {
			return
		}
		defer sem.Release(1)

		out, err := e.distributedMapRunner.RunDistributedMapItem(
			ctx, executionARN, mapRunARN, stateName, iterator, idx, item,
		)
		meta[idx] = out

		if err != nil {
			errs[idx] = err

			return
		}

		results[idx] = out.Output
	})
}
