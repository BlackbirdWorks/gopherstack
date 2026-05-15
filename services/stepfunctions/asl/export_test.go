package asl

import "context"

// EvaluateIntrinsicFunction is a test-only re-export of the package-internal
// intrinsic function evaluator. It is provided so that black-box tests can
// exercise individual intrinsics without taking a dependency on the executor.
func EvaluateIntrinsicFunction(expr string, input any) (any, error) {
	return evaluateIntrinsicFunction(expr, input)
}

// DecodeReaderItems exposes the package-internal decodeReaderItems for tests.
func DecodeReaderItems(data []byte, cfg *ReaderConfig) ([]any, error) {
	return decodeReaderItems(data, cfg)
}

// BuildContextObject exposes Executor.buildContextObject for black-box tests.
func (e *Executor) BuildContextObject() map[string]any { return e.buildContextObject() }

// NewBranchExecutor exposes Executor.newBranchExecutor for black-box tests.
func (e *Executor) NewBranchExecutor(sm *StateMachine, branchName string) *Executor {
	return e.newBranchExecutor(sm, branchName)
}

// NewMapItemExecutor exposes Executor.newMapItemExecutor for black-box tests.
func (e *Executor) NewMapItemExecutor(sm *StateMachine, idx int, itemValue any) *Executor {
	return e.newMapItemExecutor(sm, idx, itemValue)
}

// ResolveItemsFromReader exposes the package-internal resolveItemsFromReader
// for tests in the asl_test package.
func (e *Executor) ResolveItemsFromReader(ctx context.Context, ir *ItemReader) ([]any, error) {
	return e.resolveItemsFromReader(ctx, ir)
}
