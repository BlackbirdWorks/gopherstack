package expr

// Test-only exports to allow white-box testing from the expr_test package.

func (e *Evaluator) EvaluateFunction(n *FunctionExpr) (any, error) {
	return e.evaluateFunction(n)
}

func (e *Evaluator) UnwrapAttributeValue(v any) any {
	return e.unwrapAttributeValue(v)
}

func (e *Evaluator) CalculateSize(v any) float64 {
	return e.calculateSize(v)
}

func (e *Evaluator) Mutate(current any, path []PathElement, value any, isRemove bool) (any, error) {
	return e.mutate(current, path, value, isRemove)
}

func (e *Evaluator) ExportedApplyAdd(path []PathElement, val any) error {
	return e.applyAdd(path, val)
}

func (e *Evaluator) CompareValues(lhs any, op TokenType, rhs any) bool {
	return e.compareValues(lhs, op, rhs)
}

func (e *Evaluator) ToString(v any) string {
	return e.toString(v)
}

func (e *Evaluator) GetValueAtPath(item map[string]any, path []PathElement) (any, bool) {
	return e.getValueAtPath(item, path)
}
