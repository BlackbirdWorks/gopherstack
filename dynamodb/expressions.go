package dynamodb

import (
	"Gopherstack/dynamodb/expr"
)

// EvaluateExpression evaluates a DynamoDB condition expression against an item.
func EvaluateExpression(
	expression string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	if expression == "" {
		return true, nil
	}

	l := expr.NewLexer(expression)
	p := expr.NewParser(l)
	node, err := p.ParseCondition()
	if err != nil {
		return false, err
	}

	eval := &expr.Evaluator{
		Item:       item,
		AttrNames:  attrNames,
		AttrValues: attrValues,
	}

	result, err := eval.Evaluate(node)
	if err != nil {
		return false, err
	}

	if b, ok := result.(bool); ok {
		return b, nil
	}

	return false, nil
}

// applyUpdate is an internal entry point for updating an item using an UpdateExpression.
// It returns the set of top-level attribute names that were touched by the expression.
func applyUpdate(
	item map[string]any,
	expression string,
	attrNames map[string]string,
	attrValues map[string]any,
) (map[string]struct{}, error) {
	if expression == "" {
		return nil, nil //nolint:nilnil // no paths touched when expression is empty
	}

	l := expr.NewLexer(expression)
	p := expr.NewParser(l)
	u, err := p.ParseUpdate()
	if err != nil {
		return nil, err
	}

	eval := &expr.Evaluator{
		Item:       item,
		AttrNames:  attrNames,
		AttrValues: attrValues,
	}

	if applyErr := eval.ApplyUpdate(u); applyErr != nil {
		return nil, applyErr
	}

	return eval.UpdatedPaths, nil
}

// projectItem creates a new item containing only the attributes specified in the ProjectionExpression.
func projectItem(item map[string]any, projectionExpression string, attrNames map[string]string) map[string]any {
	if projectionExpression == "" {
		return item
	}

	l := expr.NewLexer(projectionExpression)
	p := expr.NewParser(l)
	proj, err := p.ParseProjection()
	if err != nil {
		return item // Return full item if projection fails? Or error? Standard seems to be quiet.
	}

	eval := &expr.Evaluator{
		Item:      item,
		AttrNames: attrNames,
	}

	return eval.ApplyProjection(proj)
}

// Compatibility layer for unexported calls within the package.
func evaluateExpression(
	expression string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	return EvaluateExpression(expression, item, attrValues, attrNames)
}
