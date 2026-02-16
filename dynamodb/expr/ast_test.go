package expr_test

import (
	"Gopherstack/dynamodb/expr"
	"testing"
)

func TestASTNodes_ImplementNode(t *testing.T) {
	t.Parallel()

	// Verify all node types satisfy the Node interface.
	nodes := []expr.Node{
		&expr.LogicalExpr{},
		&expr.NotExpr{},
		&expr.ComparisonExpr{},
		&expr.BetweenExpr{},
		&expr.InExpr{},
		&expr.FunctionExpr{},
		&expr.PathExpr{},
		&expr.ValuePlaceholder{},
		&expr.UpdateExpr{},
		&expr.ProjectionExpr{},
	}

	_ = nodes
}
