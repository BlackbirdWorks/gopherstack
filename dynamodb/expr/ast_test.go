package expr

import (
	"testing"
)

func TestASTNodes_exprNode(t *testing.T) {
	t.Parallel()

	// Test each node type to satisfy the interface and coverage
	nodes := []Node{
		&LogicalExpr{},
		&NotExpr{},
		&ComparisonExpr{},
		&BetweenExpr{},
		&InExpr{},
		&FunctionExpr{},
		&PathExpr{},
		&ValuePlaceholder{},
		&UpdateExpr{},
		&ProjectionExpr{},
	}

	for _, n := range nodes {
		n.exprNode()
	}
}
