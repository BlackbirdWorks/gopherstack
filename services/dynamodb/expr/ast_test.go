package expr_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"
)

func TestASTNodes_ImplementNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		node expr.Node
		name string
	}{
		{
			name: "LogicalExpr implements Node",
			node: &expr.LogicalExpr{},
		},
		{
			name: "NotExpr implements Node",
			node: &expr.NotExpr{},
		},
		{
			name: "ComparisonExpr implements Node",
			node: &expr.ComparisonExpr{},
		},
		{
			name: "BetweenExpr implements Node",
			node: &expr.BetweenExpr{},
		},
		{
			name: "InExpr implements Node",
			node: &expr.InExpr{},
		},
		{
			name: "FunctionExpr implements Node",
			node: &expr.FunctionExpr{},
		},
		{
			name: "PathExpr implements Node",
			node: &expr.PathExpr{},
		},
		{
			name: "ValuePlaceholder implements Node",
			node: &expr.ValuePlaceholder{},
		},
		{
			name: "UpdateExpr implements Node",
			node: &expr.UpdateExpr{},
		},
		{
			name: "ProjectionExpr implements Node",
			node: &expr.ProjectionExpr{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.NotNil(t, tt.node, "node should not be nil")
		})
	}
}
