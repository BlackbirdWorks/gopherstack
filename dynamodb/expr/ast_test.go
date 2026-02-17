package expr_test

import (
	"context"
	"testing"

	"Gopherstack/dynamodb/expr"
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
			ctx := context.Background()
			_ = ctx
			// Verify node is not nil and is of correct type
			if tt.node == nil {
				t.Fatal("node should not be nil")
			}
			// Accessing any Node interface method would work here
			// This test serves as compile-time verification that all types implement expr.Node
			_ = tt.node
		})
	}
}
