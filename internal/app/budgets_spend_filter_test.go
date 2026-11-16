package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	budgetsbackend "github.com/blackbirdworks/gopherstack/services/budgets"
)

func TestBudgetsSpendFilterCarriesCategoryAndProductAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expr *budgetsbackend.Expression
		name string
	}{
		{
			name: "cost_categories",
			expr: &budgetsbackend.Expression{CostCategories: &budgetsbackend.ValuesFilter{
				Key: "Team", Values: []string{"blue"}, MatchOptions: []string{"EQUALS"},
			}},
		},
		{
			name: "product_attributes",
			expr: &budgetsbackend.Expression{ProductAttributes: &budgetsbackend.ValuesFilter{
				Key: "model", Values: []string{"Claude"},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := budgetsSpendFilter(&budgetsbackend.Expression{And: []budgetsbackend.Expression{*tc.expr}})
			require.Len(t, got.And, 1)

			switch {
			case tc.expr.CostCategories != nil:
				require.NotNil(t, got.And[0].CostCategories)
				assert.Equal(t, "Team", got.And[0].CostCategories.Key)
				assert.Equal(t, []string{"blue"}, got.And[0].CostCategories.Values)
			default:
				require.NotNil(t, got.And[0].ProductAttributes)
				assert.Equal(t, "model", got.And[0].ProductAttributes.Key)
			}
		})
	}
}
