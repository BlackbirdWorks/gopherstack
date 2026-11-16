package budgets_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/stretchr/testify/assert"
)

func nestedNot(depth int) *types.Expression {
	e := &types.Expression{Tags: &types.TagValues{Key: aws.String("k"), Values: []string{"v"}}}
	for range depth {
		e = &types.Expression{Not: e}
	}

	return e
}

func TestBudgetExpressionClauseValidation(t *testing.T) {
	t.Parallel()

	pa := func(key string, vals []string, opts ...types.MatchOption) *types.Expression {
		return &types.Expression{ProductAttributes: &types.ProductAttributeValues{
			Key: aws.String(key), Values: vals, MatchOptions: opts,
		}}
	}

	tests := []struct {
		expr *types.Expression
		name string
		want string
	}{
		{name: "pa_ok", expr: pa("model", []string{"Claude"}, types.MatchOptionEquals)},
		{
			name: "pa_ok_sensitive",
			expr: pa("model", []string{"Claude"}, types.MatchOptionEquals, types.MatchOptionCaseSensitive),
		},
		{name: "pa_absent_ok", expr: pa("provider", nil, types.MatchOptionAbsent)},
		{name: "pa_bad_key", expr: pa("colour", []string{"x"}), want: "InvalidParameterException"},
		{name: "pa_no_values", expr: pa("model", nil), want: "InvalidParameterException"},
		{
			name: "pa_sensitive_alone", want: "InvalidParameterException",
			expr: pa("model", []string{"x"}, types.MatchOptionCaseSensitive),
		},
		{
			name: "pa_absent_sensitive", want: "InvalidParameterException",
			expr: pa("model", nil, types.MatchOptionAbsent, types.MatchOptionCaseSensitive),
		},
		{
			name: "cc_no_key",
			expr: &types.Expression{CostCategories: &types.CostCategoryValues{Values: []string{"x"}}},
			want: "InvalidParameterException",
		},
		{name: "depth_ok", expr: nestedNot(30)},
		{name: "depth_too_deep", expr: nestedNot(200), want: "InvalidParameterException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			b := costBudget("b1", "10")
			b.FilterExpression = tc.expr

			_, err := f.client.CreateBudget(
				t.Context(), &bsdk.CreateBudgetInput{AccountId: aws.String(testAccount), Budget: b})
			assert.Equal(t, tc.want, apiCode(err))
		})
	}
}
