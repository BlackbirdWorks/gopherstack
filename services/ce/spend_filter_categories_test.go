package ce_test

import (
	"maps"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

func newCategoryLedger(t *testing.T) *ce.InMemoryBackend {
	t.Helper()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCEClient(t, ce.NewHandler(b))

	_, err := client.CreateCostCategoryDefinition(t.Context(), &costexplorersdk.CreateCostCategoryDefinitionInput{
		Name: aws.String("Team"), RuleVersion: cetypes.CostCategoryRuleVersionCostCategoryExpressionV1,
		DefaultValue: aws.String("other"),
		Rules: []cetypes.CostCategoryRule{{
			Type: cetypes.CostCategoryRuleTypeInheritedValue,
			InheritedValue: &cetypes.CostCategoryInheritedValueDimension{
				DimensionName: cetypes.CostCategoryInheritedValueDimensionNameTag,
				DimensionKey:  aws.String("team"),
			},
		}},
	})
	require.NoError(t, err)

	base := ce.CostEntry{Date: "2026-01-05", Service: "Test Svc", Account: "000000000000", Region: "us-east-1"}

	blue := base
	blue.Tags, blue.UnblendedCost = map[string]string{"team": "blue"}, 10
	b.RecordCharge(blue)

	red := base
	red.Tags, red.UnblendedCost = map[string]string{"team": "red"}, 5
	b.RecordCharge(red)

	plain := base
	plain.UnblendedCost = 2
	b.RecordCharge(plain)

	sub := base
	sub.SubscriptionID, sub.UnblendedCost = "sp-1", 3
	sub.ProductAttributes = map[string]string{"model": "Claude", "provider": "Anthropic"}
	b.RecordCharge(sub)

	return b
}

func TestLedgerSpendCostCategoryAndSubscriptionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters map[string][]string
		name    string
		want    float64
	}{
		{name: "category_value", filters: map[string][]string{"CostCategory": {"Team$blue"}}, want: 10},
		{name: "category_default_value", filters: map[string][]string{"CostCategory": {"Team$other"}}, want: 5},
		{name: "category_any_of", filters: map[string][]string{"CostCategory": {"Team$blue", "Team$red"}}, want: 15},
		{name: "category_unknown", filters: map[string][]string{"CostCategory": {"Nope$blue"}}, want: 0},
		{name: "category_malformed", filters: map[string][]string{"CostCategory": {"Team"}}, want: 0},
		{name: "subscription", filters: map[string][]string{"SubscriptionId": {"sp-1"}}, want: 3},
		{name: "subscription_other", filters: map[string][]string{"SubscriptionId": {"sp-2"}}, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newCategoryLedger(t)
			f := map[string][]string{"Service": {"Test Svc"}}

			maps.Copy(f, tc.filters)

			assert.InDelta(t, tc.want, b.LedgerSpend("2026-01-05", "2026-01-06", f, "UnblendedCost"), 1e-9)
		})
	}
}

func TestLedgerSpendFilteredCategoryAndProductAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter *ce.SpendFilter
		name   string
		want   float64
	}{
		{
			name:   "category_equals",
			filter: &ce.SpendFilter{CostCategories: &ce.SpendValues{Key: "Team", Values: []string{"blue"}}},
			want:   10,
		},
		{
			name: "category_starts_with",
			filter: &ce.SpendFilter{CostCategories: &ce.SpendValues{
				Key: "Team", Values: []string{"bl"}, MatchOptions: []string{"STARTS_WITH"},
			}},
			want: 10,
		},
		{
			name: "category_not",
			filter: &ce.SpendFilter{
				Not: &ce.SpendFilter{CostCategories: &ce.SpendValues{Key: "Team", Values: []string{"blue"}}},
			},
			want: 10,
		},
		{
			name:   "product_attribute",
			filter: &ce.SpendFilter{ProductAttributes: &ce.SpendValues{Key: "model", Values: []string{"Claude"}}},
			want:   3,
		},
		{
			name: "product_attribute_case_sensitive",
			filter: &ce.SpendFilter{ProductAttributes: &ce.SpendValues{
				Key: "model", Values: []string{"claude"}, MatchOptions: []string{"EQUALS", "CASE_SENSITIVE"},
			}},
			want: 0,
		},
		{
			name: "product_attribute_absent",
			filter: &ce.SpendFilter{ProductAttributes: &ce.SpendValues{
				Key: "model", MatchOptions: []string{"ABSENT"},
			}},
			want: 17,
		},
		{
			name:   "product_attribute_key_is_case_sensitive",
			filter: &ce.SpendFilter{ProductAttributes: &ce.SpendValues{Key: "Model", Values: []string{"Claude"}}},
			want:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newCategoryLedger(t)
			got := b.LedgerSpendFiltered(
				"2026-01-05", "2026-01-06", tc.filter, map[string][]string{"Service": {"Test Svc"}}, "UnblendedCost",
			)

			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}
