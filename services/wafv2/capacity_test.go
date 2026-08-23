package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// checkCapacityFor issues a CheckCapacity request for a single rule wrapping
// stmt and returns the Capacity the handler reports.
func checkCapacityFor(t *testing.T, h *wafv2.Handler, stmt map[string]any) int {
	t.Helper()

	rec := doWafv2Request(t, h, "CheckCapacity", map[string]any{
		"Scope": "REGIONAL",
		"Rules": []any{
			map[string]any{
				"Name":      "r1",
				"Priority":  0,
				"Statement": stmt,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

	consumed, ok := result["Capacity"].(float64)
	require.True(t, ok)

	return int(consumed)
}

// TestCheckCapacity_PerStatementWCUModel verifies CheckCapacity replicates
// AWS's real per-statement-type WCU cost model (see capacity.go's doc
// comment for the sourcing of every constant), instead of a flat 1-WCU-per-rule
// stub.
func TestCheckCapacity_PerStatementWCUModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stmt map[string]any
		name string
		want int
	}{
		{
			name: "ByteMatch_ExactlyShort",
			stmt: map[string]any{"ByteMatchStatement": map[string]any{
				"SearchString":         "x",
				"PositionalConstraint": "EXACTLY",
				"FieldToMatch":         map[string]any{"UriPath": map[string]any{}},
				"TextTransformations":  []any{map[string]any{"Priority": 0, "Type": "NONE"}},
			}},
			want: 2 + 10, // base 2 + one text transformation
		},
		{
			name: "ByteMatch_ContainsWord",
			stmt: map[string]any{"ByteMatchStatement": map[string]any{
				"SearchString":         "x",
				"PositionalConstraint": "CONTAINS_WORD",
				"FieldToMatch":         map[string]any{"AllQueryArguments": map[string]any{}},
				"TextTransformations":  []any{},
			}},
			want: 10 + 10, // base 10 (CONTAINS_WORD) + AllQueryArguments surcharge
		},
		{
			name: "ByteMatch_JsonBodyDoubles",
			stmt: map[string]any{"ByteMatchStatement": map[string]any{
				"SearchString":         "x",
				"PositionalConstraint": "STARTS_WITH",
				"FieldToMatch":         map[string]any{"JsonBody": map[string]any{}},
				"TextTransformations":  []any{},
			}},
			want: 4, // base 2, doubled for JsonBody
		},
		{
			name: "SqliMatch_DefaultLow",
			stmt: map[string]any{"SqliMatchStatement": map[string]any{
				"FieldToMatch":        map[string]any{"Body": map[string]any{}},
				"TextTransformations": []any{},
			}},
			want: 20,
		},
		{
			name: "SqliMatch_High",
			stmt: map[string]any{"SqliMatchStatement": map[string]any{
				"SensitivityLevel":    "HIGH",
				"FieldToMatch":        map[string]any{"Body": map[string]any{}},
				"TextTransformations": []any{},
			}},
			want: 30,
		},
		{
			name: "XssMatch_Base",
			stmt: map[string]any{"XssMatchStatement": map[string]any{
				"FieldToMatch":        map[string]any{"Body": map[string]any{}},
				"TextTransformations": []any{},
			}},
			want: 40,
		},
		{
			name: "SizeConstraint_Base",
			stmt: map[string]any{"SizeConstraintStatement": map[string]any{
				"ComparisonOperator":  "GT",
				"Size":                100,
				"FieldToMatch":        map[string]any{"QueryString": map[string]any{}},
				"TextTransformations": []any{},
			}},
			want: 1,
		},
		{
			name: "RegexMatch_Base",
			stmt: map[string]any{"RegexMatchStatement": map[string]any{
				"RegexString":         "^a",
				"FieldToMatch":        map[string]any{"UriPath": map[string]any{}},
				"TextTransformations": []any{},
			}},
			want: 3,
		},
		{
			name: "RegexPatternSetReference_Base",
			stmt: map[string]any{"RegexPatternSetReferenceStatement": map[string]any{
				"ARN":          "arn:aws:wafv2:us-east-1:000000000000:regional/regexpatternset/x/id",
				"FieldToMatch": map[string]any{"UriPath": map[string]any{}},
				"TextTransformations": []any{
					map[string]any{"Priority": 0, "Type": "NONE"},
					map[string]any{"Priority": 1, "Type": "LOWERCASE"},
				},
			}},
			want: 25 + 20, // base 25 + 2 text transformations
		},
		{
			name: "GeoMatch",
			stmt: map[string]any{"GeoMatchStatement": map[string]any{"CountryCodes": []any{"US"}}},
			want: 1,
		},
		{
			name: "LabelMatch",
			stmt: map[string]any{"LabelMatchStatement": map[string]any{"Scope": "LABEL", "Key": "x"}},
			want: 1,
		},
		{
			name: "AsnMatch",
			stmt: map[string]any{"AsnMatchStatement": map[string]any{"AsnList": []any{64496}}},
			want: 1,
		},
		{
			name: "IPSetReference_NoForwardedIP",
			stmt: map[string]any{"IPSetReferenceStatement": map[string]any{
				"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/id",
			}},
			want: 1,
		},
		{
			name: "IPSetReference_ForwardedIPAny",
			stmt: map[string]any{"IPSetReferenceStatement": map[string]any{
				"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/id",
				"IPSetForwardedIPConfig": map[string]any{
					"HeaderName":       "X-Forwarded-For",
					"FallbackBehavior": "MATCH",
					"Position":         "ANY",
				},
			}},
			want: 1 + 4,
		},
		{
			name: "IPSetReference_ForwardedIPFirst_NoSurcharge",
			stmt: map[string]any{"IPSetReferenceStatement": map[string]any{
				"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/id",
				"IPSetForwardedIPConfig": map[string]any{
					"HeaderName":       "X-Forwarded-For",
					"FallbackBehavior": "MATCH",
					"Position":         "FIRST",
				},
			}},
			want: 1,
		},
		{
			name: "RateBased_BaseOnly",
			stmt: map[string]any{"RateBasedStatement": map[string]any{
				"Limit":            2000,
				"AggregateKeyType": "IP",
			}},
			want: 2,
		},
		{
			name: "RateBased_CustomKeysAndScopeDown",
			stmt: map[string]any{"RateBasedStatement": map[string]any{
				"Limit":            2000,
				"AggregateKeyType": "CUSTOM_KEYS",
				"CustomKeys": []any{
					map[string]any{"IP": map[string]any{}},
					map[string]any{"Header": map[string]any{"Name": "x", "TextTransformations": []any{}}},
				},
				"ScopeDownStatement": map[string]any{
					"GeoMatchStatement": map[string]any{"CountryCodes": []any{"US"}},
				},
			}},
			want: 2 + 2*30 + 1, // base 2 + 2 custom keys*30 + GeoMatch scope-down (1)
		},
		{
			name: "AndStatement_SumsNested",
			stmt: map[string]any{"AndStatement": map[string]any{
				"Statements": []any{
					map[string]any{"GeoMatchStatement": map[string]any{"CountryCodes": []any{"US"}}},
					map[string]any{"LabelMatchStatement": map[string]any{"Scope": "LABEL", "Key": "x"}},
				},
			}},
			want: 1 + 1,
		},
		{
			name: "OrStatement_SumsNested",
			stmt: map[string]any{"OrStatement": map[string]any{
				"Statements": []any{
					map[string]any{"AsnMatchStatement": map[string]any{"AsnList": []any{1}}},
					map[string]any{"AsnMatchStatement": map[string]any{"AsnList": []any{2}}},
				},
			}},
			want: 1 + 1,
		},
		{
			name: "NotStatement_PassesThroughNested",
			stmt: map[string]any{"NotStatement": map[string]any{
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{"CountryCodes": []any{"US"}},
				},
			}},
			want: 1,
		},
		{
			name: "NestedAndOfOr",
			stmt: map[string]any{"AndStatement": map[string]any{
				"Statements": []any{
					map[string]any{"GeoMatchStatement": map[string]any{"CountryCodes": []any{"US"}}},
					map[string]any{"NotStatement": map[string]any{
						"Statement": map[string]any{"OrStatement": map[string]any{
							"Statements": []any{
								map[string]any{"LabelMatchStatement": map[string]any{"Scope": "LABEL", "Key": "a"}},
								map[string]any{"LabelMatchStatement": map[string]any{"Scope": "LABEL", "Key": "b"}},
							},
						}},
					}},
				},
			}},
			want: 1 + (1 + 1), // GeoMatch (1) + NOT(OR(1,1)) == 2
		},
		{
			name: "ManagedRuleGroup_CatalogLookup",
			stmt: map[string]any{"ManagedRuleGroupStatement": map[string]any{
				"VendorName": "AWS",
				"Name":       "AWSManagedRulesCommonRuleSet",
			}},
			want: 700,
		},
		{
			name: "ManagedRuleGroup_UnknownFallsBackToDefault",
			stmt: map[string]any{"ManagedRuleGroupStatement": map[string]any{
				"VendorName": "SomeVendor",
				"Name":       "SomeMarketplaceRuleGroup",
			}},
			want: 700,
		},
		{
			name: "UnknownStatementType_FallsBackToOne",
			stmt: map[string]any{"SomeFutureStatementTypeNotYetModeled": map[string]any{}},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			got := checkCapacityFor(t, h, tt.stmt)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCheckCapacity_RuleGroupReferenceUsesReferencedCapacity verifies
// RuleGroupReferenceStatement costs exactly the referenced RuleGroup's fixed
// Capacity, matching AWS's documented "the cost of using a rule group ... is
// the rule group's capacity setting" -- not the flat 1-WCU-per-rule fallback.
func TestCheckCapacity_RuleGroupReferenceUsesReferencedCapacity(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	rg := &wafv2.RuleGroup{
		ID:       "rg-1",
		Name:     "my-rule-group",
		Scope:    "REGIONAL",
		Capacity: 137,
	}
	wafv2.AddRuleGroupInternal(backend, rg)

	h := wafv2.NewHandler(backend)

	got := checkCapacityFor(t, h, map[string]any{
		"RuleGroupReferenceStatement": map[string]any{"ARN": rg.ARN},
	})
	assert.Equal(t, 137, got)
}

// TestCheckCapacity_RuleGroupReferenceUnknownARNFallsBack verifies a
// RuleGroupReferenceStatement pointing at an ARN this backend doesn't know
// about (e.g. cross-account) falls back to 1 WCU instead of failing the
// whole CheckCapacity call.
func TestCheckCapacity_RuleGroupReferenceUnknownARNFallsBack(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	got := checkCapacityFor(t, h, map[string]any{
		"RuleGroupReferenceStatement": map[string]any{
			"ARN": "arn:aws:wafv2:us-east-1:999999999999:regional/rulegroup/unknown/id",
		},
	})
	assert.Equal(t, 1, got)
}

// TestCheckCapacity_MultipleRulesSum verifies capacities across multiple
// rules in one CheckCapacity call are summed correctly, exercising the
// top-level rules loop rather than a single-rule shortcut.
func TestCheckCapacity_MultipleRulesSum(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CheckCapacity", map[string]any{
		"Scope": "REGIONAL",
		"Rules": []any{
			map[string]any{
				"Name":     "r1",
				"Priority": 0,
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{"CountryCodes": []any{"US"}},
				},
			},
			map[string]any{
				"Name":     "r2",
				"Priority": 1,
				"Statement": map[string]any{
					"XssMatchStatement": map[string]any{
						"FieldToMatch":        map[string]any{"Body": map[string]any{}},
						"TextTransformations": []any{},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	consumed, ok := result["Capacity"].(float64)
	require.True(t, ok)
	assert.InDelta(t, float64(1+40), consumed, 0)
}
