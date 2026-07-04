package iam_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// condPolicy builds a single-statement Allow policy whose only gate is the
// supplied Condition block, so EvaluatePolicies returns Allow exactly when the
// condition is satisfied.
func condPolicy(operator, key string, values any) string {
	stmt := map[string]any{
		"Effect":   "Allow",
		"Action":   "svc:Do",
		"Resource": "*",
		"Condition": map[string]any{
			operator: map[string]any{key: values},
		},
	}
	doc := map[string]any{
		"Version":   "2012-10-17",
		"Statement": []any{stmt},
	}

	return mustJSON(doc)
}

func evalCond(operator, key string, values any, ctx iam.ConditionContext) bool {
	res := iam.EvaluatePolicies([]string{condPolicy(operator, key, values)}, "svc:Do", "*", ctx)

	return res == iam.EvalAllow
}

func extraCtx(key, val string) iam.ConditionContext {
	return iam.ConditionContext{Extra: map[string]string{key: val}}
}

// condCase is the shared table row for operator tests. Fields are ordered for
// govet fieldalignment (interface first, then strings, then bool).
type condCase struct {
	condVal  any
	name     string
	operator string
	ctxVal   string
	want     bool
}

func TestConditionNumericOperators(t *testing.T) {
	t.Parallel()

	const key = "svc:count"

	tests := []condCase{
		{name: "equals_match", operator: "NumericEquals", ctxVal: "10", condVal: "10", want: true},
		{name: "equals_nomatch", operator: "NumericEquals", ctxVal: "10", condVal: "11", want: false},
		{name: "notequals_match", operator: "NumericNotEquals", ctxVal: "10", condVal: "11", want: true},
		{name: "notequals_nomatch", operator: "NumericNotEquals", ctxVal: "10", condVal: "10", want: false},
		{name: "lessthan_true", operator: "NumericLessThan", ctxVal: "5", condVal: "10", want: true},
		{name: "lessthan_false_equal", operator: "NumericLessThan", ctxVal: "10", condVal: "10", want: false},
		{name: "lessthanequals_true", operator: "NumericLessThanEquals", ctxVal: "10", condVal: "10", want: true},
		{name: "lessthanequals_false", operator: "NumericLessThanEquals", ctxVal: "11", condVal: "10", want: false},
		{name: "greaterthan_true", operator: "NumericGreaterThan", ctxVal: "11", condVal: "10", want: true},
		{name: "greaterthan_false_equal", operator: "NumericGreaterThan", ctxVal: "10", condVal: "10", want: false},
		{name: "greaterthanequals_true", operator: "NumericGreaterThanEquals", ctxVal: "10", condVal: "10", want: true},
		{
			name:     "greaterthanequals_false",
			operator: "NumericGreaterThanEquals",
			ctxVal:   "9",
			condVal:  "10",
			want:     false,
		},
		{name: "float_match", operator: "NumericEquals", ctxVal: "3.14", condVal: "3.14", want: true},
		{name: "negative_lessthan", operator: "NumericLessThan", ctxVal: "-5", condVal: "0", want: true},
		{name: "any_of_list", operator: "NumericEquals", ctxVal: "7", condVal: []any{"5", "6", "7"}, want: true},
		{name: "none_of_list", operator: "NumericEquals", ctxVal: "9", condVal: []any{"5", "6", "7"}, want: false},
		{name: "invalid_ctx_number", operator: "NumericEquals", ctxVal: "abc", condVal: "10", want: false},
		{
			name:     "invalid_cond_number_skipped",
			operator: "NumericEquals",
			ctxVal:   "10",
			condVal:  []any{"xx", "10"},
			want:     true,
		},
	}

	runCondCases(t, key, tests)
}

func TestConditionDateOperators(t *testing.T) {
	t.Parallel()

	const key = "aws:currenttime"

	const (
		early = "2023-01-01T00:00:00Z"
		mid   = "2023-06-15T12:00:00Z"
		late  = "2023-12-31T23:59:59Z"
	)

	tests := []condCase{
		{name: "equals_match", operator: "DateEquals", ctxVal: mid, condVal: mid, want: true},
		{name: "equals_nomatch", operator: "DateEquals", ctxVal: mid, condVal: late, want: false},
		{name: "notequals_match", operator: "DateNotEquals", ctxVal: mid, condVal: late, want: true},
		{name: "notequals_nomatch", operator: "DateNotEquals", ctxVal: mid, condVal: mid, want: false},
		{name: "lessthan_true", operator: "DateLessThan", ctxVal: early, condVal: mid, want: true},
		{name: "lessthan_false", operator: "DateLessThan", ctxVal: late, condVal: mid, want: false},
		{name: "lessthan_false_equal", operator: "DateLessThan", ctxVal: mid, condVal: mid, want: false},
		{name: "lessthanequals_equal", operator: "DateLessThanEquals", ctxVal: mid, condVal: mid, want: true},
		{name: "lessthanequals_true", operator: "DateLessThanEquals", ctxVal: early, condVal: mid, want: true},
		{name: "lessthanequals_false", operator: "DateLessThanEquals", ctxVal: late, condVal: mid, want: false},
		{name: "greaterthan_true", operator: "DateGreaterThan", ctxVal: late, condVal: mid, want: true},
		{name: "greaterthan_false", operator: "DateGreaterThan", ctxVal: early, condVal: mid, want: false},
		{name: "greaterthanequals_equal", operator: "DateGreaterThanEquals", ctxVal: mid, condVal: mid, want: true},
		{name: "greaterthanequals_true", operator: "DateGreaterThanEquals", ctxVal: late, condVal: mid, want: true},
		{name: "greaterthanequals_false", operator: "DateGreaterThanEquals", ctxVal: early, condVal: mid, want: false},
		{name: "invalid_ctx_date", operator: "DateLessThan", ctxVal: "not-a-date", condVal: mid, want: false},
		{
			name:     "invalid_cond_date_skipped",
			operator: "DateEquals",
			ctxVal:   mid,
			condVal:  []any{"bad", mid},
			want:     true,
		},
	}

	runCondCases(t, key, tests)
}

func TestConditionBinaryOperator(t *testing.T) {
	t.Parallel()

	const key = "svc:blob"

	hello := base64.StdEncoding.EncodeToString([]byte("hello"))
	world := base64.StdEncoding.EncodeToString([]byte("world"))

	tests := []condCase{
		{name: "match", operator: "BinaryEquals", ctxVal: hello, condVal: hello, want: true},
		{name: "nomatch", operator: "BinaryEquals", ctxVal: hello, condVal: world, want: false},
		{name: "any_of_list", operator: "BinaryEquals", ctxVal: hello, condVal: []any{world, hello}, want: true},
		{name: "none_of_list", operator: "BinaryEquals", ctxVal: hello, condVal: []any{world}, want: false},
		{name: "invalid_ctx_b64", operator: "BinaryEquals", ctxVal: "!!!notb64!!!", condVal: hello, want: false},
	}

	runCondCases(t, key, tests)
}

func TestConditionSetQualifiers(t *testing.T) {
	t.Parallel()

	const key = "svc:tags"

	tests := []condCase{
		// ForAllValues: every context value must match.
		{
			name:     "forall_all_match",
			operator: "ForAllValues:StringEquals",
			ctxVal:   "a,b",
			condVal:  []any{"a", "b", "c"},
			want:     true,
		},
		{
			name:     "forall_one_outside",
			operator: "ForAllValues:StringEquals",
			ctxVal:   "a,z",
			condVal:  []any{"a", "b", "c"},
			want:     false,
		},
		{
			name:     "forall_single_match",
			operator: "ForAllValues:StringEquals",
			ctxVal:   "b",
			condVal:  []any{"a", "b"},
			want:     true,
		},
		{
			name:     "forall_empty_set_vacuous",
			operator: "ForAllValues:StringEquals",
			ctxVal:   "",
			condVal:  []any{"a"},
			want:     true,
		},
		{
			name:     "forall_numeric_all_in_range",
			operator: "ForAllValues:NumericLessThan",
			ctxVal:   "1,2,3",
			condVal:  "10",
			want:     true,
		},
		{
			name:     "forall_numeric_one_out",
			operator: "ForAllValues:NumericLessThan",
			ctxVal:   "1,20",
			condVal:  "10",
			want:     false,
		},
		{
			name:     "forall_stringlike_all",
			operator: "ForAllValues:StringLike",
			ctxVal:   "img-1,img-2",
			condVal:  "img-*",
			want:     true,
		},
		// ForAnyValue: at least one context value must match.
		{
			name:     "forany_one_match",
			operator: "ForAnyValue:StringEquals",
			ctxVal:   "z,b",
			condVal:  []any{"a", "b"},
			want:     true,
		},
		{
			name:     "forany_none_match",
			operator: "ForAnyValue:StringEquals",
			ctxVal:   "y,z",
			condVal:  []any{"a", "b"},
			want:     false,
		},
		{
			name:     "forany_empty_set_false",
			operator: "ForAnyValue:StringEquals",
			ctxVal:   "",
			condVal:  []any{"a"},
			want:     false,
		},
		{
			name:     "forany_numeric_one_in",
			operator: "ForAnyValue:NumericEquals",
			ctxVal:   "1,5,9",
			condVal:  "5",
			want:     true,
		},
		{
			name:     "forany_numeric_none",
			operator: "ForAnyValue:NumericEquals",
			ctxVal:   "1,2,3",
			condVal:  "5",
			want:     false,
		},
		{
			name:     "forany_stringlike_one",
			operator: "ForAnyValue:StringLike",
			ctxVal:   "doc-1,img-2",
			condVal:  "img-*",
			want:     true,
		},
	}

	runCondCases(t, key, tests)
}

// runCondCases executes a table of condCase rows against the given context key.
func runCondCases(t *testing.T, key string, tests []condCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := evalCond(tt.operator, key, tt.condVal, extraCtx(key, tt.ctxVal))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConditionSetQualifierIfExists(t *testing.T) {
	t.Parallel()

	const key = "svc:missing"

	// With no context value present, IfExists makes the condition pass.
	got := evalCond("ForAllValues:StringEqualsIfExists", key, []any{"a"}, iam.ConditionContext{})
	assert.True(t, got, "IfExists should pass when the key is absent")

	// Without IfExists, ForAnyValue over an empty set is false.
	got = evalCond("ForAnyValue:StringEquals", key, []any{"a"}, iam.ConditionContext{})
	assert.False(t, got, "ForAnyValue over an empty set is false")
}

func TestConditionPrincipalTagKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		condVal any
		tags    map[string]string
		name    string
		want    bool
	}{
		{name: "match", tags: map[string]string{"team": "blue"}, condVal: "blue", want: true},
		{name: "nomatch", tags: map[string]string{"team": "blue"}, condVal: "red", want: false},
		{name: "absent_tag", tags: map[string]string{"other": "x"}, condVal: "blue", want: false},
		{name: "case_insensitive_key", tags: map[string]string{"Team": "blue"}, condVal: "blue", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := iam.ConditionContext{PrincipalTags: tt.tags}
			got := evalCond("StringEquals", "aws:PrincipalTag/team", tt.condVal, ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConditionRequestTagKey(t *testing.T) {
	t.Parallel()

	ctx := iam.ConditionContext{RequestTags: map[string]string{"env": "prod"}}

	assert.True(t, evalCond("StringEquals", "aws:RequestTag/env", "prod", ctx))
	assert.False(t, evalCond("StringEquals", "aws:RequestTag/env", "dev", ctx))
}

func TestConditionTagKeyFallsThroughToExtra(t *testing.T) {
	t.Parallel()

	// When the tag is not in PrincipalTags, a simulated ContextEntry supplied
	// through Extra must still resolve (SimulatePolicy path).
	ctx := extraCtx("aws:principaltag/team", "green")
	assert.True(t, evalCond("StringEquals", "aws:PrincipalTag/team", "green", ctx))
}

// mustJSON marshals v to a compact JSON string, panicking on error (test-only).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal policy: " + err.Error())
	}

	return string(b)
}
