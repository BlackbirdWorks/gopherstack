package iam_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestEvaluatePolicies_NotAction(t *testing.T) {
	t.Parallel()

	// NotAction: allow any action EXCEPT s3:DeleteObject.
	notActionPolicy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"NotAction":"s3:DeleteObject",
		"Resource":"*"
	}]}`

	tests := []struct {
		name   string
		action string
		want   iam.EvaluationResult
	}{
		{
			name:   "allowed_action_not_in_not_list",
			action: "s3:GetObject",
			want:   iam.EvalAllow,
		},
		{
			name:   "allowed_action_put",
			action: "s3:PutObject",
			want:   iam.EvalAllow,
		},
		{
			name:   "excluded_action_implicit_deny",
			action: "s3:DeleteObject",
			want:   iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{notActionPolicy}, tt.action, "*", iam.ConditionContext{})
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_NotResource(t *testing.T) {
	t.Parallel()

	// NotResource: allow s3:GetObject on everything EXCEPT the logs bucket.
	notResourcePolicy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:GetObject",
		"NotResource":"arn:aws:s3:::logs-bucket/*"
	}]}`

	tests := []struct {
		name     string
		resource string
		want     iam.EvaluationResult
	}{
		{
			name:     "normal_bucket_allowed",
			resource: "arn:aws:s3:::my-bucket/object.txt",
			want:     iam.EvalAllow,
		},
		{
			name:     "logs_bucket_excluded",
			resource: "arn:aws:s3:::logs-bucket/file.log",
			want:     iam.EvalImplicitDeny,
		},
		{
			name:     "star_wildcard_resource_allowed",
			resource: "arn:aws:s3:::other/key",
			want:     iam.EvalAllow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies(
				[]string{notResourcePolicy},
				"s3:GetObject",
				tt.resource,
				iam.ConditionContext{},
			)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_StringEquals(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"StringEquals":{"aws:username":"alice"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "user_matches",
			ctx:  iam.ConditionContext{Username: "alice"},
			want: iam.EvalAllow,
		},
		{
			name: "user_does_not_match",
			ctx:  iam.ConditionContext{Username: "bob"},
			want: iam.EvalImplicitDeny,
		},
		{
			name: "no_user",
			ctx:  iam.ConditionContext{},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_StringNotEquals(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"StringNotEquals":{"aws:username":"admin"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "non_admin_allowed",
			ctx:  iam.ConditionContext{Username: "alice"},
			want: iam.EvalAllow,
		},
		{
			name: "admin_denied",
			ctx:  iam.ConditionContext{Username: "admin"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_StringLike(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"StringLike":{"aws:username":"dev-*"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "prefix_match",
			ctx:  iam.ConditionContext{Username: "dev-alice"},
			want: iam.EvalAllow,
		},
		{
			name: "no_prefix_no_match",
			ctx:  iam.ConditionContext{Username: "prod-alice"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_IpAddress(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "ip_in_cidr",
			ctx:  iam.ConditionContext{SourceIP: "10.1.2.3"},
			want: iam.EvalAllow,
		},
		{
			name: "ip_outside_cidr",
			ctx:  iam.ConditionContext{SourceIP: "192.168.1.1"},
			want: iam.EvalImplicitDeny,
		},
		{
			name: "no_ip",
			ctx:  iam.ConditionContext{},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_NotIpAddress(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Deny",
		"Action":"*",
		"Resource":"*",
		"Condition":{
			"NotIpAddress":{"aws:SourceIp":"10.0.0.0/8"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "external_ip_denied",
			ctx:  iam.ConditionContext{SourceIP: "192.168.1.1"},
			want: iam.EvalExplicitDeny,
		},
		{
			name: "internal_ip_not_denied",
			ctx:  iam.ConditionContext{SourceIP: "10.5.5.5"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestEvaluatePolicies_Conditions_IpAddressIPv6 proves IpAddress supports
// IPv6 CIDR ranges and bare literals, matching AWS's documented IPv6 support.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_IPAddress
//
//nolint:lll // AWS doc URL, cannot be split
func TestEvaluatePolicies_Conditions_IpAddressIPv6(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"IpAddress":{"aws:SourceIp":["2001:DB8:1234:5678::/64","203.0.113.7"]}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "ipv6_in_cidr",
			ctx:  iam.ConditionContext{SourceIP: "2001:db8:1234:5678::1"},
			want: iam.EvalAllow,
		},
		{
			name: "ipv6_outside_cidr",
			ctx:  iam.ConditionContext{SourceIP: "2001:db8:9999::1"},
			want: iam.EvalImplicitDeny,
		},
		{
			name: "ipv4_bare_literal_default_slash32",
			ctx:  iam.ConditionContext{SourceIP: "203.0.113.7"},
			want: iam.EvalAllow,
		},
		{
			name: "ipv4_bare_literal_mismatch",
			ctx:  iam.ConditionContext{SourceIP: "203.0.113.8"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_ArnLike(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"sts:AssumeRole",
		"Resource":"*",
		"Condition":{
			"ArnLike":{"aws:userid":"arn:aws:iam::000000000000:user/*"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "arn_matches",
			ctx:  iam.ConditionContext{UserID: "arn:aws:iam::000000000000:user/alice"},
			want: iam.EvalAllow,
		},
		{
			name: "different_account",
			ctx:  iam.ConditionContext{UserID: "arn:aws:iam::999999999999:user/alice"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "sts:AssumeRole", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestConditionArnSegmentWise proves ArnEquals/ArnLike compare each of the
// six colon-delimited ARN components separately (rather than one wildcard
// glob over the whole string), and that matching is case-sensitive.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_ARN
func TestConditionArnSegmentWise(t *testing.T) {
	t.Parallel()

	const key = "aws:sourcearn"

	tests := []condCase{
		{
			name:     "wildcard_confined_to_last_segment",
			operator: "ArnLike",
			ctxVal:   "arn:aws:sqs:us-east-1:123456789012:my-queue",
			condVal:  "arn:aws:sqs:us-east-1:123456789012:my-*",
			want:     true,
		},
		{
			// A malformed pattern missing a colon must not match by letting
			// '*' span the region+account segment boundary, which a naive
			// single-string glob (pre-fix) would have allowed.
			name:     "wildcard_does_not_span_segments",
			operator: "ArnLike",
			ctxVal:   "arn:aws:s3:us-east-1:123456789012:mybucket",
			condVal:  "arn:aws:s3:*:mybucket",
			want:     false,
		},
		{
			name:     "wildcard_confined_within_one_segment_still_matches",
			operator: "ArnLike",
			ctxVal:   "arn:aws:iam::123456789012:role/prod/deploy",
			condVal:  "arn:aws:iam::123456789012:role*",
			want:     true,
		},
		{
			name:     "region_segment_mismatch_no_match",
			operator: "ArnEquals",
			ctxVal:   "arn:aws:iam::123456789012:role/prod",
			condVal:  "arn:aws:iam:us-east-1:123456789012:role/prod",
			want:     false,
		},
		{
			name:     "differing_segment_count_no_match",
			operator: "ArnEquals",
			ctxVal:   "not-an-arn-at-all",
			condVal:  "arn:aws:iam::123456789012:role/prod",
			want:     false,
		},
		{
			name:     "case_sensitive_no_match",
			operator: "ArnEquals",
			ctxVal:   "arn:aws:iam::123456789012:role/Prod",
			condVal:  "arn:aws:iam::123456789012:role/prod",
			want:     false,
		},
		{
			name:     "arnnotlike_confined_to_segment",
			operator: "ArnNotLike",
			ctxVal:   "arn:aws:sqs:us-east-1:123456789012:my-queue",
			condVal:  "arn:aws:sqs:us-east-1:123456789012:other-*",
			want:     true,
		},
	}

	runCondCases(t, key, tests)
}

func TestEvaluatePolicies_Conditions_Bool(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"Bool":{"aws:SecureTransport":"true"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "secure_transport_matches",
			ctx: iam.ConditionContext{
				Extra: map[string]string{"aws:securetransport": "true"},
			},
			want: iam.EvalAllow,
		},
		{
			name: "non_secure",
			ctx: iam.ConditionContext{
				Extra: map[string]string{"aws:securetransport": "false"},
			},
			want: iam.EvalImplicitDeny,
		},
		{
			// SecureTransport is a dedicated ConditionContext field (populated
			// by the enforcement middleware from the request's TLS state),
			// not just an Extra entry.
			name: "secure_transport_dedicated_field",
			ctx:  iam.ConditionContext{SecureTransport: "true"},
			want: iam.EvalAllow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_Null(t *testing.T) {
	t.Parallel()

	// Null: "true" means key must be absent; "false" means key must be present.
	policyKeyAbsent := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{"Null":{"aws:username":"true"}}
	}]}`

	policyKeyPresent := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{"Null":{"aws:username":"false"}}
	}]}`

	tests := []struct {
		ctx    iam.ConditionContext
		name   string
		policy string
		want   iam.EvaluationResult
	}{
		{
			name:   "key_absent_with_null_true",
			policy: policyKeyAbsent,
			ctx:    iam.ConditionContext{},
			want:   iam.EvalAllow,
		},
		{
			name:   "key_present_with_null_true",
			policy: policyKeyAbsent,
			ctx:    iam.ConditionContext{Username: "alice"},
			want:   iam.EvalImplicitDeny,
		},
		{
			name:   "key_present_with_null_false",
			policy: policyKeyPresent,
			ctx:    iam.ConditionContext{Username: "alice"},
			want:   iam.EvalAllow,
		},
		{
			name:   "key_absent_with_null_false",
			policy: policyKeyPresent,
			ctx:    iam.ConditionContext{},
			want:   iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{tt.policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestEvaluatePolicies_Conditions_NullIfExistsUnrecognized proves "NullIfExists"
// is not treated as a stripped-suffix alias for Null: AWS documents IfExists
// as valid on any operator except Null (Null already tests key presence), so
// gopherstack's evaluator must not silently accept the combination.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_IfExists
//
//nolint:lll // AWS doc URL, cannot be split
func TestEvaluatePolicies_Conditions_NullIfExistsUnrecognized(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{"NullIfExists":{"aws:username":"true"}}
	}]}`

	got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", iam.ConditionContext{})
	assert.Equal(t, iam.EvalImplicitDeny, got, "an unrecognized operator must not match")
}

func TestEvaluatePolicies_Conditions_IfExists(t *testing.T) {
	t.Parallel()

	// StringEqualsIfExists: passes when key is absent.
	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"StringEqualsIfExists":{"aws:username":"alice"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "key_absent_passes",
			ctx:  iam.ConditionContext{},
			want: iam.EvalAllow,
		},
		{
			name: "key_matches",
			ctx:  iam.ConditionContext{Username: "alice"},
			want: iam.EvalAllow,
		},
		{
			name: "key_does_not_match",
			ctx:  iam.ConditionContext{Username: "bob"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_MultipleConditions(t *testing.T) {
	t.Parallel()

	// AND: user must be alice AND ip must be in 10.0.0.0/8.
	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"StringEquals":{"aws:username":"alice"},
			"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "both_match",
			ctx:  iam.ConditionContext{Username: "alice", SourceIP: "10.5.5.5"},
			want: iam.EvalAllow,
		},
		{
			name: "wrong_user",
			ctx:  iam.ConditionContext{Username: "bob", SourceIP: "10.5.5.5"},
			want: iam.EvalImplicitDeny,
		},
		{
			name: "wrong_ip",
			ctx:  iam.ConditionContext{Username: "alice", SourceIP: "1.2.3.4"},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:GetObject", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_Conditions_ExtraContextKey(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:ListBucket",
		"Resource":"*",
		"Condition":{
			"StringEquals":{"s3:prefix":"home/"}
		}
	}]}`

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		want iam.EvaluationResult
	}{
		{
			name: "prefix_matches",
			ctx:  iam.ConditionContext{Extra: map[string]string{"s3:prefix": "home/"}},
			want: iam.EvalAllow,
		},
		{
			name: "prefix_no_match",
			ctx:  iam.ConditionContext{Extra: map[string]string{"s3:prefix": "work/"}},
			want: iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:ListBucket", "*", tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

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
		// AWS accepts epoch (UNIX) seconds interchangeably with ISO 8601:
		//nolint:lll // AWS doc URL, cannot be split
		// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_Date
		{name: "epoch_ctx_matches_iso_cond", operator: "DateEquals", ctxVal: "1686830400", condVal: mid, want: true},
		{name: "iso_ctx_matches_epoch_cond", operator: "DateEquals", ctxVal: mid, condVal: "1686830400", want: true},
		{name: "epoch_both_sides", operator: "DateLessThan", ctxVal: "1672531200", condVal: "1686830400", want: true},
		{
			name:     "epoch_fractional_seconds",
			operator: "DateEquals",
			ctxVal:   "1686830400.000",
			condVal:  mid,
			want:     true,
		},
		{name: "date_only_iso", operator: "DateLessThan", ctxVal: "2023-01-01", condVal: mid, want: true},
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
