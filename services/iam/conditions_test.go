package iam_test

import (
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
