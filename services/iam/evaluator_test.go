package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestEvaluatePolicies_WildcardPatterns(t *testing.T) {
	t.Parallel()

	// Helper: wrap an action into a simple Allow policy.
	allowPolicy := func(action string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"` + action + `","Resource":"*"}]}`
	}

	tests := []struct {
		name         string
		policyAction string
		testAction   string
		want         iam.EvaluationResult
	}{
		{name: "exact_match", policyAction: "s3:PutObject", testAction: "s3:PutObject", want: iam.EvalAllow},
		{name: "star_wildcard", policyAction: "*", testAction: "s3:PutObject", want: iam.EvalAllow},
		{name: "service_wildcard", policyAction: "s3:*", testAction: "s3:PutObject", want: iam.EvalAllow},
		{
			name:         "service_wildcard_no_match",
			policyAction: "s3:*",
			testAction:   "ec2:RunInstances",
			want:         iam.EvalImplicitDeny,
		},
		{name: "prefix_wildcard", policyAction: "s3:Get*", testAction: "s3:GetObject", want: iam.EvalAllow},
		{
			name:         "prefix_wildcard_no_match",
			policyAction: "s3:Get*",
			testAction:   "s3:PutObject",
			want:         iam.EvalImplicitDeny,
		},
		{name: "question_mark", policyAction: "s3:GetObjec?", testAction: "s3:GetObject", want: iam.EvalAllow},
		{
			name:         "question_mark_no_match",
			policyAction: "s3:GetObjec?",
			testAction:   "s3:GetObjects",
			want:         iam.EvalImplicitDeny,
		},
		{name: "multi_wildcard", policyAction: "s3:*Object*", testAction: "s3:GetObjectAcl", want: iam.EvalAllow},
		{name: "case_insensitive", policyAction: "S3:PUTOBJECT", testAction: "s3:PutObject", want: iam.EvalAllow},
		{name: "case_insensitive_wildcard", policyAction: "S3:*", testAction: "s3:GetObject", want: iam.EvalAllow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies(
				[]string{allowPolicy(tt.policyAction)},
				tt.testAction,
				"*",
				iam.ConditionContext{},
			)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies(t *testing.T) {
	t.Parallel()

	allowS3All := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	allowSpecific :=
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	denyAll := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`
	denyS3 := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}`
	allowMultiple := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":["s3:GetObject","s3:PutObject"],"Resource":"*"}]}`
	allowWithResource := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"arn:aws:s3:::my-bucket/*"}]}`

	tests := []struct {
		name       string
		action     string
		resource   string
		policyDocs []string
		want       iam.EvaluationResult
	}{
		{
			name:       "no_policies",
			policyDocs: []string{},
			action:     "s3:GetObject",
			resource:   "*",
			want:       iam.EvalImplicitDeny,
		},
		{
			name:       "allow_wildcard_action",
			policyDocs: []string{allowS3All},
			action:     "s3:GetObject",
			resource:   "*",
			want:       iam.EvalAllow,
		},
		{
			name:       "allow_specific_action",
			policyDocs: []string{allowSpecific},
			action:     "s3:GetObject",
			resource:   "*",
			want:       iam.EvalAllow,
		},
		{
			name:       "allow_specific_action_no_match",
			policyDocs: []string{allowSpecific},
			action:     "s3:PutObject",
			resource:   "*",
			want:       iam.EvalImplicitDeny,
		},
		{
			name:       "explicit_deny_overrides_allow",
			policyDocs: []string{allowS3All, denyAll},
			action:     "s3:GetObject",
			resource:   "*",
			want:       iam.EvalExplicitDeny,
		},
		{
			name:       "deny_s3_overrides_allow",
			policyDocs: []string{allowS3All, denyS3},
			action:     "s3:PutObject",
			resource:   "*",
			want:       iam.EvalExplicitDeny,
		},
		{
			name:       "deny_does_not_match_different_service",
			policyDocs: []string{allowS3All, denyS3},
			action:     "dynamodb:GetItem",
			resource:   "*",
			want:       iam.EvalImplicitDeny,
		},
		{
			name:       "allow_multiple_actions",
			policyDocs: []string{allowMultiple},
			action:     "s3:PutObject",
			resource:   "*",
			want:       iam.EvalAllow,
		},
		{
			name:       "allow_with_resource_match",
			policyDocs: []string{allowWithResource},
			action:     "s3:GetObject",
			resource:   "arn:aws:s3:::my-bucket/key",
			want:       iam.EvalAllow,
		},
		{
			name:       "allow_with_resource_no_match",
			policyDocs: []string{allowWithResource},
			action:     "s3:GetObject",
			resource:   "arn:aws:s3:::other-bucket/key",
			want:       iam.EvalImplicitDeny,
		},
		{
			name:       "invalid_json_skipped",
			policyDocs: []string{"not-json", allowS3All},
			action:     "s3:GetObject",
			resource:   "*",
			want:       iam.EvalAllow,
		},
		{
			name:       "case_insensitive_action",
			policyDocs: []string{allowS3All},
			action:     "S3:GETOBJECT",
			resource:   "*",
			want:       iam.EvalAllow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies(tt.policyDocs, tt.action, tt.resource, iam.ConditionContext{})
			assert.Equal(t, tt.want, got)
		})
	}
}
