package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestSubstituteVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx  iam.ConditionContext
		name string
		doc  string
		want string
	}{
		{
			name: "no_variables",
			doc:  `{"Effect":"Allow","Action":"s3:*"}`,
			ctx:  iam.ConditionContext{},
			want: `{"Effect":"Allow","Action":"s3:*"}`,
		},
		{
			name: "username_substituted",
			doc:  `{"Resource":"arn:aws:s3:::home/${aws:username}/*"}`,
			ctx:  iam.ConditionContext{Username: "alice"},
			want: `{"Resource":"arn:aws:s3:::home/alice/*"}`,
		},
		{
			name: "userid_substituted",
			doc:  `{"Resource":"arn:aws:iam::000000000000:user/${aws:userid}"}`,
			ctx:  iam.ConditionContext{UserID: "AIDAIOSFODNN7EXAMPLE"},
			want: `{"Resource":"arn:aws:iam::000000000000:user/AIDAIOSFODNN7EXAMPLE"}`,
		},
		{
			name: "sourceip_substituted",
			doc:  `{"Condition":{"IpAddress":{"aws:SourceIp":"${aws:sourceip}"}}}`,
			ctx:  iam.ConditionContext{SourceIP: "10.0.0.1"},
			want: `{"Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.1"}}}`,
		},
		{
			name: "multiple_substitutions",
			doc:  `{"Resource":"arn:aws:s3:::home/${aws:username}/data/${aws:userid}"}`,
			ctx:  iam.ConditionContext{Username: "bob", UserID: "UID123"},
			want: `{"Resource":"arn:aws:s3:::home/bob/data/UID123"}`,
		},
		{
			name: "unknown_variable_preserved",
			doc:  `{"Resource":"${aws:principaltag/team}"}`,
			ctx:  iam.ConditionContext{},
			want: `{"Resource":"${aws:principaltag/team}"}`,
		},
		{
			name: "extra_context_variable",
			doc:  `{"Resource":"${s3:prefix}"}`,
			ctx:  iam.ConditionContext{Extra: map[string]string{"s3:prefix": "home/"}},
			want: `{"Resource":"home/"}`,
		},
		{
			name: "unclosed_variable_preserved",
			doc:  `{"Resource":"${aws:username"}`,
			ctx:  iam.ConditionContext{Username: "alice"},
			want: `{"Resource":"${aws:username"}`,
		},
		{
			name: "case_insensitive_variable",
			doc:  `{"Resource":"arn:aws:s3:::home/${AWS:Username}/*"}`,
			ctx:  iam.ConditionContext{Username: "charlie"},
			want: `{"Resource":"arn:aws:s3:::home/charlie/*"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.SubstituteVariables(tt.doc, tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluatePolicies_VariableSubstitution(t *testing.T) {
	t.Parallel()

	// Policy uses ${aws:username} to restrict access to user's own home directory.
	policy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"arn:aws:s3:::home/${aws:username}/*"
	}]}`

	tests := []struct {
		ctx      iam.ConditionContext
		name     string
		resource string
		want     iam.EvaluationResult
	}{
		{
			name:     "own_home_allowed",
			ctx:      iam.ConditionContext{Username: "alice"},
			resource: "arn:aws:s3:::home/alice/file.txt",
			want:     iam.EvalAllow,
		},
		{
			name:     "other_home_denied",
			ctx:      iam.ConditionContext{Username: "alice"},
			resource: "arn:aws:s3:::home/bob/file.txt",
			want:     iam.EvalImplicitDeny,
		},
		{
			name:     "no_user_denied",
			ctx:      iam.ConditionContext{},
			resource: "arn:aws:s3:::home/alice/file.txt",
			want:     iam.EvalImplicitDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iam.EvaluatePolicies([]string{policy}, "s3:PutObject", tt.resource, tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}
