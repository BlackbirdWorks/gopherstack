package iam

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestScope_PrincipalMatching(t *testing.T) {
	t.Parallel()

	const (
		user    = "arn:aws:iam::000000000000:user/alice"
		session = "arn:aws:sts::000000000000:assumed-role/dev/sess"
	)

	tests := []struct {
		name      string
		principal string
		caller    string
		want      EvaluationResult
	}{
		{"star_string", `"*"`, "", EvalAllow},
		{"aws_star", `{"AWS":"*"}`, "", EvalAllow},
		{"exact_user", `{"AWS":"` + user + `"}`, user, EvalAllow},
		{"user_list", `{"AWS":["arn:aws:iam::000000000000:user/x","` + user + `"]}`, user, EvalAllow},
		{"account_id", `{"AWS":"000000000000"}`, user, EvalAllow},
		{"account_root", `{"AWS":"arn:aws:iam::000000000000:root"}`, user, EvalAllow},
		{"role_session", `{"AWS":"arn:aws:iam::000000000000:role/dev"}`, session, EvalAllow},
		{"other_user", `{"AWS":"arn:aws:iam::000000000000:user/eve"}`, user, EvalImplicitDeny},
		{"other_account", `{"AWS":"111111111111"}`, user, EvalImplicitDeny},
		{"named_vs_anonymous", `{"AWS":"` + user + `"}`, "", EvalImplicitDeny},
		{"service", `{"Service":"lambda.amazonaws.com"}`, user, EvalImplicitDeny},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc := `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":` + tt.principal +
				`,"Action":"lambda:InvokeFunctionUrl","Resource":"*"}}`
			ctx := ConditionContext{PrincipalARN: tt.caller, PrincipalAccount: "000000000000"}
			filtered := filterPrincipalDocs([]string{doc}, ctx)

			assert.Equal(t, tt.want, EvaluatePolicies(filtered, "lambda:InvokeFunctionUrl", "arn:x", ctx))
		})
	}
}
