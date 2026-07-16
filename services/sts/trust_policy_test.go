package sts_test

import (
	"errors"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestEvaluateAssumeRoleTrust_Principal exercises the trust-policy Principal,
// Action, Effect, and Condition evaluation for sts:AssumeRole.
func TestEvaluateAssumeRoleTrust_Principal(t *testing.T) {
	t.Parallel()

	const (
		caller      = "arn:aws:sts::123456789012:assumed-role/AppRole/session"
		callerUser  = "arn:aws:iam::123456789012:user/alice"
		otherCaller = "arn:aws:iam::999999999999:user/mallory"
	)

	allowPrincipal := func(principal string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":` + principal + `},"Action":"sts:AssumeRole"}]}`
	}

	tests := []struct {
		name    string
		policy  string
		ev      sts.TrustEvalForTest
		wantErr bool
	}{
		{
			name:    "no_principal_context_is_permissive",
			policy:  allowPrincipal(`"arn:aws:iam::123456789012:user/only"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole},
			wantErr: false,
		},
		{
			name:    "empty_policy_permissive",
			policy:  "",
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller},
			wantErr: false,
		},
		{
			name:    "malformed_policy_permissive",
			policy:  "not-json",
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller},
			wantErr: false,
		},
		{
			name:    "exact_arn_allow",
			policy:  allowPrincipal(`"` + callerUser + `"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: false,
		},
		{
			name:    "exact_arn_mismatch_denied",
			policy:  allowPrincipal(`"` + callerUser + `"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: otherCaller},
			wantErr: true,
		},
		{
			name:    "principal_string_wildcard_allow",
			policy:  `{"Statement":{"Effect":"Allow","Principal":"*","Action":"sts:AssumeRole"}}`,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: otherCaller},
			wantErr: false,
		},
		{
			name:    "principal_aws_wildcard_allow",
			policy:  allowPrincipal(`"*"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: otherCaller},
			wantErr: false,
		},
		{
			name:    "account_root_same_account_allow",
			policy:  allowPrincipal(`"arn:aws:iam::123456789012:root"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller},
			wantErr: false,
		},
		{
			name:    "account_root_other_account_denied",
			policy:  allowPrincipal(`"arn:aws:iam::123456789012:root"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: otherCaller},
			wantErr: true,
		},
		{
			name:    "bare_account_id_allow",
			policy:  allowPrincipal(`"123456789012"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: false,
		},
		{
			name:    "role_arn_matches_assumed_role_caller",
			policy:  allowPrincipal(`"arn:aws:iam::123456789012:role/AppRole"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller},
			wantErr: false,
		},
		{
			name:    "role_arn_does_not_match_plain_user_caller",
			policy:  allowPrincipal(`"arn:aws:iam::123456789012:role/AppRole"`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: true,
		},
		{
			name:    "principal_array_any_match_allow",
			policy:  allowPrincipal(`["` + otherCaller + `","` + callerUser + `"]`),
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: false,
		},
		{
			name: "explicit_deny_wins_over_allow",
			policy: `{"Statement":[` +
				`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"},` +
				`{"Effect":"Deny","Principal":{"AWS":"` + callerUser + `"},"Action":"sts:AssumeRole"}]}`,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: true,
		},
		{
			name:    "action_mismatch_denied",
			policy:  `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithSAML"}]}`,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: true,
		},
		{
			name:    "action_wildcard_allow",
			policy:  `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:*"}]}`,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: callerUser},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := sts.EvaluateAssumeRoleTrust(tt.policy, tt.ev)
			if tt.wantErr {
				if !errors.Is(err, sts.ErrAccessDenied) {
					t.Fatalf("want ErrAccessDenied, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

// TestEvaluateAssumeRoleTrust_Conditions exercises condition-key evaluation
// (sts:ExternalId, aws:PrincipalArn) alongside a matching Principal.
func TestEvaluateAssumeRoleTrust_Conditions(t *testing.T) {
	t.Parallel()

	const caller = "arn:aws:iam::123456789012:user/alice"

	externalIDPolicy := `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"secret"}}}]}`

	principalArnPolicy := `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"sts:AssumeRole","Condition":{"StringLike":` +
		`{"aws:PrincipalArn":"arn:aws:iam::123456789012:user/*"}}}]}`

	tests := []struct {
		name    string
		policy  string
		ev      sts.TrustEvalForTest
		wantErr bool
	}{
		{
			name:    "external_id_match",
			policy:  externalIDPolicy,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller, ExternalID: "secret"},
			wantErr: false,
		},
		{
			name:    "external_id_mismatch_denied",
			policy:  externalIDPolicy,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller, ExternalID: "wrong"},
			wantErr: true,
		},
		{
			name:    "external_id_missing_denied",
			policy:  externalIDPolicy,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller},
			wantErr: true,
		},
		{
			name:   "principal_arn_stringlike_match",
			policy: principalArnPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRole,
				CallerArn:    caller,
				ConditionCtx: map[string]string{"aws:principalarn": caller},
			},
			wantErr: false,
		},
		{
			name:   "principal_arn_stringlike_mismatch_denied",
			policy: principalArnPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRole,
				CallerArn:    "arn:aws:iam::123456789012:role/svc",
				ConditionCtx: map[string]string{"aws:principalarn": "arn:aws:iam::123456789012:role/svc"},
			},
			wantErr: true,
		},
		{
			name: "unmodelled_condition_key_ignored",
			policy: `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole",` +
				`"Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}}]}`,
			ev:      sts.TrustEvalForTest{Action: sts.ActionAssumeRole, CallerArn: caller},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := sts.EvaluateAssumeRoleTrust(tt.policy, tt.ev)
			if tt.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v got err=%v", tt.wantErr, err)
			}

			if tt.wantErr && !errors.Is(err, sts.ErrAccessDenied) {
				t.Fatalf("want ErrAccessDenied, got %v", err)
			}
		})
	}
}

// TestEvaluateAssumeRoleTrust_Federated exercises Federated principal matching
// for SAML providers and OIDC issuers.
func TestEvaluateAssumeRoleTrust_Federated(t *testing.T) {
	t.Parallel()

	samlPolicy := `{"Statement":[{"Effect":"Allow",` +
		`"Principal":{"Federated":"arn:aws:iam::123456789012:saml-provider/Okta"},` +
		`"Action":"sts:AssumeRoleWithSAML"}]}`

	oidcPolicy := `{"Statement":[{"Effect":"Allow",` +
		`"Principal":{"Federated":"arn:aws:iam::123456789012:oidc-provider/accounts.google.com"},` +
		`"Action":"sts:AssumeRoleWithWebIdentity"}]}`

	tests := []struct {
		name    string
		policy  string
		ev      sts.TrustEvalForTest
		wantErr bool
	}{
		{
			name:   "saml_provider_match",
			policy: samlPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRoleWithSAML,
				FederatedArn: "arn:aws:iam::123456789012:saml-provider/Okta",
			},
			wantErr: false,
		},
		{
			name:   "saml_provider_mismatch_denied",
			policy: samlPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRoleWithSAML,
				FederatedArn: "arn:aws:iam::123456789012:saml-provider/Other",
			},
			wantErr: true,
		},
		{
			name:   "oidc_issuer_via_provider_arn_match",
			policy: oidcPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRoleWithWebID,
				FederatedArn: "accounts.google.com",
			},
			wantErr: false,
		},
		{
			name:   "oidc_issuer_with_scheme_match",
			policy: oidcPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRoleWithWebID,
				FederatedArn: "https://accounts.google.com",
			},
			wantErr: false,
		},
		{
			name:   "oidc_issuer_mismatch_denied",
			policy: oidcPolicy,
			ev: sts.TrustEvalForTest{
				Action:       sts.ActionAssumeRoleWithWebID,
				FederatedArn: "evil.example.com",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := sts.EvaluateAssumeRoleTrust(tt.policy, tt.ev)
			if tt.wantErr != (err != nil) {
				t.Fatalf("wantErr=%v got err=%v", tt.wantErr, err)
			}
		})
	}
}

// TestWildcardMatch exercises the glob matcher used for action and StringLike
// condition matching.
func TestWildcardMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		input   string
		want    bool
	}{
		{name: "exact", pattern: "sts:assumerole", input: "sts:assumerole", want: true},
		{name: "trailing_star", pattern: "sts:*", input: "sts:assumerole", want: true},
		{name: "leading_star", pattern: "*role", input: "assumerole", want: true},
		{name: "middle_star", pattern: "arn:*:user/*", input: "arn:aws:user/alice", want: true},
		{name: "question_single", pattern: "a?c", input: "abc", want: true},
		{name: "question_too_long", pattern: "a?c", input: "abbc", want: false},
		{name: "no_match", pattern: "sts:*", input: "iam:listroles", want: false},
		{name: "star_matches_empty", pattern: "abc*", input: "abc", want: true},
		{
			name:    "prefix_mismatch",
			pattern: "arn:aws:iam::1:user/*",
			input:   "arn:aws:iam::2:user/x",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := sts.WildcardMatch(tt.pattern, tt.input); got != tt.want {
				t.Fatalf("WildcardMatch(%q,%q)=%v want %v", tt.pattern, tt.input, got, tt.want)
			}
		})
	}
}
