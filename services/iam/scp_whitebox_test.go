package iam

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func scpDoc(effect, action string) SCPPolicy {
	return SCPPolicy{
		ARN: "arn:aws:organizations::1:policy/o-x/service_control_policy/p-" + effect,
		Document: `{"Version":"2012-10-17","Statement":[{"Effect":"` + effect +
			`","Action":"` + action + `","Resource":"*"}]}`,
	}
}

func TestEvaluateSCPs(t *testing.T) {
	t.Parallel()

	allowAll := scpDoc("Allow", "*")

	tests := []struct {
		name         string
		wantArn      string
		levels       []SCPLevel
		wantDenied   bool
		wantExplicit bool
	}{
		{
			name:   "all_levels_allow",
			levels: []SCPLevel{{Policies: []SCPPolicy{allowAll}}, {Policies: []SCPPolicy{allowAll}}},
		},
		{
			name:       "level_without_allow_denies",
			levels:     []SCPLevel{{Policies: []SCPPolicy{allowAll}}, {}},
			wantDenied: true,
		},
		{
			name: "level_allow_for_other_action_denies",
			levels: []SCPLevel{
				{Policies: []SCPPolicy{allowAll}},
				{Policies: []SCPPolicy{scpDoc("Allow", "ec2:*")}},
			},
			wantDenied: true,
		},
		{
			name: "deny_wins_over_allow_at_other_level",
			levels: []SCPLevel{
				{Policies: []SCPPolicy{allowAll, scpDoc("Deny", "s3:DeleteBucket")}},
				{Policies: []SCPPolicy{allowAll}},
			},
			wantDenied:   true,
			wantExplicit: true,
			wantArn:      "arn:aws:organizations::1:policy/o-x/service_control_policy/p-Deny",
		},
		{
			name:   "union_of_allows_within_level",
			levels: []SCPLevel{{Policies: []SCPPolicy{scpDoc("Allow", "ec2:*"), scpDoc("Allow", "s3:*")}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := evaluateSCPs(tt.levels, "s3:DeleteBucket", "*", ConditionContext{})
			assert.Equal(t, tt.wantDenied, v.denied)
			assert.Equal(t, tt.wantExplicit, v.explicit)
			assert.Equal(t, tt.wantArn, v.policyArn)
		})
	}
}

func TestSCPExemptPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want bool
	}{
		{"user", "arn:aws:iam::111122223333:user/alice", false},
		{"role", "arn:aws:iam::111122223333:role/app", false},
		{"slr_path", "arn:aws:iam::111122223333:role/aws-service-role/ecs.amazonaws.com/AWSServiceRoleForECS", true},
		{"slr_assumed", "arn:aws:sts::111122223333:assumed-role/AWSServiceRoleForECS/sess", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, scpExemptPrincipal(tt.arn))
		})
	}
}

func TestSCPDeniedMessage(t *testing.T) {
	t.Parallel()

	const user = "arn:aws:iam::123456789012:user/John"

	tests := []struct {
		name string
		want string
		v    scpVerdict
	}{
		{
			"implicit", "User: " + user + " is not authorized to perform: codecommit:ListRepositories " +
				"because no service control policy allows the codecommit:ListRepositories action",
			scpVerdict{denied: true},
		},
		{
			"explicit_no_arn", "User: " + user + " is not authorized to perform: codecommit:ListRepositories " +
				"with an explicit deny in a service control policy",
			scpVerdict{denied: true, explicit: true},
		},
		{
			"explicit_arn", "User: " + user + " is not authorized to perform: codecommit:ListRepositories " +
				"with an explicit deny in a service control policy: arn:p",
			scpVerdict{denied: true, explicit: true, policyArn: "arn:p"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, scpDeniedMessage(user, "codecommit:ListRepositories", tt.v))
		})
	}
}
