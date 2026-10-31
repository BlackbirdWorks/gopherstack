package organizations_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

type scpOrg struct {
	b       *organizations.InMemoryBackend
	rootID  string
	ouID    string
	childID string
	acctID  string
}

func newSCPOrg(t *testing.T) scpOrg {
	t.Helper()

	b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	_, root, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	ou, err := b.CreateOrganizationalUnit(root.ID, "prod", nil)
	require.NoError(t, err)

	child, err := b.CreateOrganizationalUnit(ou.ID, "web", nil)
	require.NoError(t, err)

	st, err := b.CreateAccount("member", "m@example.com", "", "", nil)
	require.NoError(t, err)
	require.NoError(t, b.MoveAccount(st.AccountID, root.ID, child.ID))

	return scpOrg{b: b, rootID: root.ID, ouID: ou.ID, childID: child.ID, acctID: st.AccountID}
}

func TestSCPsForAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(t *testing.T, o scpOrg) string
		name       string
		wantTarget []string
		wantOK     bool
	}{
		{
			name:   "member_path_root_first",
			mutate: func(_ *testing.T, o scpOrg) string { return o.acctID },
			wantOK: true,
		},
		{
			name:   "management_account_exempt",
			mutate: func(_ *testing.T, _ scpOrg) string { return "000000000000" },
		},
		{
			name:   "unknown_account",
			mutate: func(_ *testing.T, _ scpOrg) string { return "999999999999" },
		},
		{
			name: "type_disabled",
			mutate: func(t *testing.T, o scpOrg) string {
				t.Helper()

				_, err := o.b.DisablePolicyType(o.rootID, "SERVICE_CONTROL_POLICY")
				require.NoError(t, err)

				return o.acctID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := newSCPOrg(t)
			acct := tt.mutate(t, o)

			levels, ok := o.b.SCPsForAccount(acct)
			require.Equal(t, tt.wantOK, ok)

			if !tt.wantOK {
				return
			}

			require.Len(t, levels, 4)
			assert.Equal(t, []string{o.rootID, o.ouID, o.childID, o.acctID},
				[]string{levels[0].TargetID, levels[1].TargetID, levels[2].TargetID, levels[3].TargetID})

			for _, lvl := range levels {
				require.Len(t, lvl.Policies, 1, lvl.TargetID)
				assert.Contains(t, lvl.Policies[0].ARN, "p-FullAWSAccess")
			}
		})
	}
}

func TestSCPsForAccountReenableAttachesDefault(t *testing.T) {
	t.Parallel()

	o := newSCPOrg(t)

	_, err := o.b.DisablePolicyType(o.rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)
	_, err = o.b.EnablePolicyType(o.rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	levels, ok := o.b.SCPsForAccount(o.acctID)
	require.True(t, ok)

	for _, lvl := range levels {
		assert.Len(t, lvl.Policies, 1, lvl.TargetID)
	}
}

func TestRestoreMigratesMissingDefaultSCPs(t *testing.T) {
	t.Parallel()

	o := newSCPOrg(t)

	var snap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(o.b.Snapshot(context.Background()), &snap))

	oldShape := func(key string, v any) {
		raw, err := json.Marshal(v)
		require.NoError(t, err)

		snap[key] = raw
	}

	oldShape("targetPolicies", map[string][]string{o.rootID: {"p-FullAWSAccess"}})
	oldShape("policyTargets", map[string][]string{"p-FullAWSAccess": {o.rootID}})

	data, err := json.Marshal(snap)
	require.NoError(t, err)

	restored := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(context.Background(), data))

	levels, ok := restored.SCPsForAccount(o.acctID)
	require.True(t, ok)
	require.Len(t, levels, 4)

	for _, lvl := range levels {
		require.Len(t, lvl.Policies, 1, lvl.TargetID)
		assert.Equal(t, iam.EvalAllow,
			iam.EvaluatePolicies([]string{lvl.Policies[0].Document}, "s3:DeleteBucket", "*", iam.ConditionContext{}))
	}
}

func TestDetachLastSCPRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replace bool
		wantErr bool
	}{
		{name: "last_scp_rejected", wantErr: true},
		{name: "replaced_first_allowed", replace: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := newSCPOrg(t)

			if tt.replace {
				p, err := o.b.CreatePolicy("allow-s3", "", `{"Version":"2012-10-17","Statement":[`+
					`{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`, "SERVICE_CONTROL_POLICY", nil)
				require.NoError(t, err)
				require.NoError(t, o.b.AttachPolicy(p.PolicySummary.ID, o.ouID))
			}

			err := o.b.DetachPolicy("p-FullAWSAccess", o.ouID)
			if tt.wantErr {
				require.ErrorIs(t, err, organizations.ErrMinPolicyAttachment)

				return
			}

			require.NoError(t, err)
		})
	}
}
