package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestRCPContentValidation(t *testing.T) {
	t.Parallel()

	const (
		head = `{"Version":"2012-10-17","Statement":`
		deny = `{"Effect":"Deny","Principal":"*",`
	)

	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name: "valid_deny_with_condition",
			content: head + `[` + deny + `"Action":["s3:*","sts:AssumeRole"],"Resource":"*",` +
				`"Condition":{"Bool":{"aws:SecureTransport":"false"}}}]}`,
		},
		{
			name:    "valid_single_statement_object_notresource",
			content: head + deny + `"Action":"kms:Decrypt","NotResource":"arn:aws:kms:*:*:key/x"}}`,
		},
		{
			name:    "allow_effect_rejected",
			content: head + `[{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"}]}`,
			wantErr: "MalformedPolicyDocumentException",
		},
		{
			name:    "principal_must_be_star",
			content: head + `[{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:*","Resource":"*"}]}`,
			wantErr: "InvalidInputException",
		},
		{
			name:    "missing_principal_rejected",
			content: head + `[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}`,
			wantErr: "InvalidInputException",
		},
		{
			name:    "notprincipal_rejected",
			content: head + `[{"Effect":"Deny","NotPrincipal":"*","Action":"s3:*","Resource":"*"}]}`,
			wantErr: "InvalidInputException",
		},
		{
			name:    "notaction_rejected",
			content: head + `[` + deny + `"NotAction":"s3:*","Resource":"*"}]}`,
			wantErr: "MalformedPolicyDocumentException",
		},
		{
			name:    "bare_star_action_rejected",
			content: head + `[` + deny + `"Action":"*","Resource":"*"}]}`,
			wantErr: "MalformedPolicyDocumentException",
		},
		{
			name:    "unsupported_service_rejected",
			content: head + `[` + deny + `"Action":"ec2:*","Resource":"*"}]}`,
			wantErr: "MalformedPolicyDocumentException",
		},
		{
			name:    "resource_required",
			content: head + `[` + deny + `"Action":"s3:*"}]}`,
			wantErr: "MalformedPolicyDocumentException",
		},
		{
			name:    "wrong_version_rejected",
			content: `{"Version":"2008-10-17","Statement":[` + deny + `"Action":"s3:*","Resource":"*"}]}`,
			wantErr: "MalformedPolicyDocumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			p, err := b.CreatePolicy("rcp", "", tt.content, "RESOURCE_CONTROL_POLICY", nil)
			if tt.wantErr == "" {
				require.NoError(t, err)

				_, err = b.UpdatePolicy(p.PolicySummary.ID, "", nil, tt.content)
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestRCPsForAccount(t *testing.T) {
	t.Parallel()

	o := newSCPOrg(t)

	_, ok := o.b.RCPsForAccount(o.acctID)
	assert.False(t, ok, "RCPs not enabled")

	_, err := o.b.EnablePolicyType(o.rootID, "RESOURCE_CONTROL_POLICY")
	require.NoError(t, err)

	levels, ok := o.b.RCPsForAccount(o.acctID)
	require.True(t, ok)
	require.Len(t, levels, 4, "root, two OUs, account")

	for _, lvl := range levels {
		require.Len(t, lvl.Policies, 1)
		assert.Equal(t,
			"arn:aws:organizations::aws:policy/resource_control_policy/p-RCPFullAWSAccess", lvl.Policies[0].ARN)
	}

	_, ok = o.b.RCPsForAccount("000000000000")
	assert.False(t, ok, "management account is exempt")

	assert.NotEmpty(t, o.b.OrgIDForAccount(o.acctID))
	assert.NotEmpty(t, o.b.OrgIDForAccount("000000000000"))
	assert.Empty(t, o.b.OrgIDForAccount("999999999999"))

	require.ErrorContains(t, o.b.DetachPolicy("p-RCPFullAWSAccess", o.ouID), "InvalidInputException")

	managed, err := o.b.DescribePolicy("p-RCPFullAWSAccess")
	require.NoError(t, err)
	assert.Equal(t, "Allows access to every resource", managed.PolicySummary.Description)

	_, err = o.b.DisablePolicyType(o.rootID, "RESOURCE_CONTROL_POLICY")
	require.NoError(t, err)

	_, ok = o.b.RCPsForAccount(o.acctID)
	assert.False(t, ok)

	_, err = o.b.EnablePolicyType(o.rootID, "RESOURCE_CONTROL_POLICY")
	require.NoError(t, err)

	levels, _ = o.b.RCPsForAccount(o.acctID)
	require.Len(t, levels[0].Policies, 1, "re-enable restores only the default")
}
