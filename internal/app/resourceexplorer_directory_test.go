package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestOrganizationsDirectory(t *testing.T) {
	t.Parallel()

	b := organizationsbackend.NewInMemoryBackend("000000000001", "us-east-1")
	dir := &organizationsDirectory{dir: b}

	_, ok := dir.ManagementAccount()
	assert.False(t, ok, "no organization yet")
	assert.False(t, dir.ScopeExists("arn:aws:organizations::000000000001:organization/o-x"))

	org, root, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	ou, err := b.CreateOrganizationalUnit(root.ID, "eng", nil)
	require.NoError(t, err)

	mgmt, ok := dir.ManagementAccount()
	require.True(t, ok)
	assert.Equal(t, org.MasterAccountID, mgmt)

	tests := []struct {
		name  string
		scope string
		want  bool
	}{
		{name: "organization", scope: org.ARN, want: true},
		{name: "ou", scope: ou.ARN, want: true},
		{name: "unknown_ou", scope: ou.ARN + "zz", want: false},
		{name: "wrong_service", scope: "arn:aws:iam::000000000001:root", want: false},
		{name: "garbage", scope: "nope", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, dir.ScopeExists(tc.scope))
		})
	}

	assert.True(t, dir.IsMember(org.MasterAccountID))
	assert.False(t, dir.IsMember("999999999999"))
	assert.False(t, dir.IsDelegatedAdmin("resource-explorer-2.amazonaws.com", org.MasterAccountID))
}
