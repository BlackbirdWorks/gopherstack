package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountLeaveHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		leave func(t *testing.T, accountID string, remove func(string) error, leaveAs func(string) error) error
		name  string
		want  bool
	}{
		{
			name: "remove_account",
			leave: func(_ *testing.T, id string, remove func(string) error, _ func(string) error) error {
				return remove(id)
			},
			want: true,
		},
		{
			name: "member_leaves",
			leave: func(_ *testing.T, id string, _ func(string) error, leaveAs func(string) error) error {
				return leaveAs(id)
			},
			want: true,
		},
		{
			name: "unknown_account",
			leave: func(_ *testing.T, _ string, remove func(string) error, _ func(string) error) error {
				return remove("999999999999")
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			var got []string

			b.OnAccountLeave(func(id string) { got = append(got, id) })

			status, err := b.CreateAccount("leaver", "leaver@example.com", "", "", nil)
			require.NoError(t, err)

			_ = tt.leave(t, status.AccountID, b.RemoveAccountFromOrganization, b.LeaveOrganizationAs)

			if tt.want {
				assert.Equal(t, []string{status.AccountID}, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}
