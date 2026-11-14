package redisdocker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

func TestSetUserArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    []any
		user    redisdocker.ACLUser
		wantErr bool
	}{
		{
			name: "restricted keys",
			user: redisdocker.ACLUser{Name: "app", AccessString: "on ~app:* +@all", Passwords: []string{"pw1", "pw2"}},
			want: []any{"ACL", "SETUSER", "app", "reset", ">pw1", ">pw2", "on", "~app:*", "+@all"},
		},
		{
			name: "nopass",
			user: redisdocker.ACLUser{Name: "open", AccessString: "on ~* &* +@all", NoPassword: true},
			want: []any{"ACL", "SETUSER", "open", "reset", "nopass", "on", "~*", "&*", "+@all"},
		},
		{
			name: "no access string stays off",
			user: redisdocker.ACLUser{Name: "bare", Passwords: []string{"p"}},
			want: []any{"ACL", "SETUSER", "bare", "reset", ">p"},
		},
		{name: "empty name", user: redisdocker.ACLUser{}, wantErr: true},
		{name: "space in name", user: redisdocker.ACLUser{Name: "a b"}, wantErr: true},
		{name: "reserved admin", user: redisdocker.ACLUser{Name: redisdocker.AdminUser}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := redisdocker.SetUserArgs(tc.user)
			if tc.wantErr {
				require.ErrorIs(t, err, redisdocker.ErrACL)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
