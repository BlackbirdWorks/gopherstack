package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestGetUser_MultiplePools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   func(a, b *cognitoidp.TokenResult) string
		name    string
		wantErr bool
	}{
		{name: "first pool token", token: func(a, _ *cognitoidp.TokenResult) string { return a.AccessToken }},
		{name: "second pool token", token: func(_, b *cognitoidp.TokenResult) string { return b.AccessToken }},
		{
			name:    "id token rejected",
			token:   func(a, _ *cognitoidp.TokenResult) string { return a.IDToken },
			wantErr: true,
		},
		{name: "garbage rejected", token: func(_, _ *cognitoidp.TokenResult) string { return "a.b.c" }, wantErr: true},
		{
			name:    "tampered rejected",
			token:   func(a, _ *cognitoidp.TokenResult) string { return a.AccessToken + "x" },
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			be := newTestBackend()
			toks := make([]*cognitoidp.TokenResult, 0, 2)
			names := []string{"alice", "bob"}

			for _, name := range names {
				pool, err := be.CreateUserPool("p-" + name)
				require.NoError(t, err)

				client, err := be.CreateUserPoolClient(pool.ID, "c")
				require.NoError(t, err)

				toks = append(toks, signUpConfirmAndLogin(t, be, client.ClientID, name))
			}

			u, err := be.GetUser(tc.token(toks[0], toks[1]))
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, names, u.Username)
			assert.Equal(t, names[indexOfToken(toks, tc.token(toks[0], toks[1]))], u.Username)
		})
	}
}

func indexOfToken(toks []*cognitoidp.TokenResult, access string) int {
	for i, tk := range toks {
		if tk.AccessToken == access {
			return i
		}
	}

	return -1
}
