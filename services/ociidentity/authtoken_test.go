package ociidentity_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func issued(s string) string { return s }

func suffixed(s string) string { return s + "x" }

func TestVerifyAuthToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token    func(issued string) string
		name     string
		user     string
		deleteIt bool
		want     bool
	}{
		{name: "valid", user: "alice", token: issued, deleteIt: false, want: true},
		{name: "case insensitive name", user: "ALICE", token: issued, deleteIt: false, want: true},
		{name: "wrong token", user: "alice", token: suffixed, deleteIt: false, want: false},
		{name: "empty token", user: "alice", token: func(string) string { return "" }, deleteIt: false, want: false},
		{name: "unknown user", user: "bob", token: issued, deleteIt: false, want: false},
		{name: "deleted token", user: "alice", token: issued, deleteIt: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			u := e.mkUser(t, "alice")

			r, err := e.client.CreateAuthToken(t.Context(), identity.CreateAuthTokenRequest{
				UserId: u.Id, Description: new("registry"),
			})
			require.NoError(t, err)

			if tt.deleteIt {
				_, err = e.client.DeleteAuthToken(t.Context(), identity.DeleteAuthTokenRequest{
					UserId: u.Id, AuthTokenId: r.Id,
				})
				require.NoError(t, err)
			}

			id, ok := e.backend.VerifyAuthToken(tt.user, tt.token(*r.Token))
			assert.Equal(t, tt.want, ok)

			if tt.want {
				assert.Equal(t, *u.Id, id)
			} else {
				assert.Empty(t, id)
			}
		})
	}
}

func TestVerifyAuthTokenSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	u := e.mkUser(t, "alice")

	r, err := e.client.CreateAuthToken(t.Context(), identity.CreateAuthTokenRequest{
		UserId: u.Id, Description: new("registry"),
	})
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())
	assert.NotContains(t, string(snap), *r.Token)

	fresh := oci.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	id, ok := fresh.VerifyAuthToken("alice", *r.Token)
	assert.True(t, ok)
	assert.Equal(t, *u.Id, id)

	_, ok = fresh.VerifyAuthToken("alice", "wrong")
	assert.False(t, ok)
}

func TestVerifyAuthTokenRejectsLegacyAndInactive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(snap string) string
		name   string
	}{
		{name: "legacy token without digest", mutate: func(s string) string {
			return regexp.MustCompile(`,"(salt|digest)":"[0-9a-f]+"`).ReplaceAllString(s, "")
		}},
		{name: "inactive user", mutate: func(s string) string {
			return strings.ReplaceAll(s, `"ACTIVE"`, `"INACTIVE"`)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			u := e.mkUser(t, "alice")

			r, err := e.client.CreateAuthToken(t.Context(), identity.CreateAuthTokenRequest{
				UserId: u.Id, Description: new("registry"),
			})
			require.NoError(t, err)

			snap := tt.mutate(string(e.backend.Snapshot(t.Context())))

			fresh := oci.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), []byte(snap)))

			id, ok := fresh.VerifyAuthToken("alice", *r.Token)
			assert.False(t, ok)
			assert.Empty(t, id)
		})
	}
}
