package cognitoidp

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotRoundTrip_NonUTCZone guards against the bare-'Z' layout bug: Format's
// trailing "Z" in "2006-01-02T15:04:05Z" is a literal, not a zone conversion, so a
// non-UTC time.Time round-trips through Parse (which defaults to UTC) offset by
// whatever zone it carried. Times must be normalized with .UTC() before Format.
func TestSnapshotRoundTrip_NonUTCZone(t *testing.T) {
	t.Parallel()

	cdt := time.FixedZone("CDT", -5*60*60)
	want := time.Date(2026, 8, 7, 9, 30, 15, 0, cdt)

	tests := []struct {
		roundTrip func() time.Time
		name      string
	}{
		{
			name: "user created at",
			roundTrip: func() time.Time {
				snap := buildUserSnapshot(&User{CreatedAt: want, UpdatedAt: want})

				return restoreUsersFromSnapshot([]*userSnapshot{snap})[0].CreatedAt
			},
		},
		{
			name: "user updated at",
			roundTrip: func() time.Time {
				snap := buildUserSnapshot(&User{CreatedAt: want, UpdatedAt: want})

				return restoreUsersFromSnapshot([]*userSnapshot{snap})[0].UpdatedAt
			},
		},
		{
			name: "user confirm code expires at",
			roundTrip: func() time.Time {
				snap := buildUserSnapshot(&User{CreatedAt: want, UpdatedAt: want, ConfirmCodeExpiresAt: want})

				return restoreUsersFromSnapshot([]*userSnapshot{snap})[0].ConfirmCodeExpiresAt
			},
		},
		{
			name: "user last auth time",
			roundTrip: func() time.Time {
				snap := buildUserSnapshot(&User{CreatedAt: want, UpdatedAt: want, LastAuthTime: want})

				return restoreUsersFromSnapshot([]*userSnapshot{snap})[0].LastAuthTime
			},
		},
		{
			name: "pool created at",
			roundTrip: func() time.Time {
				snap := buildPoolSnapshot(t.Context(), &UserPool{CreatedAt: want, issuer: &tokenIssuer{}})
				pools, err := restorePoolsFromSnapshot([]*userPoolSnapshot{snap})
				require.NoError(t, err)

				return pools[0].CreatedAt
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.roundTrip()
			assert.True(t, want.Equal(got), "want %v, got %v (differ by %v)", want, got, got.Sub(want))
		})
	}
}

// TestGlobalSignOut_RestoreFromV2Snapshot proves that restoring a v2-shaped
// snapshot (predating the authSeq/gs_auth_seq fix, no tokenSeq/
// tokenRevokedBeforeSeq at all -- only the legacy tokenRevokedBefore
// wall-clock map) still honors a pending GlobalSignOut at the old,
// second-granularity precision, instead of the version bump this issue
// (gopherstack-n3zi slice 1 follow-up) explicitly avoids: a version bump
// would discard the ENTIRE snapshot (every user pool/user) on restore, not
// just the revocation bookkeeping (persistence.go's Restore, `snap.Version
// != cognitoidpSnapshotVersion` branch).
func TestGlobalSignOut_RestoreFromV2Snapshot(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("v2-restore-pool")
	require.NoError(t, err)

	_, err = b.CreateUserPoolClient(pool.ID, "v2-restore-client")
	require.NoError(t, err)

	user, err := b.AdminCreateUser(pool.ID, "alice", "TempPass1!", nil)
	require.NoError(t, err)

	// baseTime is the (hand-chosen) GlobalSignOut instant a v2 snapshot would
	// have recorded. preAuthTime is strictly before it (must be rejected);
	// postAuthTime is strictly after it (must be accepted). Both tokens are
	// minted directly via the issuer (not through a real sign-in) so their
	// auth_time claims are exact, chosen values rather than real wall-clock
	// time -- no time.Sleep, no flakiness.
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	preAuthTime := baseTime.Add(-1 * time.Hour).Unix()
	postAuthTime := baseTime.Add(1 * time.Hour).Unix()

	preTokens, err := pool.issuer.Issue(TokenParams{
		ClientID: "v2-restore-client", Username: user.Username, UserSub: user.Sub,
		AuthTime: preAuthTime,
	})
	require.NoError(t, err)

	postTokens, err := pool.issuer.Issue(TokenParams{
		ClientID: "v2-restore-client", Username: user.Username, UserSub: user.Sub,
		AuthTime: postAuthTime,
	})
	require.NoError(t, err)

	// Take a real snapshot, then hand-patch it into the OLD (v2) shape: strip
	// the fields that don't exist pre-authSeq, and populate the legacy
	// tokenRevokedBefore map the way a real v2 GlobalSignOut would have.
	data := b.Snapshot(t.Context())
	require.NotEmpty(t, data)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))

	delete(raw, "tokenSeq")
	delete(raw, "tokenRevokedBeforeSeq")

	key := pool.ID + ":" + user.Username
	revoked, err := json.Marshal(map[string]time.Time{key: baseTime})
	require.NoError(t, err)
	raw["tokenRevokedBefore"] = revoked
	raw["version"] = json.RawMessage(`2`)

	patched, err := json.Marshal(raw)
	require.NoError(t, err)

	b2 := NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	require.NoError(t, b2.Restore(t.Context(), patched))

	restoredPool, ok := b2.pools.Get(pool.ID)
	require.True(t, ok, "the user pool must survive restore -- not discarded via a version-mismatch reset")
	assert.Equal(t, "v2-restore-pool", restoredPool.Name)

	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{name: "pre_signout_token_rejected", token: preTokens.AccessToken, wantErr: true},
		{name: "post_signout_token_accepted", token: postTokens.AccessToken, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, getErr := b2.GetUser(tt.token)
			if tt.wantErr {
				assert.Error(t, getErr)

				return
			}

			require.NoError(t, getErr)
		})
	}
}
