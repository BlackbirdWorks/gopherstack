package cognitoidp

import (
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
