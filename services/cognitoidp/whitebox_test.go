package cognitoidp

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCognitoIDP_RefreshTokenEviction proves storeRefreshTokenLocked
// opportunistically sweeps refresh tokens that expired naturally -- without
// ever being refreshed (InitiateAuthRefreshToken) or revoked (RevokeToken/
// GlobalSignOut), the only two paths that otherwise delete an entry -- once
// the table grows past refreshTokenEvictThreshold.
func TestCognitoIDP_RefreshTokenEviction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		seedExpired int
		inserts     int
		wantSwept   bool
	}{
		{name: "below threshold keeps expired", seedExpired: 1, inserts: 1},
		{
			name:        "threshold and sweep interval evicts expired",
			seedExpired: refreshTokenEvictThreshold + 16,
			inserts:     refreshTokenEvictSweepInterval,
			wantSwept:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", "")
			past := time.Now().Add(-time.Hour)
			live := time.Now().Add(time.Hour)

			b.mu.Lock("seed")
			for i := range tt.seedExpired {
				token := fmt.Sprintf("expired-%d", i)
				b.refreshTokens[token] = &refreshTokenEntry{
					PoolID: "pool", ClientID: "client", Username: fmt.Sprintf("user-%d", i), ExpiresAt: past,
				}
			}
			b.mu.Unlock()

			b.mu.Lock("insert")
			for i := range tt.inserts {
				token := fmt.Sprintf("live-%d", i)
				b.storeRefreshTokenLocked(token, &refreshTokenEntry{
					PoolID: "pool", ClientID: "client", Username: fmt.Sprintf("liveuser-%d", i), ExpiresAt: live,
				})
			}
			b.mu.Unlock()

			b.mu.RLock("check")
			_, stillPresent := b.refreshTokens["expired-0"]
			b.mu.RUnlock()

			if tt.wantSwept {
				assert.False(t, stillPresent, "expired refresh token should have been swept")
			} else {
				assert.True(t, stillPresent, "expired refresh token should remain below the eviction threshold")
			}
		})
	}
}

// TestCognitoIDP_DeleteUserClearsRevocationMarkers proves deleteUserStateLocked
// (shared by AdminDeleteUser/DeleteUser/DeleteUserPool's cascade) removes the
// tokenRevokedBeforeSeq/tokenRevokedBefore sign-out markers for the deleted
// user, so they don't outlive the user they revoked tokens for.
func TestCognitoIDP_DeleteUserClearsRevocationMarkers(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1", "")
	const poolID, username = "pool", "alice"
	key := userStateKey(poolID, username)

	b.mu.Lock("seed")
	b.tokenRevokedBeforeSeq[key] = 1
	b.tokenRevokedBefore[key] = time.Now()
	b.deleteUserStateLocked(poolID, username)
	_, seqPresent := b.tokenRevokedBeforeSeq[key]
	_, timePresent := b.tokenRevokedBefore[key]
	b.mu.Unlock()

	assert.False(t, seqPresent, "tokenRevokedBeforeSeq entry should be removed with the user")
	assert.False(t, timePresent, "tokenRevokedBefore entry should be removed with the user")
}
