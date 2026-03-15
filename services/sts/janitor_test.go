package sts_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestJanitor_SweepsExpiredSessions verifies that the janitor removes sessions
// whose Expiration is in the past.
func TestJanitor_SweepsExpiredSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		totalSessions  int
		expireCount    int
		wantCountAfter int
	}{
		{
			name:           "one_of_two_sessions_expired",
			totalSessions:  2,
			expireCount:    1,
			wantCountAfter: 1,
		},
		{
			name:           "all_sessions_expired",
			totalSessions:  3,
			expireCount:    3,
			wantCountAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()

			accessKeyIDs := make([]string, tt.totalSessions)

			for i := range tt.totalSessions {
				resp, err := b.AssumeRole(&sts.AssumeRoleInput{
					RoleArn:         "arn:aws:iam::123456789012:role/Role1",
					RoleSessionName: fmt.Sprintf("session%d", i),
					DurationSeconds: 900,
				})
				require.NoError(t, err)

				accessKeyIDs[i] = resp.AssumeRoleResult.Credentials.AccessKeyID
			}

			require.Equal(t, tt.totalSessions, b.SessionCount())

			// Force the first tt.expireCount sessions into the past.
			for i := range tt.expireCount {
				b.SetSessionExpiration(accessKeyIDs[i], time.Now().Add(-1*time.Second))
			}

			// Run the janitor with a very short interval.
			j := sts.NewJanitor(b, 10*time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			go j.Run(ctx)

			// Wait until the expired sessions are gone.
			require.Eventually(t, func() bool {
				return b.SessionCount() == tt.wantCountAfter
			}, 2*time.Second, 20*time.Millisecond)
		})
	}
}

// TestJanitor_PreservesActiveSessions verifies that non-expired sessions are
// not removed by the janitor.
func TestJanitor_PreservesActiveSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sessionCount   int
		wantCountAfter int
	}{
		{name: "two_active_sessions", sessionCount: 2, wantCountAfter: 2},
		{name: "one_active_session", sessionCount: 1, wantCountAfter: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()

			for i := range tt.sessionCount {
				_, err := b.AssumeRole(&sts.AssumeRoleInput{
					RoleArn:         "arn:aws:iam::123456789012:role/Role1",
					RoleSessionName: fmt.Sprintf("session%d", i),
					DurationSeconds: 900,
				})
				require.NoError(t, err)
			}

			j := sts.NewJanitor(b, 10*time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()

			go j.Run(ctx)
			<-ctx.Done()

			// All sessions still present since none are expired.
			assert.Equal(t, tt.wantCountAfter, b.SessionCount())
		})
	}
}

// TestGetCallerIdentity_ExpiredSession_FallsBackToDefault verifies that an expired
// session is treated as unknown: GetCallerIdentity falls back to the default identity.
func TestGetCallerIdentity_ExpiredSession_FallsBackToDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantARN    string
		expiredAgo time.Duration
	}{
		{
			name:       "expired_one_second_ago",
			expiredAgo: time.Second,
			wantARN:    sts.MockUserArn,
		},
		{
			name:       "expired_one_hour_ago",
			expiredAgo: time.Hour,
			wantARN:    sts.MockUserArn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()

			resp, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
				RoleSessionName: "my-session",
				DurationSeconds: 900,
			})
			require.NoError(t, err)

			accessKeyID := resp.AssumeRoleResult.Credentials.AccessKeyID

			// Verify the session is valid before expiry.
			ciResp, err := b.GetCallerIdentity(accessKeyID)
			require.NoError(t, err)
			assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")

			// Force the session to be expired.
			b.SetSessionExpiration(accessKeyID, time.Now().Add(-tt.expiredAgo))

			// After expiry, GetCallerIdentity must return the default identity.
			ciResp, err = b.GetCallerIdentity(accessKeyID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantARN, ciResp.GetCallerIdentityResult.Arn)
		})
	}
}

// TestHandler_StartWorker verifies StartWorker behaviour with and without a janitor.
func TestHandler_StartWorker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		withJanitor bool
	}{
		{name: "with_janitor", withJanitor: true},
		{name: "without_janitor", withJanitor: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			h := sts.NewHandler(b)

			if tt.withJanitor {
				h.WithJanitor(10 * time.Millisecond)
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			err := h.StartWorker(ctx)
			require.NoError(t, err)
		})
	}
}
