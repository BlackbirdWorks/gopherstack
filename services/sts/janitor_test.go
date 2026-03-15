package sts_test

import (
	"context"
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

	b := sts.NewInMemoryBackend()

	// Issue two assume-role sessions.
	resp1, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/Role1",
		RoleSessionName: "session1",
		DurationSeconds: 900,
	})
	require.NoError(t, err)

	_, err = b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/Role2",
		RoleSessionName: "session2",
		DurationSeconds: 900,
	})
	require.NoError(t, err)

	require.Equal(t, 2, b.SessionCount())

	// Force the first session into the past.
	b.SetSessionExpiration(resp1.AssumeRoleResult.Credentials.AccessKeyID, time.Now().Add(-1*time.Second))

	// Run the janitor with a very short interval.
	j := sts.NewJanitor(b, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	go j.Run(ctx)

	// Wait until the expired session is gone.
	require.Eventually(t, func() bool {
		return b.SessionCount() == 1
	}, 2*time.Second, 20*time.Millisecond)
}

// TestJanitor_PreservesActiveSessions verifies that non-expired sessions are
// not removed by the janitor.
func TestJanitor_PreservesActiveSessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		sessionCount    int
		wantCountAfter  int
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
					RoleSessionName: "session" + string(rune('0'+i)),
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
		expiredAgo time.Duration
		wantARN    string
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

// TestHandler_WithJanitor_StartWorker verifies that StartWorker can be called on a
// handler that has a janitor attached without error.
func TestHandler_WithJanitor_StartWorker(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b).WithJanitor(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

// TestHandler_StartWorker_NoJanitor verifies that StartWorker on a handler without
// a janitor is a no-op (no panic, no error).
func TestHandler_StartWorker_NoJanitor(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}
