package sts_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sts.InMemoryBackend) string
		verify func(t *testing.T, b *sts.InMemoryBackend, accessKeyID string)
		name   string
	}{
		{
			name:  "round_trip_no_state",
			setup: func(_ *sts.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sts.InMemoryBackend, _ string) {
				t.Helper()
				assert.Equal(t, 0, b.SessionCount())
			},
		},
		{
			name: "round_trip_with_active_session",
			setup: func(b *sts.InMemoryBackend) string {
				resp, err := b.AssumeRole(&sts.AssumeRoleInput{
					RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
					RoleSessionName: "restore-session",
					DurationSeconds: 900,
				})
				if err != nil {
					return ""
				}

				return resp.AssumeRoleResult.Credentials.AccessKeyID
			},
			verify: func(t *testing.T, b *sts.InMemoryBackend, accessKeyID string) {
				t.Helper()
				require.Equal(t, 1, b.SessionCount())

				// Session should still resolve correctly.
				ciResp, err := b.GetCallerIdentity(accessKeyID, "")
				require.NoError(t, err)
				assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")
				assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "TestRole")
			},
		},
		{
			name: "round_trip_expired_session_discarded",
			setup: func(b *sts.InMemoryBackend) string {
				resp, err := b.AssumeRole(&sts.AssumeRoleInput{
					RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
					RoleSessionName: "expired-session",
					DurationSeconds: 900,
				})
				if err != nil {
					return ""
				}

				accessKeyID := resp.AssumeRoleResult.Credentials.AccessKeyID
				// Force the session to be expired before snapshotting.
				b.SetSessionExpiration(accessKeyID, time.Now().Add(-time.Second))

				return accessKeyID
			},
			verify: func(t *testing.T, b *sts.InMemoryBackend, _ string) {
				t.Helper()
				// Expired session must not be restored.
				assert.Equal(t, 0, b.SessionCount())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sts.NewInMemoryBackendWithConfig("000000000000")
			accessKeyID := tt.setup(original)

			snap := original.Snapshot(t.Context())

			fresh := sts.NewInMemoryBackendWithConfig("000000000000")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, accessKeyID)
		})
	}
}

func TestInMemoryBackend_Restore_NilData(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(), nil))
	assert.Equal(t, 0, b.SessionCount())
}

func TestSTSHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackendWithConfig("000000000000")
	h := sts.NewHandler(backend)

	// Verify round-trip with no sessions.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := sts.NewInMemoryBackendWithConfig("000000000000")
	freshH := sts.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))
}

func TestSTSHandler_Routing(t *testing.T) {
	t.Parallel()

	h := sts.NewHandler(sts.NewInMemoryBackendWithConfig("000000000000"))

	assert.Equal(t, "STS", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		ct        string
		body      string
		wantMatch bool
	}{
		{"sts form", "application/x-www-form-urlencoded", "Action=GetCallerIdentity&Version=2011-06-15", true},
		{"json ct", "application/json", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.ct)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}
