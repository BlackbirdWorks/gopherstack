package sts_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/sts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sts.InMemoryBackend) string
		verify func(t *testing.T, b *sts.InMemoryBackend, id string)
		name   string
	}{
		{
			name:  "round_trip_no_state",
			setup: func(_ *sts.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sts.InMemoryBackend, _ string) {
				t.Helper()
				// STS has no mutable state; just verify the backend is functional after restore
				assert.NotNil(t, b)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sts.NewInMemoryBackendWithConfig("000000000000")
			_ = tt.setup(original)

			snap := original.Snapshot()
			// STS returns nil snapshot since it has no state
			assert.Nil(t, snap)

			fresh := sts.NewInMemoryBackendWithConfig("000000000000")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, "")
		})
	}
}

func TestSTSHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackendWithConfig("000000000000")
	h := sts.NewHandler(backend)

	// STS has no state; just verify delegation doesn't panic
	snap := h.Snapshot()
	assert.Nil(t, snap) // STS returns nil

	fresh := sts.NewInMemoryBackendWithConfig("000000000000")
	freshH := sts.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))
}

func TestSTSHandler_Routing(t *testing.T) {
	t.Parallel()

	h := sts.NewHandler(sts.NewInMemoryBackendWithConfig("000000000000"), slog.Default())

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
