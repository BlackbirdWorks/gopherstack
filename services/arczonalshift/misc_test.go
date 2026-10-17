package arczonalshift_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	zs "github.com/blackbirdworks/gopherstack/services/arczonalshift"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	sdkcheck.CheckCompleteness(
		t,
		&sdk.Client{},
		zs.NewHandler(zs.NewInMemoryBackend(testAccount)).GetSupportedOperations(),
		[]string{},
	)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	lb := f.loadBalancer(t, "web", 2)
	started := f.start(t, lb, "us-east-1a", "1h")
	_, err := f.backend.SetObserverNotificationStatus("ENABLED")
	require.NoError(t, err)

	snap := f.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := zs.NewHandler(zs.NewInMemoryBackend(testAccount))
	require.NoError(t, restored.Restore(t.Context(), snap))

	shifts, _, err := restored.Backend.ListZonalShifts("", "", "", 0)
	require.NoError(t, err)
	require.Len(t, shifts, 1)
	assert.Equal(t, *started.ZonalShiftId, shifts[0].ZonalShiftID)
	assert.Equal(t, "ENABLED", restored.Backend.ObserverNotificationStatus())

	require.NoError(t, restored.Restore(t.Context(), []byte(`{"version":999}`)))
	shifts, _, err = restored.Backend.ListZonalShifts("", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, shifts)
}

func TestRouting(t *testing.T) {
	t.Parallel()

	h := zs.NewHandler(zs.NewInMemoryBackend(testAccount))

	tests := []struct {
		name, method, path, auth, wantOp string
		wantMatch                        bool
	}{
		{"start", http.MethodPost, "/zonalshifts", "arc-zonal-shift", "StartZonalShift", true},
		{"list", http.MethodGet, "/zonalshifts", "arc-zonal-shift", "ListZonalShifts", true},
		{"update", http.MethodPatch, "/zonalshifts/abc", "arc-zonal-shift", "UpdateZonalShift", true},
		{"cancel", http.MethodDelete, "/zonalshifts/abc", "arc-zonal-shift", "CancelZonalShift", true},
		{"start practice run", http.MethodPost, "/practiceruns", "arc-zonal-shift", "StartPracticeRun", true},
		{"cancel practice run", http.MethodDelete, "/practiceruns/abc", "arc-zonal-shift", "CancelPracticeRun", true},
		{
			"create configuration",
			http.MethodPost,
			"/configuration",
			"arc-zonal-shift",
			"CreatePracticeRunConfiguration",
			true,
		},
		{
			"update configuration",
			http.MethodPatch,
			"/configuration/arn%3Ax",
			"arc-zonal-shift",
			"UpdatePracticeRunConfiguration",
			true,
		},
		{
			"autoshift config",
			http.MethodPut,
			"/managedresources/arn%3Ax",
			"arc-zonal-shift",
			"UpdateZonalAutoshiftConfiguration",
			true,
		},
		{
			"observer",
			http.MethodGet,
			"/autoshift-observer-notification",
			"arc-zonal-shift",
			"GetAutoshiftObserverNotificationStatus",
			true,
		},
		{"autoshifts", http.MethodGet, "/autoshifts", "arc-zonal-shift", "ListAutoshifts", true},
		{"other service", http.MethodGet, "/zonalshifts", "s3", "", false},
		{"unknown path", http.MethodGet, "/nothing", "arc-zonal-shift", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+tt.auth+"/aws4_request",
			)
			c := echo.New().NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))

			if tt.wantMatch {
				assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			}
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	t.Parallel()

	h := zs.NewHandler(zs.NewInMemoryBackend(testAccount))

	tests := []struct {
		name, method, path, body, wantCode string
		wantStatus                         int
	}{
		{
			"not found",
			http.MethodPatch,
			"/zonalshifts/none",
			`{"comment":"c"}`,
			"ResourceNotFoundException",
			http.StatusNotFound,
		},
		{"validation", http.MethodPost, "/zonalshifts", `{}`, "ValidationException", http.StatusBadRequest},
		{"unknown operation", http.MethodPost, "/", "{}", "UnknownOperationException", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantCode, rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.NotEmpty(t, body["message"])
			assert.Equal(t, body["message"], body["Message"])
			assert.False(t, strings.HasPrefix(body["message"].(string), tt.wantCode))
		})
	}
}
