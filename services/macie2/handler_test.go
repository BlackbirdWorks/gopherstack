package macie2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) *macie2.Handler {
	t.Helper()
	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")

	return macie2.NewHandler(backend)
}

func doRequest(t *testing.T, h *macie2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestMacie2_RouteMatching(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		path string
		want bool
	}{
		{"/macie", true},
		{"/allow-lists", true},
		{"/allow-lists/some-id", true},
		{"/custom-data-identifiers", true},
		{"/findingsfilters", true},
		{"/findings", true},
		{"/findings/describe", true},
		{"/tags/arn:aws:macie2:us-east-1:000000000000:allow-list/id", true},
		{"/tags/arn:aws:guardduty:us-east-1:000000000000:detector/id", false},
		{"/s3", false},
		{"/iam/roles", false},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}
