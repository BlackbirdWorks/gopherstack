package cloudfrontkeyvaluestore_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_RequestValidationAndMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		suffix     string
		body       string
		wantType   string
		notContain string
		wantStatus int
	}{
		{
			name: "put missing value", method: http.MethodPut, suffix: "/keys/k1", body: `{}`,
			wantStatus: http.StatusBadRequest, wantType: "ValidationException",
		},
		{
			name: "update put missing value", method: http.MethodPost, suffix: "/keys",
			body:       `{"Puts":[{"Key":"k1"}]}`,
			wantStatus: http.StatusBadRequest, wantType: "ValidationException",
		},
		{
			name: "list bad token", method: http.MethodGet, suffix: "/keys?NextToken=bogus",
			wantStatus: http.StatusBadRequest, wantType: "ValidationException",
		},
		{
			name: "get missing key message", method: http.MethodGet, suffix: "/keys/nope",
			wantStatus: http.StatusNotFound, wantType: "ResourceNotFoundException", notContain: "NoSuchDistribution",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			kvs, err := backend.CreateKeyValueStore("val-"+strings.ReplaceAll(tt.name, " ", "-"), "", nil)
			require.NoError(t, err)

			_, etag, err := backend.ListKVSValues(kvs.ID)
			require.NoError(t, err)

			req := httptest.NewRequest(tt.method, "/key-value-stores/"+url.PathEscape(kvs.ARN)+tt.suffix,
				strings.NewReader(tt.body))
			req.Header.Set("If-Match", etag)
			rec := httptest.NewRecorder()

			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantType, rec.Header().Get("X-Amzn-Errortype"))

			if tt.notContain != "" {
				assert.NotContains(t, rec.Body.String(), tt.notContain)
			}
		})
	}
}

func TestHandler_ConflictMessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)
	kvs, err := backend.CreateKeyValueStore("conflict-msg", "", nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, "/key-value-stores/"+url.PathEscape(kvs.ARN)+"/keys/k",
		strings.NewReader(`{"Value":"v"}`))
	req.Header.Set("If-Match", "stale")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.NotContains(t, rec.Body.String(), "PreconditionFailed")
}
