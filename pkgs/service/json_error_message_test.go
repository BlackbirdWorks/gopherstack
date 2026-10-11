package service_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestJSONErrorMessageMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		wantBody    string
		status      int
	}{
		{
			name: "mirrors lowercase", contentType: "application/json", status: http.StatusNotFound,
			body: `{"__type":"X","message":"boom"}`, wantBody: `{"Message":"boom","__type":"X","message":"boom"}`,
		},
		{
			name: "mirrors uppercase", contentType: "application/json", status: http.StatusNotFound,
			body: `{"Message":"boom"}`, wantBody: `{"Message":"boom","message":"boom"}`,
		},
		{
			name: "keeps existing", contentType: "application/x-amz-json-1.1", status: http.StatusBadRequest,
			body: `{"message":"a","Message":"b"}`, wantBody: `{"message":"a","Message":"b"}`,
		},
		{
			name: "skips success", contentType: "application/json", status: http.StatusOK,
			body: `{"message":"ok"}`, wantBody: `{"message":"ok"}`,
		},
		{
			name: "skips non json", contentType: "text/plain", status: http.StatusBadRequest,
			body: `{"message":"x"}`, wantBody: `{"message":"x"}`,
		},
		{
			name: "skips non string", contentType: "application/json", status: http.StatusBadRequest,
			body: `{"message":{"a":1}}`, wantBody: `{"message":{"a":1}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := service.JSONErrorMessageMiddleware()(func(c *echo.Context) error {
				c.Response().Header().Set("Content-Type", tt.contentType)
				c.Response().Header().Set("X-Amz-Crc32", "0")
				c.Response().WriteHeader(tt.status)
				_, err := c.Response().Write([]byte(tt.body))

				return err
			})
			e.GET("/", h)

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

			assert.Equal(t, tt.status, rec.Code)

			if tt.status >= http.StatusBadRequest && tt.contentType != "text/plain" {
				assert.JSONEq(t, tt.wantBody, rec.Body.String())
				assert.NotEqual(t, "0", rec.Header().Get("X-Amz-Crc32"))
			} else {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}

			_, err := strconv.Atoi(rec.Header().Get("X-Amz-Crc32"))
			require.NoError(t, err)
		})
	}
}

func TestJSONErrorResponseEmitsBothCasings(t *testing.T) {
	t.Parallel()

	b, err := (service.JSONErrorResponse{Type: "T", Message: "m"}).MarshalJSON()
	require.NoError(t, err)
	assert.JSONEq(t, `{"__type":"T","message":"m","Message":"m"}`, string(b))
}
