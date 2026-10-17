package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestRequestsHandler(t *testing.T) {
	t.Parallel()

	statuses := []int{200, 404, 500, 200, 503, 400}
	services := []string{"S3", "s3", "DynamoDB", "SQS", "S3", "SQS"}

	tests := []struct {
		name      string
		query     string
		wantPaths []string
		wantCode  int
	}{
		{"default_all_newest_first", "", []string{"/5", "/4", "/3", "/2", "/1", "/0"}, 200},
		{"n_limits", "?n=2", []string{"/5", "/4"}, 200},
		{"status_exact", "?status=500", []string{"/2"}, 200},
		{"status_4xx", "?status=4xx", []string{"/5", "/1"}, 200},
		{"status_5xx_upper", "?status=5XX", []string{"/4", "/2"}, 200},
		{"status_failed", "?status=failed", []string{"/5", "/4", "/2", "/1"}, 200},
		{"service_case_insensitive", "?service=S3", []string{"/4", "/1", "/0"}, 200},
		{"combined", "?service=sqs&status=failed&n=1", []string{"/5"}, 200},
		{"no_match", "?service=nope", []string{}, 200},
		{"bad_n_text", "?n=abc", nil, 400},
		{"bad_n_zero", "?n=0", nil, 400},
		{"bad_n_too_big", "?n=7", nil, 400},
		{"bad_status", "?status=teapot", nil, 400},
		{"bad_status_range", "?status=99", nil, 400},
		{"bad_status_class", "?status=9xx", nil, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := logger.NewRequestRingBuffer(6)
			for i, s := range statuses {
				buf.Add(&logger.CapturedRequest{
					Path: "/" + strconv.Itoa(i), Status: s, Service: services[i],
					Headers: map[string]string{},
				})
			}

			e := echo.New()
			e.GET(logger.RequestsEndpointPath, buildRequestsHandler(buf))

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, logger.RequestsEndpointPath+tt.query, nil))
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.NotEmpty(t, body["message"])

				return
			}

			var got []logger.CapturedRequest
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

			paths := make([]string, 0, len(got))
			for _, r := range got {
				paths = append(paths, r.Path)
			}

			assert.Equal(t, tt.wantPaths, paths)
		})
	}
}

func TestRequestsHandler_NotSelfLogged(t *testing.T) {
	t.Parallel()

	e := echo.New()
	e.Use(logger.APIConsoleMiddleware())
	e.GET(logger.RequestsEndpointPath, buildRequestsHandler(logger.GlobalRingBuffer))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, logger.RequestsEndpointPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	for _, r := range logger.GlobalRingBuffer.GetAll() {
		assert.NotEqual(t, logger.RequestsEndpointPath, r.Path)
	}
}
