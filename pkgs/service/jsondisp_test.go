package service_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// errDispatchTest is the sentinel returned by the error-path dispatch stub.
var errDispatchTest = errors.New("dispatch error")

func newEchoContext(
	t *testing.T,
	method, path string,
	header http.Header,
	body string,
) (*echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, strings.NewReader(body))

	if header != nil {
		req.Header = header
	}

	rec := httptest.NewRecorder()

	return e.NewContext(req, rec), rec
}

func noopHandleErr(_ context.Context, c *echo.Context, _ string, reqErr error) error {
	return c.String(http.StatusBadRequest, reqErr.Error())
}

func TestHandleTarget(t *testing.T) {
	t.Parallel()

	log := slog.Default()
	supportedOps := []string{"GetFoo", "PutFoo"}

	successDispatch := service.DispatchFunc(func(_ context.Context, _ string, _ []byte) ([]byte, error) {
		return []byte("{\"ok\":true}"), nil
	})

	errDispatch := service.DispatchFunc(func(_ context.Context, _ string, _ []byte) ([]byte, error) {
		return nil, errDispatchTest
	})

	tests := []struct {
		header          http.Header
		dispatch        service.DispatchFunc
		name            string
		method          string
		path            string
		body            string
		wantBodyContain string
		wantContentType string
		wantStatus      int
	}{
		{
			name:            "GET / returns supported ops",
			method:          http.MethodGet,
			path:            "/",
			dispatch:        successDispatch,
			wantStatus:      http.StatusOK,
			wantBodyContain: "GetFoo",
		},
		{
			name:       "non-POST returns 405",
			method:     http.MethodPut,
			path:       "/",
			dispatch:   successDispatch,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "missing X-Amz-Target returns 400",
			method:     http.MethodPost,
			path:       "/",
			dispatch:   successDispatch,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid X-Amz-Target returns 400",
			method:     http.MethodPost,
			path:       "/",
			header:     http.Header{"X-Amz-Target": []string{"NoDotsHere"}},
			dispatch:   successDispatch,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:            "successful dispatch returns 200 with content-type",
			method:          http.MethodPost,
			path:            "/",
			header:          http.Header{"X-Amz-Target": []string{"TestService.GetFoo"}},
			body:            "{}",
			dispatch:        successDispatch,
			wantStatus:      http.StatusOK,
			wantBodyContain: "ok",
			wantContentType: "application/x-amz-json-1.1",
		},
		{
			name:            "dispatch error invokes handleErr",
			method:          http.MethodPost,
			path:            "/",
			header:          http.Header{"X-Amz-Target": []string{"TestService.GetFoo"}},
			body:            "{}",
			dispatch:        errDispatch,
			wantStatus:      http.StatusBadRequest,
			wantBodyContain: "dispatch error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, rec := newEchoContext(t, tc.method, tc.path, tc.header, tc.body)

			err := service.HandleTarget(
				c, log,
				"TestService", "application/x-amz-json-1.1",
				supportedOps,
				tc.dispatch,
				noopHandleErr,
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantBodyContain != "" {
				require.Contains(t, rec.Body.String(), tc.wantBodyContain)
			}

			if tc.wantContentType != "" {
				require.Contains(t, rec.Header().Get("Content-Type"), tc.wantContentType)
			}
		})
	}
}
