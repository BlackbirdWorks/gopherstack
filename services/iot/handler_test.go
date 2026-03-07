package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestHandler_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           any
		name           string
		method         string
		path           string
		wantStatus     int
		wantOp         string
		wantResource   string
		validateOutput func(t *testing.T, body []byte)
	}{
		{
			name:   "CreateThing",
			method: http.MethodPost,
			path:   "/things/my-thing",
			body: map[string]any{
				"thingTypeName": "Sensor",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "CreateThing",
			wantResource: "my-thing",
			validateOutput: func(t *testing.T, body []byte) {
				var out map[string]string
				require.NoError(t, json.Unmarshal(body, &out))
				assert.Equal(t, "my-thing", out["thingName"])
			},
		},
		{
			name:   "DescribeThing",
			method: http.MethodGet,
			path:   "/things/my-thing",
			body:   nil,
			validateOutput: func(t *testing.T, _ []byte) {
				// We need to create it first for a real test,
				// but here we just test routing/dispatch.
			},
			wantStatus:   http.StatusNotFound, // Not created in this specific test
			wantOp:       "DescribeThing",
			wantResource: "my-thing",
		},
		{
			name:   "CreatePolicy",
			method: http.MethodPost,
			path:   "/policies/my-policy",
			body: map[string]any{
				"policyDocument": "{}",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "CreatePolicy",
			wantResource: "my-policy",
		},
		{
			name:         "DescribeEndpoint",
			method:       http.MethodGet,
			path:         "/endpoint?endpointType=iot:Data-ATS",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "DescribeEndpoint",
			wantResource: "",
		},
		{
			name:         "UnknownOperation",
			method:       http.MethodGet,
			path:         "/invalid-path",
			body:         nil,
			wantStatus:   http.StatusBadRequest,
			wantOp:       "Unknown",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := iot.NewInMemoryBackend()
			handler := iot.NewHandler(backend, nil)

			var reqBody []byte
			if tt.body != nil {
				var err error
				reqBody, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(reqBody))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Test Metadata methods
			assert.Equal(t, tt.wantOp, handler.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, handler.ExtractResource(c))
			assert.True(t, handler.MatchPriority() > 0)
			assert.Equal(t, "IoT", handler.Name())
			assert.Contains(t, handler.GetSupportedOperations(), "CreateThing")

			// Test actual handler dispatch
			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.validateOutput != nil && rec.Code == http.StatusOK {
				tt.validateOutput(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	handler := iot.NewHandler(iot.NewInMemoryBackend(), nil)
	matcher := handler.RouteMatcher()

	tests := []struct {
		path string
		name string
		want bool
	}{
		{"/things/t1", "things_prefix", true},
		{"/rules/r1", "rules_prefix", true},
		{"/policies/p1", "policies_prefix", true},
		{"/endpoint", "endpoint_exact", true},
		{"/s3/bucket", "other_service", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}
