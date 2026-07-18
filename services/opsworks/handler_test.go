package opsworks_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

func newTestHandler(t *testing.T) *opsworks.Handler {
	t.Helper()
	backend := opsworks.NewInMemoryBackend("000000000000", "us-east-1")

	return opsworks.NewHandler(backend)
}

func doTarget(t *testing.T, h *opsworks.Handler, operation string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "OpsWorks_20130218."+operation)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func parseJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var result map[string]any
	require.NoError(t, json.Unmarshal(body, &result))

	return result
}

// helpers shared across new-ops tests

func createTestStack(t *testing.T, h *opsworks.Handler) string {
	t.Helper()
	rec := doTarget(t, h, "CreateStack", map[string]any{
		"Name":                      "test-stack",
		"Region":                    "us-east-1",
		"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
		"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return parseJSON(t, rec.Body.Bytes())["StackId"].(string)
}

func createTestLayer(t *testing.T, h *opsworks.Handler, stackID string) string {
	t.Helper()
	rec := doTarget(t, h, "CreateLayer", map[string]any{
		"StackId":   stackID,
		"Type":      "custom",
		"Name":      "test-layer",
		"Shortname": "tl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return parseJSON(t, rec.Body.Bytes())["LayerId"].(string)
}

func createTestInstance(t *testing.T, h *opsworks.Handler, stackID, layerID string) string {
	t.Helper()
	rec := doTarget(t, h, "CreateInstance", map[string]any{
		"StackId":      stackID,
		"LayerIds":     []string{layerID},
		"InstanceType": "t2.micro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return parseJSON(t, rec.Body.Bytes())["InstanceId"].(string)
}

// TestErrorHandling verifies error responses for invalid requests.
func TestErrorHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		operation string
		wantCode  int
	}{
		{
			name:      "DescribeStacks with nonexistent ID returns 404",
			operation: "DescribeStacks",
			body:      map[string]any{"StackIds": []string{"nonexistent-id"}},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DescribeLayers with nonexistent ID returns 404",
			operation: "DescribeLayers",
			body:      map[string]any{"LayerIds": []string{"nonexistent-id"}},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DescribeInstances with nonexistent ID returns 404",
			operation: "DescribeInstances",
			body:      map[string]any{"InstanceIds": []string{"nonexistent-id"}},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DeleteStack with nonexistent ID returns 404",
			operation: "DeleteStack",
			body:      map[string]any{"StackId": "nonexistent-id"},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "DeleteLayer with nonexistent ID returns 404",
			operation: "DeleteLayer",
			body:      map[string]any{"LayerId": "nonexistent-id"},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "StartInstance with nonexistent ID returns 404",
			operation: "StartInstance",
			body:      map[string]any{"InstanceId": "nonexistent-id"},
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "CreateStack with empty name returns 400",
			operation: "CreateStack",
			body: map[string]any{
				"Name":           "",
				"ServiceRoleArn": "arn:aws:iam::000000000000:role/test",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUnknownActionReturnsValidationException verifies an unrecognized
// X-Amz-Target action returns HTTP 400 ValidationException, matching AWS, rather
// than HTTP 501 UnsupportedOperationException.
func TestUnknownActionReturnsValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		wantType  string
		wantCode  int
	}{
		{
			name:      "unknown_action",
			operation: "ThisActionDoesNotExist",
			wantCode:  http.StatusBadRequest,
			wantType:  "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantType)
		})
	}
}
