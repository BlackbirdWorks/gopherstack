package apprunner_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

func newTestHandler(t *testing.T) *apprunner.Handler {
	t.Helper()
	backend := apprunner.NewInMemoryBackend("000000000000", "us-east-1")

	return apprunner.NewHandler(backend)
}

func doRequest(t *testing.T, h *apprunner.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AppRunner."+action)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createTestService(t *testing.T, h *apprunner.Handler) string {
	t.Helper()
	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "test-service",
		"SourceConfiguration": map[string]any{
			"ImageRepository": map[string]any{
				"ImageIdentifier":     "public.ecr.aws/nginx/nginx:latest",
				"ImageRepositoryType": "ECR_PUBLIC",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	svc, ok := resp["Service"].(map[string]any)
	require.True(t, ok)
	arn, ok := svc["ServiceArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func TestNotFound_UnknownARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		action string
	}{
		{
			name:   "DescribeService unknown ARN returns 400",
			action: "DescribeService",
			body:   map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
		},
		{
			name:   "DeleteService unknown ARN returns 400",
			action: "DeleteService",
			body:   map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
		},
		{
			name:   "PauseService unknown ARN returns 400",
			action: "PauseService",
			body:   map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
		},
		{
			name:   "ResumeService unknown ARN returns 400",
			action: "ResumeService",
			body:   map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
		},
		{
			name:   "StartDeployment unknown ARN returns 400",
			action: "StartDeployment",
			body:   map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
		},
		{
			name:   "ListOperations unknown ARN returns 400",
			action: "ListOperations",
			body:   map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestUnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BogusOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
