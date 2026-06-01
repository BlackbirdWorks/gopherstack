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

func TestAudit1_CreateService_ReturnsServiceArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "create returns ServiceArn",
			body: map[string]any{
				"ServiceName": "my-service",
				"SourceConfiguration": map[string]any{
					"ImageRepository": map[string]any{
						"ImageIdentifier":     "public.ecr.aws/nginx/nginx:latest",
						"ImageRepositoryType": "ECR_PUBLIC",
					},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				svc, ok := resp["Service"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, svc["ServiceArn"], "arn:aws:apprunner:us-east-1:000000000000:service/")
				assert.Equal(t, "RUNNING", svc["Status"])
				assert.NotEmpty(t, svc["ServiceUrl"])
				assert.NotEmpty(t, resp["OperationId"])
			},
		},
		{
			name:     "create missing ServiceName returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateService", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_ServiceCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	svcArn := createTestService(t, h)
	assert.Equal(t, 1, apprunner.ServiceCount(h.Backend.(*apprunner.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)
	assert.Equal(t, "test-service", svc["ServiceName"])
	assert.Equal(t, "RUNNING", svc["Status"])

	// List
	rec = doRequest(t, h, "ListServices", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["ServiceSummaryList"], 1)

	// Delete
	rec = doRequest(t, h, "DeleteService", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, apprunner.ServiceCount(h.Backend.(*apprunner.InMemoryBackend)))

	// Describe deleted returns 400
	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_PauseResume(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	// Pause
	rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var pauseResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pauseResp))
	svc := pauseResp["Service"].(map[string]any)
	assert.Equal(t, "PAUSED", svc["Status"])

	// Resume
	rec = doRequest(t, h, "ResumeService", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resumeResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resumeResp))
	svc = resumeResp["Service"].(map[string]any)
	assert.Equal(t, "RUNNING", svc["Status"])
}

func TestAudit1_StartDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	rec := doRequest(t, h, "StartDeployment", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["OperationId"])
}

func TestAudit1_ListOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	// Perform some operations
	doRequest(t, h, "PauseService", map[string]any{"ServiceArn": svcArn})
	doRequest(t, h, "ResumeService", map[string]any{"ServiceArn": svcArn})

	rec := doRequest(t, h, "ListOperations", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ops, ok := resp["OperationSummaryList"].([]any)
	require.True(t, ok)
	// CREATE + PAUSE + RESUME = 3 operations
	assert.Len(t, ops, 3)
}

func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	// TagResource
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": svcArn,
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "prod"},
			map[string]any{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// UntagResource
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": svcArn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify untag
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": svcArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["Tags"].([]any)
	assert.Len(t, tags, 1)
}

func TestAudit1_NotFound(t *testing.T) {
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

func TestAudit1_UpdateService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	rec := doRequest(t, h, "UpdateService", map[string]any{
		"ServiceArn": svcArn,
		"InstanceConfiguration": map[string]any{
			"Cpu":    "2 vCPU",
			"Memory": "4 GB",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["OperationId"])
}

func TestAudit1_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BogusOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_ListServices_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListServices", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	services, ok := resp["ServiceSummaryList"].([]any)
	require.True(t, ok)
	assert.Empty(t, services)
}
