package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

func TestCreateService_ReturnsServiceArn(t *testing.T) {
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

func TestServiceCRUD(t *testing.T) {
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

func TestPauseResume(t *testing.T) {
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

func TestStartDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	rec := doRequest(t, h, "StartDeployment", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["OperationId"])
}

func TestUpdateService(t *testing.T) {
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

func TestListServices_Empty(t *testing.T) {
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

func TestPauseService_StateGuard(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "pause RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "pause PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

func TestResumeService_StateGuard(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "resume PAUSED service succeeds",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "resume RUNNING service returns InvalidStateException",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "ResumeService", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

func TestUpdateService_StateGuard(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "update RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "update PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "UpdateService", map[string]any{
				"ServiceArn":            arn,
				"InstanceConfiguration": map[string]any{"Cpu": "2 vCPU", "Memory": "4 GB"},
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

func TestStartDeployment_StateGuard(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup    func(t *testing.T, h *apprunner.Handler, arn string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "deploy RUNNING service succeeds",
			setup:    func(_ *testing.T, _ *apprunner.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "deploy PAUSED service returns InvalidStateException",
			setup: func(t *testing.T, h *apprunner.Handler, arn string) {
				t.Helper()
				rec := doRequest(t, h, "PauseService", map[string]any{"ServiceArn": arn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			arn := createTestService(t, h)
			tc.setup(t, h, arn)

			rec := doRequest(t, h, "StartDeployment", map[string]any{"ServiceArn": arn})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

// TestServiceOutputInstanceConfiguration verifies DescribeService returns
// InstanceConfiguration with Cpu and Memory populated from backend storage.
func TestServiceOutputInstanceConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "ic-svc",
		"InstanceConfiguration": map[string]any{
			"Cpu":    "2 vCPU",
			"Memory": "4 GB",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	svcArn := createResp["Service"].(map[string]any)["ServiceArn"].(string)

	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)

	ic, ok := svc["InstanceConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeService must include InstanceConfiguration")
	assert.Equal(t, "2 vCPU", ic["Cpu"])
	assert.Equal(t, "4 GB", ic["Memory"])
}

// TestServiceOutputSourceConfiguration verifies DescribeService returns
// SourceConfiguration with ImageRepository.ImageIdentifier populated.
func TestServiceOutputSourceConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "sc-svc",
		"SourceConfiguration": map[string]any{
			"ImageRepository": map[string]any{
				"ImageIdentifier":     "public.ecr.aws/nginx/nginx:latest",
				"ImageRepositoryType": "ECR_PUBLIC",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	svcArn := createResp["Service"].(map[string]any)["ServiceArn"].(string)

	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)

	sc, ok := svc["SourceConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeService must include SourceConfiguration")

	ir, ok := sc["ImageRepository"].(map[string]any)
	require.True(t, ok, "SourceConfiguration must include ImageRepository")
	assert.Equal(t, "public.ecr.aws/nginx/nginx:latest", ir["ImageIdentifier"])
}

// TestDefaultInstanceConfigurationPresent verifies that services created
// without explicit InstanceConfiguration still have Cpu and Memory in the
// DescribeService response (defaults applied by backend).
func TestDefaultInstanceConfigurationPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "default-ic-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	svcArn := createResp["Service"].(map[string]any)["ServiceArn"].(string)

	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)

	ic, ok := svc["InstanceConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeService must include InstanceConfiguration even with defaults")
	assert.NotEmpty(t, ic["Cpu"], "default Cpu must be non-empty")
	assert.NotEmpty(t, ic["Memory"], "default Memory must be non-empty")
}
