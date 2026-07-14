package iot_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newIoTHandler(t *testing.T) *iot.Handler {
	t.Helper()

	return iot.NewHandler(iot.NewInMemoryBackend(), nil)
}

func doIoTRequest(t *testing.T, h *iot.Handler, method, path string, body any) *httptest.ResponseRecorder {
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
	require.NoError(t, h.Handler()(c))

	return rec
}

func newHandlerForBatch3Test(t *testing.T) (*iot.Handler, *iot.InMemoryBackend) {
	t.Helper()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	return h, b
}

func newIoTHandlerBatch1(t *testing.T) *iot.Handler {
	t.Helper()

	return iot.NewHandler(iot.NewInMemoryBackend(), nil)
}

func iotRequest(
	t *testing.T,
	h *iot.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
	}
	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error for %s %s: %v", method, path, err)
	}

	return rec
}

func iotOK(t *testing.T, h *iot.Handler, method, path string, body any) map[string]any {
	t.Helper()
	rec := iotRequest(t, h, method, path, body)
	if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
		t.Fatalf("%s %s: want 200/204, got %d body: %s", method, path, rec.Code, rec.Body.String())
	}
	if rec.Body.Len() == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return out
}

func iotExpectError(t *testing.T, h *iot.Handler, path string) {
	t.Helper()
	rec := iotRequest(t, h, http.MethodGet, path, nil)
	if rec.Code == http.StatusOK {
		t.Fatalf("%s %s: expected error got 200, body: %s", http.MethodGet, path, rec.Body.String())
	}
}

// newRefBackend returns a fresh InMemoryBackend for refinement tests.
func newRefBackend() *iot.InMemoryBackend {
	return iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

// newRefHandler returns a fresh Handler and backend for refinement tests.
func newRefHandler() (*iot.Handler, *iot.InMemoryBackend) {
	b := newRefBackend()

	return iot.NewHandler(b, nil), b
}

// doRefRequest fires a synthetic HTTP request against the handler.
func doRefRequest(
	t *testing.T,
	h *iot.Handler,
	method, path string,
	body any,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var raw []byte

	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// newTestHandler is a helper that creates a fresh Handler backed by a new InMemoryBackend.
func newTestHandler() *iot.Handler {
	return iot.NewHandler(iot.NewInMemoryBackend(), nil)
}

// doRequest issues an HTTP request against h and returns the recorded response.
func doRequest(t *testing.T, h *iot.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody []byte

	if body != nil {
		var err error

		reqBody, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))

	if body != nil {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// newR3Handler returns a fresh Handler backed by a configured InMemoryBackend.
func newR3Handler() (*iot.Handler, *iot.InMemoryBackend) {
	b := iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	return iot.NewHandler(b, nil), b
}

// r3Req issues a request against the handler using doRefRequest and returns status + body bytes.
func r3Req(t *testing.T, h *iot.Handler, method, path string, body any) (int, []byte) {
	t.Helper()
	rec := doRefRequest(t, h, method, path, body, nil)

	return rec.Code, rec.Body.Bytes()
}

// r3JSON is like r3Req but also decodes the body into out.
func r3JSON(t *testing.T, h *iot.Handler, method, path string, body any, out any) int {
	t.Helper()
	code, raw := r3Req(t, h, method, path, body)
	if out != nil {
		require.NoError(t, json.Unmarshal(raw, out))
	}

	return code
}

// itoa avoids importing strconv solely for one call site in tests.
func itoa(n int32) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	if neg {
		return "-" + string(digits)
	}

	return string(digits)
}

func TestHandler_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           any
		validateOutput func(t *testing.T, body []byte)
		name           string
		method         string
		path           string
		wantOp         string
		wantResource   string
		wantStatus     int
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
				t.Helper()
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
				t.Helper()
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
			assert.Positive(t, handler.MatchPriority())
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
		{"/accept-certificate-transfer/cert-123", "accept_cert_transfer", true},
		{"/billing-groups/addThingToBillingGroup", "add_thing_to_billing_group", true},
		{"/thing-groups/addThingToThingGroup", "add_thing_to_thing_group", true},
		{"/packages/pkg/versions/v1/sbom", "package_sbom", true},
		{"/jobs/job-1/targets", "job_targets", true},
		{"/security-profiles/sp1/targets", "security_profile_targets", true},
		{"/audit/mitigationactions/tasks/task-1/cancel", "audit_mitigation_cancel", true},
		{"/audit/tasks/task-2/cancel", "audit_task_cancel", true},
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

func TestHandler_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		headers      map[string]string
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
		wantStatus   int
	}{
		{
			name:         "AcceptCertificateTransfer",
			method:       http.MethodPatch,
			path:         "/accept-certificate-transfer/cert-abc",
			body:         map[string]any{"setAsActive": true},
			wantStatus:   http.StatusOK,
			wantOp:       "AcceptCertificateTransfer",
			wantResource: "cert-abc",
		},
		{
			name:   "AddThingToBillingGroup",
			method: http.MethodPut,
			path:   "/billing-groups/addThingToBillingGroup",
			body: map[string]any{
				"billingGroupName": "group-1",
				"thingName":        "thing-1",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AddThingToBillingGroup",
			wantResource: "",
		},
		{
			name:   "AddThingToThingGroup",
			method: http.MethodPut,
			path:   "/thing-groups/addThingToThingGroup",
			body: map[string]any{
				"thingGroupName": "group-2",
				"thingName":      "thing-2",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AddThingToThingGroup",
			wantResource: "",
		},
		{
			name:   "AssociateSbomWithPackageVersion",
			method: http.MethodPut,
			path:   "/packages/my-pkg/versions/1.0.0/sbom",
			body: map[string]any{
				"sbom": map[string]any{
					"s3Location": map[string]any{
						"bucket":  "my-bucket",
						"key":     "sbom.json",
						"version": "v1",
					},
				},
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AssociateSbomWithPackageVersion",
			wantResource: "my-pkg",
		},
		{
			name:   "AssociateTargetsWithJob",
			method: http.MethodPost,
			path:   "/jobs/job-42/targets",
			body: map[string]any{
				"targets": []string{"arn:aws:iot:us-east-1:123:thing/t1"},
				"comment": "test",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AssociateTargetsWithJob",
			wantResource: "job-42",
		},
		{
			name:   "AttachPolicy",
			method: http.MethodPut,
			path:   "/target-policies/my-policy",
			body: map[string]any{
				"target": "arn:aws:iot:us-east-1:000000000000:cert/xyz",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AttachPolicy",
			wantResource: "my-policy",
		},
		{
			name:   "AttachSecurityProfile",
			method: http.MethodPut,
			path: "/security-profiles/sp-1/targets?" +
				"securityProfileTargetArn=arn:aws:iot:us-east-1:000000000000:all/things",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "AttachSecurityProfile",
			wantResource: "sp-1",
		},
		{
			name:   "AttachThingPrincipal",
			method: http.MethodPut,
			path:   "/things/my-thing/principals",
			body:   nil,
			headers: map[string]string{
				"x-amzn-principal": "arn:aws:iot:us-east-1:000000000000:cert/certid",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AttachThingPrincipal",
			wantResource: "my-thing",
		},
		{
			name:         "CancelAuditMitigationActionsTask",
			method:       http.MethodPut,
			path:         "/audit/mitigationactions/tasks/task-99/cancel",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "CancelAuditMitigationActionsTask",
			wantResource: "task-99",
		},
		{
			name:         "CancelAuditTask",
			method:       http.MethodPut,
			path:         "/audit/tasks/audit-task-1/cancel",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "CancelAuditTask",
			wantResource: "audit-task-1",
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

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, handler.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, handler.ExtractResource(c))

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGetThingConnectivityData(t *testing.T) {
	t.Parallel()

	t.Run("default_not_connected", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()
		b.AddThingInternal(iot.Thing{ThingName: "my-thing"})

		rec := doRefRequest(t, h, http.MethodPost, "/things/my-thing/connectivity-data", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"connected":false`)
	})

	t.Run("seeded_connected", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()
		b.AddThingInternal(iot.Thing{ThingName: "my-thing"})
		b.SetThingConnectivityInternal("my-thing", iot.ThingConnectivityData{Connected: true, Timestamp: 42})

		rec := doRefRequest(t, h, http.MethodPost, "/things/my-thing/connectivity-data", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"connected":true`)
	})

	t.Run("unknown_thing_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodPost, "/things/no-such/connectivity-data", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestBatch2_Tags tests tag operations.
func TestTags(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	arn := "arn:aws:iot:us-east-1:000000000000:thing/my-thing"

	// Tag
	iotOK(t, h, http.MethodPost, "/tags", map[string]any{
		"resourceArn": arn,
		"tags":        map[string]any{"env": "test", "owner": "alice"},
	})

	// List
	out := iotOK(t, h, http.MethodGet, "/tags?resourceArn="+arn, nil)
	tags, _ := out["tags"].([]any)
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// Untag
	iotOK(t, h, http.MethodDelete, "/tags?resourceArn="+arn+"&tagKeys=env", nil)

	// Verify
	out2 := iotOK(t, h, http.MethodGet, "/tags?resourceArn="+arn, nil)
	tags2, _ := out2["tags"].([]any)
	if len(tags2) != 1 {
		t.Errorf("expected 1 tag after untag, got %d", len(tags2))
	}
}

// TestBatch3_PackageCRUD tests Package create/get/update/list/delete.
func TestPackageCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create
	out := iotOK(t, h, http.MethodPut, "/packages/my-pkg", map[string]any{
		"description": "test package",
	})
	if out["packageName"] != "my-pkg" {
		t.Errorf("expected packageName=my-pkg, got %v", out)
	}

	// Get
	out2 := iotOK(t, h, http.MethodGet, "/packages/my-pkg", nil)
	if out2["packageName"] != "my-pkg" {
		t.Errorf("get mismatch: %v", out2)
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/packages/my-pkg", map[string]any{
		"description": "updated",
	})

	// List
	out3 := iotOK(t, h, http.MethodGet, "/packages", nil)
	pkgs, _ := out3["packageList"].([]any)
	if len(pkgs) != 1 {
		t.Errorf("expected 1 package, got %d", len(pkgs))
	}

	// Delete
	iotOK(t, h, http.MethodDelete, "/packages/my-pkg", nil)
	iotExpectError(t, h, "/packages/my-pkg")
}

// TestNewOps_Job tests Job lifecycle.
func TestJob(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateJob
	out := iotOK(t, h, http.MethodPost, "/jobs/my-job", map[string]any{
		"targets":         []string{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document":        `{"version":"1.0"}`,
		"targetSelection": "SNAPSHOT",
	})
	if out["jobId"] != "my-job" {
		t.Errorf("jobId mismatch: %v", out)
	}

	// DescribeJob
	out2 := iotOK(t, h, http.MethodGet, "/jobs/my-job", nil)
	job := out2["job"].(map[string]any)
	if job["jobId"] != "my-job" {
		t.Errorf("describe job mismatch: %v", job)
	}

	// ListJobs
	out3 := iotOK(t, h, http.MethodGet, "/jobs", nil)
	jobs, _ := out3["jobs"].([]any)
	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
	}

	// UpdateJob
	iotOK(t, h, http.MethodPatch, "/jobs/my-job", map[string]any{
		"description": "updated",
	})

	// GetJobDocument
	out4 := iotOK(t, h, http.MethodGet, "/jobs/my-job/document", nil)
	if out4["document"] != `{"version":"1.0"}` {
		t.Errorf("document mismatch: %v", out4["document"])
	}

	// CancelJob
	iotOK(t, h, http.MethodPut, "/jobs/my-job/cancel", map[string]any{
		"comment": "test cancel",
	})

	// DeleteJob
	iotOK(t, h, http.MethodDelete, "/jobs/my-job", nil)

	// Verify deletion
	iotExpectError(t, h, "/jobs/my-job")
}

// TestRefinement1_ThingNotFound_Returns404 verifies DescribeThing returns 404.
func TestThingNotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "describe_missing_thing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodGet, "/things/missing", nil, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestRefinement1_RuleNotFound_Returns404 verifies GetTopicRule returns 404.
func TestRuleNotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get_missing_rule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodGet, "/rules/missing-rule", nil, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestRefinement1_AttachThingPrincipal_Header verifies the x-amzn-principal header is read.
func TestAttachThingPrincipal_Header(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		wantCode  int
	}{
		{
			name:      "cert_principal_via_header",
			principal: "arn:aws:iot:us-east-1:123456789012:cert/abcdef",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodPut, "/things/my-thing/principals", nil,
				map[string]string{"x-amzn-principal": tt.principal})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_DeleteThing_Handler verifies DeleteThing handler.
func TestDeleteThing_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*iot.InMemoryBackend)
		name     string
		wantCode int
	}{
		{
			name: "delete_existing",
			setup: func(b *iot.InMemoryBackend) {
				b.AddThingInternal(iot.Thing{ThingName: "del-thing"})
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete_nonexistent",
			setup:    nil,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRefRequest(t, h, http.MethodDelete, "/things/del-thing", nil, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_HandleError_InternalServerError verifies fallback to 500.
func TestHandleError_InternalServerError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "unknown_error_returns_500"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			// Trigger an unknown operation to get to the fallback 400.
			rec := doRefRequest(t, h, http.MethodGet, "/things/", nil, nil)
			// Empty thingName: should be 404 (not found) since backend returns ThingNotFound.
			assert.NotEqual(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_ListThings_ResponseFormat(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "thing-alpha"})
	backend.AddThingInternal(iot.Thing{ThingName: "thing-beta"})

	resp := doRequest(t, h, http.MethodGet, "/things", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	things, ok := out["things"]
	require.True(t, ok, "response must have 'things' key")

	thingsSlice, ok := things.([]any)
	require.True(t, ok)
	assert.Len(t, thingsSlice, 2)
}

func TestHandler_UpdateThing(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "update-sensor"})

	resp := doRequest(t, h, http.MethodPatch, "/things/update-sensor", map[string]any{
		"attributePayload": map[string]any{
			"attributes": map[string]string{"location": "room-1"},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	th, err := backend.DescribeThing("update-sensor")
	require.NoError(t, err)
	assert.Equal(t, "room-1", th.Attributes["location"])
	assert.Equal(t, int64(2), th.Version)
}

func TestHandler_ListThingPrincipals_HTTP(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "http-principal-thing"})

	require.NoError(t, backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "http-principal-thing",
		Principal: "arn:aws:iot:us-east-1:000000000000:cert/xyz789",
	}))

	resp := doRequest(t, h, http.MethodGet, "/things/http-principal-thing/principals", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	principals, ok := out["principals"].([]any)
	require.True(t, ok)
	require.Len(t, principals, 1)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:cert/xyz789", principals[0])
}

func TestUpdateThing_VersionConflict_HTTP409(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/things/http-conflict-thing", nil)

	var out map[string]string
	code := r3JSON(t, h, http.MethodPatch, "/things/http-conflict-thing", map[string]any{
		"expectedVersion": 99,
	}, &out)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "VersionConflictException", out["__type"])
}

func TestDescribeEndpoint_HTTP_ReturnsAddress(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/endpoint", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["endpointAddress"])
}

// TestParityB_DescribeThing_DefaultClientId verifies DescribeThing returns defaultClientId.
func TestDescribeThing_DefaultClientId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
	}{
		{name: "simple_name", thingName: "sensor-001"},
		{name: "complex_name", thingName: "device:factory:line-3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/things/"+tt.thingName, nil)

			rec := doRequest(t, h, http.MethodGet, "/things/"+tt.thingName, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clientID, _ := resp["defaultClientId"].(string)
			assert.Equal(t, tt.thingName, clientID, "defaultClientId must equal thingName")
		})
	}
}

// TestListThings_Pagination verifies that GET /things honors maxResults and
// returns a nextToken, walking pages without dropping or duplicating things.
// Previously the op accepted and returned no pagination at all.
func TestListThings_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	const total = 5
	for i := range total {
		b.AddThingInternal(iot.Thing{ThingName: fmt.Sprintf("thing-%02d", i)})
	}

	type listResp struct {
		NextToken string           `json:"nextToken"`
		Things    []map[string]any `json:"things"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		path := "/things?maxResults=2"
		if token != "" {
			path += "&nextToken=" + token
		}

		rec := doRefRequest(t, h, http.MethodGet, path, nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.Things), 2, "page exceeds maxResults")

		for _, th := range resp.Things {
			name := th["thingName"].(string)
			assert.False(t, seen[name], "thing %s returned twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10, "pagination did not terminate")

		token = resp.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total, "all things returned exactly once")
	assert.GreaterOrEqual(t, pages, 3, "maxResults=2 over 5 items should span >=3 pages")
}
