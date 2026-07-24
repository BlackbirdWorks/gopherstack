package ssoadmin_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

func newTestHandler() *ssoadmin.Handler {
	backend := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return ssoadmin.NewHandler(backend)
}

func doRequest(
	t *testing.T,
	h *ssoadmin.Handler,
	op string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SWBExternalService."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// doRequestRaw sends a POST request with a raw byte body and returns the recorder.
func doRequestRaw(
	t *testing.T,
	h *ssoadmin.Handler,
	op string,
	bodyBytes []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SWBExternalService."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func createInstance(t *testing.T, h *ssoadmin.Handler, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateInstance", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	arn, ok := resp["InstanceArn"].(string)
	require.True(t, ok, "expected InstanceArn in response")
	require.NotEmpty(t, arn)

	return arn
}

func createPermissionSet(t *testing.T, h *ssoadmin.Handler, instanceArn, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceArn,
		"Name":        name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	ps, ok := resp["PermissionSet"].(map[string]any)
	require.True(t, ok)
	arn, ok := ps["PermissionSetArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "Name", want: "SsoAdmin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	tests := []struct {
		name string
		op   string
	}{
		{name: "ListInstances", op: "ListInstances"},
		{name: "CreateInstance", op: "CreateInstance"},
		{name: "DescribeInstance", op: "DescribeInstance"},
		{name: "DeleteInstance", op: "DeleteInstance"},
		{name: "CreatePermissionSet", op: "CreatePermissionSet"},
		{name: "DescribePermissionSet", op: "DescribePermissionSet"},
		{name: "ListPermissionSets", op: "ListPermissionSets"},
		{name: "DeletePermissionSet", op: "DeletePermissionSet"},
		{name: "UpdatePermissionSet", op: "UpdatePermissionSet"},
		{name: "CreateAccountAssignment", op: "CreateAccountAssignment"},
		{name: "DeleteAccountAssignment", op: "DeleteAccountAssignment"},
		{name: "ListAccountAssignments", op: "ListAccountAssignments"},
		{name: "TagResource", op: "TagResource"},
		{name: "UntagResource", op: "UntagResource"},
		{name: "ListTagsForResource", op: "ListTagsForResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, tt.op)
		})
	}
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches ssoadmin prefix",
			target:    "SWBExternalService.ListInstances",
			wantMatch: true,
		},
		{
			name:      "matches ssoadmin prefix other op",
			target:    "SWBExternalService.CreatePermissionSet",
			wantMatch: true,
		},
		{
			name:      "does not match different prefix",
			target:    "AWSIdentityStore.ListUsers",
			wantMatch: false,
		},
		{
			name:      "does not match empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "unknown operation returns bad request",
			op:         "UnknownOp",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
			resp := parseResponse(t, rec)
			assert.Equal(t, "UnknownOperationException", resp["__type"])
		})
	}
}

func TestErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "describe non-existent instance",
			op:         "DescribeInstance",
			body:       map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-bad"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "delete non-existent permission set",
			op:   "DeletePermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-bad",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-bad/badid",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create permission set missing instance arn",
			op:         "CreatePermissionSet",
			body:       map[string]any{"Name": "PS"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create permission set missing name",
			op:         "CreatePermissionSet",
			body:       map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-bad"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe instance missing arn",
			op:         "DescribeInstance",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestMissingTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{
			name:       "missing X-Amz-Target returns 400",
			target:     "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wrong prefix returns 400",
			target:     "OtherService.ListInstances",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{}")))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			rec := httptest.NewRecorder()

			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "UnrecognizedClientException", resp["__type"])
		})
	}
}

func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "standard_account", accountID: "123456789012", region: "us-east-1"},
		{name: "different_region", accountID: "000000000001", region: "eu-west-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssoadmin.NewInMemoryBackend(tt.accountID, tt.region)
			assert.Equal(t, tt.accountID, b.AccountID())
			assert.Equal(t, tt.region, b.Region())
		})
	}
}

func TestHandler_ChaosMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "service_name", got: h.ChaosServiceName(), want: "sso"},
		{name: "region", got: h.ChaosRegions()[0], want: config.DefaultRegion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "valid_target", target: "SWBExternalService.ListInstances", want: "ListInstances"},
		{name: "no_prefix", target: "ListInstances", want: "ListInstances"},
		{name: "empty", target: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want string
	}{
		{
			name: "returns_instance_arn",
			body: map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-abc"},
			want: "arn:aws:sso:::instance/ssoins-abc",
		},
		{
			name: "empty_body",
			body: nil,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// createApplication is a test helper to create an application and return its ARN.
func createApplication(t *testing.T, h *ssoadmin.Handler, instanceArn, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
		"Name":                   name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	arn, ok := resp["ApplicationArn"].(string)
	require.True(t, ok, "expected ApplicationArn in response")
	require.NotEmpty(t, arn)

	return arn
}

// TestHandlerOpsLen verifies GetSupportedOperations count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, 79, ssoadmin.HandlerOpsLen(h))
}

// TestGetSupportedOperationsSorted verifies that GetSupportedOperations is sorted.
func TestGetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()
	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "ops should be sorted: %s >= %s", ops[i-1], ops[i])
	}
}

// TestHandlerReset verifies that Reset clears all backend state.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(backend)

	// Create resources.
	instanceArn := createInstance(t, h, "reset-inst")
	createPermissionSet(t, h, instanceArn, "reset-ps")
	_ = doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::000000000000:applicationProvider/custom",
		"Name":                   "ResetApp",
	})

	h.Reset()

	// After reset, only the default pre-seeded instance remains.
	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	instances := resp["Instances"].([]any)
	assert.Len(t, instances, 1, "reset should re-seed the default instance")
}

// TestBadJSONReturns400 verifies that all handlers return 400 on bad JSON bodies.
func TestBadJSONReturns400(t *testing.T) {
	t.Parallel()

	ops := []string{
		"CreateInstance",
		"DescribeInstance",
		"DeleteInstance",
		"CreatePermissionSet",
		"CreateAccountAssignment",
		"DeleteApplication",
		"CreateApplication",
		"CreateTrustedTokenIssuer",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequestRaw(t, h, op, []byte(`{invalid json`))
			assert.Equal(t, http.StatusBadRequest, rec.Code, "op=%s", op)
		})
	}
}

// TestResetReseeds verifies that Reset re-seeds the default instance.
func TestResetReseeds(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(backend)

	h.Reset()

	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	instances := resp["Instances"].([]any)
	assert.Len(t, instances, 1, "default instance should be re-seeded after Reset")
}
