package servicediscovery_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

func newTestHandler(t *testing.T) *servicediscovery.Handler {
	t.Helper()

	return servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doSDRequest(t *testing.T, h *servicediscovery.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doSDRawRequest(t, h, op, bodyBytes)
}

func doSDRawRequest(t *testing.T, h *servicediscovery.Handler, op string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Route53AutoNaming_v20170314."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ServiceDiscovery", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateHttpNamespace", "CreatePrivateDnsNamespace", "CreatePublicDnsNamespace",
		"DeleteNamespace", "GetNamespace", "ListNamespaces",
		"CreateService", "DeleteService", "GetService", "ListServices",
		"RegisterInstance", "DeregisterInstance", "GetInstance", "ListInstances", "DiscoverInstances",
		"GetOperation", "ListOperations",
		"ListTagsForResource", "TagResource", "UntagResource",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "servicediscovery", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches_create_http_namespace",
			target: "Route53AutoNaming_v20170314.CreateHttpNamespace",
			want:   true,
		},
		{
			name:   "matches_list_namespaces",
			target: "Route53AutoNaming_v20170314.ListNamespaces",
			want:   true,
		},
		{
			name:   "no_match_wrong_prefix",
			target: "SageMaker.CreateModel",
			want:   false,
		},
		{
			name:   "no_match_empty",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "create_http_namespace",
			target: "Route53AutoNaming_v20170314.CreateHttpNamespace",
			wantOp: "CreateHttpNamespace",
		},
		{
			name:   "list_services",
			target: "Route53AutoNaming_v20170314.ListServices",
			wantOp: "ListServices",
		},
		{
			name:   "unknown",
			target: "Route53AutoNaming_v20170314.Unknown",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   any
		wantID string
	}{
		{
			name:   "with_id",
			body:   map[string]any{"Id": "ns-00000001"},
			wantID: "ns-00000001",
		},
		{
			name:   "with_service_id",
			body:   map[string]any{"ServiceId": "svc-00000001"},
			wantID: "svc-00000001",
		},
		{
			name:   "empty_body",
			body:   map[string]any{},
			wantID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantID, h.ExtractResource(c))
		})
	}
}

func TestHandler_CreateHTTPNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "my-namespace"},
			wantStatus: http.StatusOK,
			wantBody:   "OperationId",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreateHttpNamespace", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreateHttpNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_CreatePrivateDNSNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "private.local", "Vpc": "vpc-12345"},
			wantStatus: http.StatusOK,
			wantBody:   "OperationId",
		},
		{
			name:       "missing_name",
			body:       map[string]any{"Vpc": "vpc-12345"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreatePrivateDnsNamespace", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreatePrivateDnsNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_CreatePublicDNSNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "public.example.com"},
			wantStatus: http.StatusOK,
			wantBody:   "OperationId",
		},
		{
			name:       "missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreatePublicDnsNamespace", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreatePublicDnsNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_CreateNamespace_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "my-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "my-ns"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "NamespaceAlreadyExists")
}

func TestHandler_GetNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
		createNS   bool
	}{
		{
			name:       "success",
			createNS:   true,
			wantStatus: http.StatusOK,
			wantBody:   "Namespace",
		},
		{
			name:       "not_found",
			body:       map[string]any{"Id": "ns-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "ResourceNotFoundException",
		},
		{
			name:       "missing_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.createNS:
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "my-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

				opID := createResp["OperationId"].(string)

				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				require.Equal(t, http.StatusOK, opRec.Code)

				var opResp map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))

				operation := opResp["Operation"].(map[string]any)
				targets := operation["Targets"].(map[string]any)
				nsID := targets["NAMESPACE"].(string)

				rec = doSDRequest(t, h, "GetNamespace", map[string]any{"Id": nsID})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "GetNamespace", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "GetNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeleteNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		createNS   bool
	}{
		{
			name:       "success",
			createNS:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       map[string]any{"Id": "ns-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.createNS:
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "delete-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				opID := createResp["OperationId"].(string)

				opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
				require.Equal(t, http.StatusOK, opRec.Code)

				var opResp map[string]any
				require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
				operation := opResp["Operation"].(map[string]any)
				targets := operation["Targets"].(map[string]any)
				nsID := targets["NAMESPACE"].(string)

				rec = doSDRequest(t, h, "DeleteNamespace", map[string]any{"Id": nsID})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "DeleteNamespace", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "DeleteNamespace", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListNamespaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-alpha"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSDRequest(t, h, "ListNamespaces", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Namespaces")
	assert.Contains(t, rec.Body.String(), "ns-alpha")
}

func TestHandler_CreateService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"Name": "my-service"},
			wantStatus: http.StatusOK,
			wantBody:   "Service",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doSDRawRequest(t, h, "CreateService", tt.bodyRaw)
			} else {
				rec = doSDRequest(t, h, "CreateService", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_GetService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
		createSvc  bool
	}{
		{
			name:       "success",
			createSvc:  true,
			wantStatus: http.StatusOK,
			wantBody:   "Service",
		},
		{
			name:       "not_found",
			body:       map[string]any{"Id": "svc-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "ResourceNotFoundException",
		},
		{
			name:       "missing_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			if tt.createSvc {
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "my-svc"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				svcData := createResp["Service"].(map[string]any)
				svcID := svcData["Id"].(string)

				rec = doSDRequest(t, h, "GetService", map[string]any{"Id": svcID})
			} else {
				rec = doSDRequest(t, h, "GetService", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeleteService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		createSvc  bool
	}{
		{
			name:       "success",
			createSvc:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       map[string]any{"Id": "svc-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			if tt.createSvc {
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "del-svc"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				svcData := createResp["Service"].(map[string]any)
				svcID := svcData["Id"].(string)

				rec = doSDRequest(t, h, "DeleteService", map[string]any{"Id": svcID})
			} else {
				rec = doSDRequest(t, h, "DeleteService", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListServices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-alpha"})

	rec := doSDRequest(t, h, "ListServices", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Services")
	assert.Contains(t, rec.Body.String(), "svc-alpha")
}

func TestHandler_RegisterInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		createSvc  bool
	}{
		{
			name:       "success",
			createSvc:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{"InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_instance_id",
			body:       map[string]any{"ServiceId": "svc-00000001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "service_not_found",
			body:       map[string]any{"ServiceId": "svc-does-not-exist", "InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.createSvc:
				createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "reg-svc"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				svcData := createResp["Service"].(map[string]any)
				svcID := svcData["Id"].(string)

				rec = doSDRequest(t, h, "RegisterInstance", map[string]any{
					"ServiceId":  svcID,
					"InstanceId": "i-001",
					"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
				})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "RegisterInstance", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "RegisterInstance", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "inst-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	svcData := createResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-001",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-001"},
			wantStatus: http.StatusOK,
			wantBody:   "Instance",
		},
		{
			name:       "not_found",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "ResourceNotFoundException",
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{"InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_instance_id",
			body:       map[string]any{"ServiceId": svcID},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "GetInstance", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "list-inst-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	svcData := createResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-001",
	})

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ServiceId": svcID},
			wantStatus: http.StatusOK,
			wantBody:   "Instances",
		},
		{
			name:       "service_not_found",
			body:       map[string]any{"ServiceId": "svc-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "ListInstances", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeregisterInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "dereg-svc"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	svcData := createResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-001",
	})

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-001"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       map[string]any{"ServiceId": svcID, "InstanceId": "i-does-not-exist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_service_id",
			body:       map[string]any{"InstanceId": "i-001"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_instance_id",
			body:       map[string]any{"ServiceId": svcID},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "DeregisterInstance", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DiscoverInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "example.local"})
	require.Equal(t, http.StatusOK, nsRec.Code)

	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	opID := nsResp["OperationId"].(string)

	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, http.StatusOK, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	operation := opResp["Operation"].(map[string]any)
	targets := operation["Targets"].(map[string]any)
	nsID := targets["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":        "my-service",
		"NamespaceId": nsID,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcData := svcResp["Service"].(map[string]any)
	svcID := svcData["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-001",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
	})

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success_with_results",
			body: map[string]any{
				"NamespaceName": "example.local",
				"ServiceName":   "my-service",
			},
			wantStatus: http.StatusOK,
			wantBody:   "Instances",
		},
		{
			name: "no_results_unknown_ns",
			body: map[string]any{
				"NamespaceName": "does-not-exist",
				"ServiceName":   "my-service",
			},
			wantStatus: http.StatusOK,
			wantBody:   "Instances",
		},
		{
			name:       "missing_namespace_name",
			body:       map[string]any{"ServiceName": "my-service"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_service_name",
			body:       map[string]any{"NamespaceName": "example.local"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, "DiscoverInstances", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_GetOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
		createNS   bool
	}{
		{
			name:       "success",
			createNS:   true,
			wantStatus: http.StatusOK,
			wantBody:   "Operation",
		},
		{
			name:       "not_found",
			body:       map[string]any{"OperationId": "op-does-not-exist"},
			wantStatus: http.StatusBadRequest,
			wantBody:   "ResourceNotFoundException",
		},
		{
			name:       "missing_operation_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.createNS:
				createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "op-ns"})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				opID := createResp["OperationId"].(string)

				rec = doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
			case tt.bodyRaw != nil:
				rec = doSDRawRequest(t, h, "GetOperation", tt.bodyRaw)
			default:
				rec = doSDRequest(t, h, "GetOperation", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ops-ns"})

	rec := doSDRequest(t, h, "ListOperations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Operations")
}

func TestHandler_TagsLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
		"Name": "tag-ns",
		"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	opID := createResp["OperationId"].(string)

	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, http.StatusOK, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	operation := opResp["Operation"].(map[string]any)
	targets := operation["Targets"].(map[string]any)
	nsID := targets["NAMESPACE"].(string)

	nsRec := doSDRequest(t, h, "GetNamespace", map[string]any{"Id": nsID})
	require.Equal(t, http.StatusOK, nsRec.Code)

	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	arn := nsResp["Namespace"].(map[string]any)["Arn"].(string)

	rec := doSDRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	tagRec := doSDRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	untagRec := doSDRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

func TestHandler_TagsErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list_tags_missing_arn",
			op:         "ListTagsForResource",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag_resource_missing_arn",
			op:         "TagResource",
			body:       map[string]any{"Tags": []map[string]string{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "untag_resource_missing_arn",
			op:         "UntagResource",
			body:       map[string]any{"TagKeys": []string{"env"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "list_tags_not_found",
			op:   "ListTagsForResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:servicediscovery:us-east-1:000000000000:" +
					"namespace/ns-does-not-exist",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSDRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSDRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInput")
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestBackend_ListNamespaces(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateHTTPNamespace("ns-b", "", nil)
	require.NoError(t, err)

	_, err = b.CreateHTTPNamespace("ns-a", "", nil)
	require.NoError(t, err)

	list := b.ListNamespaces()
	require.Len(t, list, 2)
	assert.Equal(t, "ns-a", list[0].Name, "namespaces should be sorted by name")
}

func TestBackend_ListServices_FilterByNamespace(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

	opID, err := b.CreateHTTPNamespace("ns-filter", "", nil)
	require.NoError(t, err)

	op, err := b.GetOperation(opID)
	require.NoError(t, err)

	nsID := op.TargetID

	_, err = b.CreateService("svc-in-ns", nsID, "", nil)
	require.NoError(t, err)

	_, err = b.CreateService("svc-no-ns", "", "", nil)
	require.NoError(t, err)

	all := b.ListServices("")
	assert.Len(t, all, 2)

	filtered := b.ListServices(nsID)
	assert.Len(t, filtered, 1)
	assert.Equal(t, "svc-in-ns", filtered[0].Name)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &servicediscovery.Provider{}
	assert.Equal(t, "ServiceDiscovery", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
