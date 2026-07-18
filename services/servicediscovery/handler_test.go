package servicediscovery_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSDRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInput")
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &servicediscovery.Provider{}
	assert.Equal(t, "ServiceDiscovery", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestServiceDiscovery_Handler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createNamespaces int
		wantAfter        int
	}{
		{
			name:             "reset clears all namespaces",
			createNamespaces: 2,
			wantAfter:        0,
		},
		{
			name:             "reset on empty backend is a no-op",
			createNamespaces: 0,
			wantAfter:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createNamespaces {
				rec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
					"Name":             fmt.Sprintf("ns-%d", i),
					"CreatorRequestId": fmt.Sprintf("req-%d", i),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			rec := doSDRequest(t, h, "ListNamespaces", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Namespaces []any `json:"Namespaces"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Namespaces, tt.wantAfter)
		})
	}
}

// newBackendAndHandler returns both handler and backend for refinement tests
// that need direct backend access.
func newBackendAndHandler(t *testing.T) (*servicediscovery.InMemoryBackend, *servicediscovery.Handler) {
	t.Helper()

	b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")

	return b, servicediscovery.NewHandler(b)
}

// TestHandler_AccountID verifies that AccountID() method is exposed on the backend.
func TestHandler_AccountID(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("999999999999", "eu-west-1")
	assert.Equal(t, "999999999999", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestHandler_ErrNilAppContext verifies that Provider.Init rejects nil context.
func TestHandler_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &servicediscovery.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, servicediscovery.ErrNilAppContext)
}

// TestHandler_GetSupportedOperationsSorted verifies that GetSupportedOperations
// returns a sorted, deterministic list with the expected count.
func TestHandler_GetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Equal(t, 30, servicediscovery.HandlerOpsLen(h), "expected 30 supported operations")

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "operations should be sorted; %q > %q", ops[i-1], ops[i])
	}
}

// TestHandlerBackendInterface verifies that NewHandler accepts a StorageBackend.
func TestHandlerBackendInterface(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

	// This should compile because *InMemoryBackend implements StorageBackend.
	var sb servicediscovery.StorageBackend = b
	h := servicediscovery.NewHandler(sb)
	assert.NotNil(t, h)
}

// createNamespaceHelper is a convenience function that creates an HTTP namespace
// and returns its ID.
func createNamespaceHelper(t *testing.T, h *servicediscovery.Handler, name string) string {
	t.Helper()

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": name})
	require.Equal(t, 200, nsRec.Code)

	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))

	opID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})

	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))

	return nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)
}

// TestHandler_GetSupportedOperationsIncludesUpdateAndAttributeOps verifies the
// update, attribute, and revision operations are in GetSupportedOperations.
func TestHandler_GetSupportedOperationsIncludesUpdateAndAttributeOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"DeleteServiceAttributes",
		"DiscoverInstancesRevision",
		"GetServiceAttributes",
		"UpdateHttpNamespace",
		"UpdateInstanceCustomHealthStatus",
		"UpdatePrivateDnsNamespace",
		"UpdatePublicDnsNamespace",
		"UpdateService",
		"UpdateServiceAttributes",
	} {
		assert.Contains(t, ops, op, "expected %s in GetSupportedOperations", op)
	}
}
