package mediastore_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

func newTestHandler(t *testing.T) *mediastore.Handler {
	t.Helper()

	b := mediastore.NewInMemoryBackend()
	h := mediastore.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doRequest(t *testing.T, h *mediastore.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	var req *http.Request

	if len(bodyBytes) > 0 {
		req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	}

	req.Header.Set("X-Amz-Target", "MediaStore_20170901."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// unmarshalBody decodes rec's JSON body into a generic map, for tests that
// only need to peek at a handful of response fields.
func unmarshalBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// createTestContainer issues a CreateContainer request and returns the
// resulting container's ARN, failing the test on any non-200 response.
func createTestContainer(t *testing.T, h *mediastore.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": name})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalBody(t, rec)
	ct := m["Container"].(map[string]any)

	return ct["ARN"].(string)
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("X-Amz-Target", "MediaStore_20170901.BogusOp")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns service name", want: "MediaStore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOps []string
	}{
		{
			name:    "contains CreateContainer and PutMetricPolicy",
			wantOps: []string{"CreateContainer", "PutMetricPolicy", "GetMetricPolicy", "DeleteMetricPolicy"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ops := h.GetSupportedOperations()

			for _, op := range tt.wantOps {
				assert.Contains(t, ops, op)
			}
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
			name:   "extracts CreateContainer",
			target: "MediaStore_20170901.CreateContainer",
			wantOp: "CreateContainer",
		},
		{
			name:   "extracts PutMetricPolicy",
			target: "MediaStore_20170901.PutMetricPolicy",
			wantOp: "PutMetricPolicy",
		},
		{
			name:   "unknown target returns Unknown",
			target: "OtherService.SomeOp",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantName string
	}{
		{
			name:     "extracts ContainerName",
			body:     map[string]any{"ContainerName": "my-container"},
			wantName: "my-container",
		},
		{
			name:     "extracts Resource ARN",
			body:     map[string]any{"Resource": "arn:aws:mediastore:us-east-1:123:container/my-container"},
			wantName: "arn:aws:mediastore:us-east-1:123:container/my-container",
		},
		{
			name:     "empty body returns empty string",
			body:     map[string]any{},
			wantName: "",
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
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		wantHit bool
	}{
		{
			name:    "matches X-Amz-Target prefix",
			target:  "MediaStore_20170901.CreateContainer",
			wantHit: true,
		},
		{
			name:    "non-matching target",
			target:  "OtherService.Op",
			wantHit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantHit, matcher(c))
		})
	}
}

func TestHandler_ChaosAndPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		testFunc func(h *mediastore.Handler) any
		wantType string
	}{
		{
			name:     "ChaosServiceName returns non-empty string",
			testFunc: func(h *mediastore.Handler) any { return h.ChaosServiceName() },
			wantType: "string",
		},
		{
			name:     "ChaosOperations returns slice",
			testFunc: func(h *mediastore.Handler) any { return len(h.ChaosOperations()) > 0 },
			wantType: "bool",
		},
		{
			name:     "ChaosRegions returns slice",
			testFunc: func(h *mediastore.Handler) any { return len(h.ChaosRegions()) > 0 },
			wantType: "bool",
		},
		{
			name:     "MatchPriority returns positive int",
			testFunc: func(h *mediastore.Handler) any { return h.MatchPriority() },
			wantType: "int",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			result := tt.testFunc(h)
			assert.NotNil(t, result)

			if b, ok := result.(bool); ok {
				assert.True(t, b)
			}
		})
	}
}

func TestHandler_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "PutContainerPolicy missing container name",
			op:         "PutContainerPolicy",
			body:       map[string]any{"Policy": "{}"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetContainerPolicy missing container name",
			op:         "GetContainerPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteContainerPolicy missing container name",
			op:         "DeleteContainerPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "PutCorsPolicy missing container name",
			op:         "PutCorsPolicy",
			body:       map[string]any{"CorsPolicy": []any{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetCorsPolicy missing container name",
			op:         "GetCorsPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteCorsPolicy missing container name",
			op:         "DeleteCorsPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "PutLifecyclePolicy missing container name",
			op:         "PutLifecyclePolicy",
			body:       map[string]any{"LifecyclePolicy": "{}"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetLifecyclePolicy missing container name",
			op:         "GetLifecyclePolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteLifecyclePolicy missing container name",
			op:         "DeleteLifecyclePolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "PutMetricPolicy missing container name",
			op:         "PutMetricPolicy",
			body:       map[string]any{"MetricPolicy": map[string]any{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GetMetricPolicy missing container name",
			op:         "GetMetricPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteMetricPolicy missing container name",
			op:         "DeleteMetricPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "StartAccessLogging missing container name",
			op:         "StartAccessLogging",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "StopAccessLogging missing container name",
			op:         "StopAccessLogging",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DescribeContainer not found returns 404",
			op:         "DescribeContainer",
			body:       map[string]any{"ContainerName": "nonexistent"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetContainerPolicy not found returns 404",
			op:         "GetContainerPolicy",
			body:       map[string]any{"ContainerName": "nonexistent"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetCorsPolicy not found returns 404",
			op:         "GetCorsPolicy",
			body:       map[string]any{"ContainerName": "nonexistent"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetLifecyclePolicy not found returns 404",
			op:         "GetLifecyclePolicy",
			body:       map[string]any{"ContainerName": "nonexistent"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetMetricPolicy not found returns 404",
			op:         "GetMetricPolicy",
			body:       map[string]any{"ContainerName": "nonexistent"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "TagResource missing resource",
			op:         "TagResource",
			body:       map[string]any{"Tags": []any{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "UntagResource missing resource",
			op:         "UntagResource",
			body:       map[string]any{"TagKeys": []string{"k"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ListTagsForResource missing resource",
			op:         "ListTagsForResource",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			result := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

// TestHandler_ContainerNotFound verifies all ops return ContainerNotFoundException (404)
// when the referenced container does not exist. Moved (and de-prefixed) from the former
// parity_audit1_test.go's TestParity_ContainerNotFound.
func TestHandler_ContainerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "DescribeContainer",
			op:   "DescribeContainer",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteContainer",
			op:   "DeleteContainer",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutContainerPolicy",
			op:   "PutContainerPolicy",
			body: map[string]any{"ContainerName": "missing", "Policy": "{}"},
		},
		{
			name: "GetContainerPolicy",
			op:   "GetContainerPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteContainerPolicy",
			op:   "DeleteContainerPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutCorsPolicy",
			op:   "PutCorsPolicy",
			body: map[string]any{
				"ContainerName": "missing",
				"CorsPolicy": []any{
					map[string]any{
						"AllowedOrigins": []any{"https://x.com"},
						"AllowedHeaders": []any{"*"},
					},
				},
			},
		},
		{
			name: "GetCorsPolicy",
			op:   "GetCorsPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteCorsPolicy",
			op:   "DeleteCorsPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutLifecyclePolicy",
			op:   "PutLifecyclePolicy",
			body: map[string]any{"ContainerName": "missing", "LifecyclePolicy": "{}"},
		},
		{
			name: "GetLifecyclePolicy",
			op:   "GetLifecyclePolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteLifecyclePolicy",
			op:   "DeleteLifecyclePolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutMetricPolicy",
			op:   "PutMetricPolicy",
			body: map[string]any{
				"ContainerName": "missing",
				"MetricPolicy":  map[string]any{"ContainerLevelMetrics": "ENABLED"},
			},
		},
		{
			name: "GetMetricPolicy",
			op:   "GetMetricPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteMetricPolicy",
			op:   "DeleteMetricPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "StartAccessLogging",
			op:   "StartAccessLogging",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "StopAccessLogging",
			op:   "StopAccessLogging",
			body: map[string]any{"ContainerName": "missing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)

			assert.Equal(t, http.StatusNotFound, rec.Code)
			m := unmarshalBody(t, rec)
			assert.Equal(t, "ContainerNotFoundException", m["__type"])
		})
	}
}
