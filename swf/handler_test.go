package swf_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/swf"
)

func newTestSWFHandler(t *testing.T) *swf.Handler {
	t.Helper()

	return swf.NewHandler(swf.NewInMemoryBackend(), slog.Default())
}

func doSWFRequest(t *testing.T, h *swf.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SimpleWorkflowService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSWFHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "RegisterDomain",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)

				rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{
					"name":        "my-domain",
					"description": "test",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "ListDomains",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d1"})
				doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d2"})

				rec := doSWFRequest(t, h, "ListDomains", map[string]any{"registrationStatus": "REGISTERED"})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "domainInfos")
			},
		},
		{
			name: "DeprecateDomain",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "my-domain"})

				rec := doSWFRequest(t, h, "DeprecateDomain", map[string]any{"name": "my-domain"})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DeprecateDomain_NotFound",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)

				rec := doSWFRequest(t, h, "DeprecateDomain", map[string]any{"name": "nonexistent"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "RegisterWorkflowType",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)

				rec := doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
					"domain":  "my-domain",
					"name":    "my-workflow",
					"version": "1.0",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "ListWorkflowTypes",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{"domain": "d1", "name": "wf1", "version": "1.0"})

				rec := doSWFRequest(t, h, "ListWorkflowTypes", map[string]any{"domain": "d1"})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "typeInfos")
			},
		},
		{
			name: "StartWorkflowExecution",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)

				rec := doSWFRequest(t, h, "StartWorkflowExecution", map[string]any{
					"domain":     "my-domain",
					"workflowId": "wf-001",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["runId"])
			},
		},
		{
			name: "DescribeWorkflowExecution",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "StartWorkflowExecution", map[string]any{"domain": "d1", "workflowId": "wf-001"})

				rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
					"domain":    "d1",
					"execution": map[string]any{"workflowId": "wf-001"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "executionInfo")
			},
		},
		{
			name: "UnknownAction",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)

				rec := doSWFRequest(t, h, "UnknownAction", nil)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				matcher := h.RouteMatcher()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "SimpleWorkflowService.RegisterDomain")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.True(t, matcher(c))
			},
		},
		{
			name: "Provider",
			run: func(t *testing.T) {
				p := &swf.Provider{}
				assert.Equal(t, "SWF", p.Name())
			},
		},
		{
			name: "Name",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				assert.Equal(t, "SWF", h.Name())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "RegisterDomain")
				assert.Contains(t, ops, "ListDomains")
				assert.Contains(t, ops, "DeprecateDomain")
				assert.Contains(t, ops, "RegisterWorkflowType")
				assert.Contains(t, ops, "ListWorkflowTypes")
				assert.Contains(t, ops, "StartWorkflowExecution")
				assert.Contains(t, ops, "DescribeWorkflowExecution")
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				assert.Equal(t, 100, h.MatchPriority())
			},
		},
		{
			name: "ExtractOperation",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "SimpleWorkflowService.RegisterDomain")
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "RegisterDomain", h.ExtractOperation(c))

				// No target → "Unknown"
				req2 := httptest.NewRequest(http.MethodPost, "/", nil)
				c2 := e.NewContext(req2, httptest.NewRecorder())
				assert.Equal(t, "Unknown", h.ExtractOperation(c2))
			},
		},
		{
			name: "ExtractResource",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				e := echo.New()

				// name field
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"my-domain"}`))
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "my-domain", h.ExtractResource(c))

				// domain field (fallback)
				req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"domain":"test-domain"}`))
				c2 := e.NewContext(req2, httptest.NewRecorder())
				assert.Equal(t, "test-domain", h.ExtractResource(c2))
			},
		},
		{
			name: "RouteMatcher_NoMatch",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				matcher := h.RouteMatcher()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "Firehose_20150804.CreateDeliveryStream")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.False(t, matcher(c))
			},
		},
		{
			name: "RegisterDomain_AlreadyExists",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "my-domain"})

				rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "my-domain"})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "RegisterDomain_Deprecated",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "my-domain"})
				doSWFRequest(t, h, "DeprecateDomain", map[string]any{"name": "my-domain"})

				// Re-registering a deprecated domain returns ErrDeprecated
				rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "my-domain"})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "RegisterWorkflowType_AlreadyExists",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)
				doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
					"domain":  "my-domain",
					"name":    "my-wf",
					"version": "1.0",
				})

				rec := doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
					"domain":  "my-domain",
					"name":    "my-wf",
					"version": "1.0",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "DescribeWorkflowExecution_NotFound",
			run: func(t *testing.T) {
				h := newTestSWFHandler(t)

				rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
					"domain":    "d1",
					"execution": map[string]any{"workflowId": "nonexistent"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "Provider_Init",
			run: func(t *testing.T) {
				p := &swf.Provider{}
				ctx := &service.AppContext{Logger: slog.Default()}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				assert.NotNil(t, svc)
				assert.Equal(t, "SWF", svc.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
