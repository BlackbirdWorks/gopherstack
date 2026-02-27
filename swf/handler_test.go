package swf_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestSWF_Handler_RegisterDomain(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{
		"name":        "my-domain",
		"description": "test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSWF_Handler_ListDomains(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d1"})
	doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d2"})

	rec := doSWFRequest(t, h, "ListDomains", map[string]any{"registrationStatus": "REGISTERED"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "domainInfos")
}

func TestSWF_Handler_DeprecateDomain(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "my-domain"})

	rec := doSWFRequest(t, h, "DeprecateDomain", map[string]any{"name": "my-domain"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSWF_Handler_DeprecateDomain_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	rec := doSWFRequest(t, h, "DeprecateDomain", map[string]any{"name": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSWF_Handler_RegisterWorkflowType(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	rec := doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
		"domain":  "my-domain",
		"name":    "my-workflow",
		"version": "1.0",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSWF_Handler_ListWorkflowTypes(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{"domain": "d1", "name": "wf1", "version": "1.0"})

	rec := doSWFRequest(t, h, "ListWorkflowTypes", map[string]any{"domain": "d1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "typeInfos")
}

func TestSWF_Handler_StartWorkflowExecution(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	rec := doSWFRequest(t, h, "StartWorkflowExecution", map[string]any{
		"domain":     "my-domain",
		"workflowId": "wf-001",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["runId"])
}

func TestSWF_Handler_DescribeWorkflowExecution(t *testing.T) {
	t.Parallel()

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
}

func TestSWF_Handler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	rec := doSWFRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSWF_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "SimpleWorkflowService.RegisterDomain")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

func TestSWF_Provider(t *testing.T) {
	t.Parallel()

	p := &swf.Provider{}
	assert.Equal(t, "SWF", p.Name())
}
