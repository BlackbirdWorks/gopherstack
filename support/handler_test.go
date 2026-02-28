package support_test

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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/support"
)

func newTestSupportHandler(t *testing.T) *support.Handler {
	t.Helper()

	return support.NewHandler(support.NewInMemoryBackend(), slog.Default())
}

func doSupportRequest(t *testing.T, h *support.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonSupport."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSupport_Handler_Name(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	assert.Equal(t, "Support", h.Name())
}

func TestSupport_Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCase")
	assert.Contains(t, ops, "DescribeCases")
	assert.Contains(t, ops, "ResolveCase")
}

func TestSupport_Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestSupport_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonSupport.CreateCase")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))

	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "OtherService.Action")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, h.RouteMatcher()(c2))
}

func TestSupport_Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonSupport.CreateCase")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateCase", h.ExtractOperation(c))
}

func TestSupport_Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	e := echo.New()

	body := `{"subject":"my ticket"}`
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	req.Header.Set("X-Amz-Target", "AmazonSupport.CreateCase")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my ticket", h.ExtractResource(c))
}

func TestSupport_Handler_CreateCase(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject":           "My issue",
		"serviceCode":       "amazon-s3",
		"categoryCode":      "data-management",
		"severityCode":      "low",
		"communicationBody": "I have a question about S3.",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["caseId"])
}

func TestSupport_Handler_CreateCase_MissingSubject(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"serviceCode": "amazon-s3",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSupport_Handler_DescribeCases(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec1 := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject": "Case One",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	rec := doSupportRequest(t, h, "DescribeCases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cases, ok := resp["cases"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, cases)

	// Describe by caseId
	rec2 := doSupportRequest(t, h, "DescribeCases", map[string]any{
		"caseIdList": []string{caseID},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	cases2, ok := resp2["cases"].([]any)
	require.True(t, ok)
	assert.Len(t, cases2, 1)
}

func TestSupport_Handler_ResolveCase(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	createRec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject": "Resolve me",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	rec := doSupportRequest(t, h, "ResolveCase", map[string]any{
		"caseId": caseID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "opened", resp["initialCaseStatus"])
	assert.Equal(t, "resolved", resp["finalCaseStatus"])
}

func TestSupport_Handler_ResolveCase_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(t, h, "ResolveCase", map[string]any{
		"caseId": "case-does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSupport_Handler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(t, h, "UnknownAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSupport_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	assert.Equal(t, "Support", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
