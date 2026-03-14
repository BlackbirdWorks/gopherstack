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
	"github.com/blackbirdworks/gopherstack/services/support"
)

func newTestSupportHandler(t *testing.T) *support.Handler {
	t.Helper()

	return support.NewHandler(support.NewInMemoryBackend())
}

func doSupportRequest(t *testing.T, h *support.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSSupport_20130415."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSupport_Name(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	assert.Equal(t, "Support", h.Name())
}

func TestSupport_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCase")
	assert.Contains(t, ops, "DescribeCases")
	assert.Contains(t, ops, "ResolveCase")
}

func TestSupport_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestSupport_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matching target",
			target:    "AWSSupport_20130415.CreateCase",
			wantMatch: true,
		},
		{
			name:      "non-matching target",
			target:    "OtherService.Action",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestSupport_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSSupport_20130415.CreateCase")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateCase", h.ExtractOperation(c))
}

func TestSupport_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	e := echo.New()

	body := `{"subject":"my ticket"}`
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	req.Header.Set("X-Amz-Target", "AWSSupport_20130415.CreateCase")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my ticket", h.ExtractResource(c))
}

func TestSupport_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		action           string
		wantContains     []string
		wantNonEmptyKeys []string
		wantCode         int
	}{
		{
			name:   "CreateCase",
			action: "CreateCase",
			body: map[string]any{
				"subject":           "My issue",
				"serviceCode":       "amazon-s3",
				"categoryCode":      "data-management",
				"severityCode":      "low",
				"communicationBody": "I have a question about S3.",
			},
			wantCode:         http.StatusOK,
			wantNonEmptyKeys: []string{"caseId"},
		},
		{
			name:     "CreateCase_MissingSubject",
			action:   "CreateCase",
			body:     map[string]any{"serviceCode": "amazon-s3"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ResolveCase_NotFound",
			action:   "ResolveCase",
			body:     map[string]any{"caseId": "case-does-not-exist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, tt.action, tt.body)

			require.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}

			if len(tt.wantNonEmptyKeys) > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				for _, key := range tt.wantNonEmptyKeys {
					assert.NotEmpty(t, resp[key])
				}
			}
		})
	}
}

func TestSupport_DescribeCases(t *testing.T) {
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

func TestSupport_ResolveCase(t *testing.T) {
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

func TestSupport_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	assert.Equal(t, "Support", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestSupport_AddCommunicationToCase(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	createRec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject": "Comm test",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "add communication",
			body:     map[string]any{"caseId": caseID, "communicationBody": "Hello support"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing caseId",
			body:     map[string]any{"communicationBody": "Hello"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown caseId",
			body:     map[string]any{"caseId": "nonexistent", "communicationBody": "Hello"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, h, "AddCommunicationToCase", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSupport_DescribeCommunications(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	createRec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject": "Comm test",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	caseID := createResp["caseId"].(string)

	addRec := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "Hello support",
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	tests := []struct {
		body          map[string]any
		name          string
		wantCode      int
		wantCommCount int
	}{
		{
			name:          "describe communications",
			body:          map[string]any{"caseId": caseID},
			wantCode:      http.StatusOK,
			wantCommCount: 1,
		},
		{
			name:     "missing caseId",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown caseId",
			body:     map[string]any{"caseId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, h, "DescribeCommunications", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCommCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				comms, ok := resp["communications"].([]any)
				require.True(t, ok)
				assert.Len(t, comms, tt.wantCommCount)
			}
		})
	}
}

func TestSupport_DescribeTrustedAdvisorChecks(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(t, h, "DescribeTrustedAdvisorChecks", map[string]any{
		"language": "en",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	checks, ok := resp["checks"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, checks)
}

func TestSupport_AddAttachmentsToSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "new attachment set",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "with existing set id",
			body:     map[string]any{"attachmentSetId": "existing-set-id"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "AddAttachmentsToSet", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["attachmentSetId"])
				assert.NotEmpty(t, resp["expiryTime"])
			}
		})
	}
}
