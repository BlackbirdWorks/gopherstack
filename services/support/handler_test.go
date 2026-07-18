package support_test

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// decodeSupportResponse decodes a JSON response body into a generic map, for
// tests that inspect specific fields. Shared across the family test files.
func decodeSupportResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

	return result
}

// createAccurateCase creates a fully-populated case via the HTTP handler and
// returns its caseId. Shared across the family test files that need a
// realistic seeded case.
func createAccurateCase(t *testing.T, h *support.Handler, extra map[string]any) string {
	t.Helper()

	body := map[string]any{
		"subject":           "API fidelity case",
		"communicationBody": "Initial customer report",
		"serviceCode":       "amazon-s3",
		"categoryCode":      "general-guidance",
		"severityCode":      "normal",
		"language":          "en",
		"issueType":         "technical",
	}
	maps.Copy(body, extra)
	rec := doSupportRequest(t, h, "CreateCase", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return decodeSupportResponse(t, rec)["caseId"].(string)
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

func TestSupport_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)
	assert.Equal(t, 16, support.HandlerOpsLen(h))
}

func TestSupport_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
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

	tests := []struct {
		body string
		want string
	}{
		{body: `{"subject":"my ticket"}`, want: "my ticket"},
		{body: `{"checkId":"Pfx0RwqBli"}`, want: "Pfx0RwqBli"},
		{body: `{"attachmentId":"att-1"}`, want: "att-1"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(tt.body)))
			req.Header.Set("X-Amz-Target", "AWSSupport_20130415.CreateCase")
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
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
				resp := decodeSupportResponse(t, rec)

				for _, key := range tt.wantNonEmptyKeys {
					assert.NotEmpty(t, resp[key])
				}
			}
		})
	}
}
