package xray

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doWhiteboxRequest(t *testing.T, h *Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.RequestURI = path

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestCreateSamplingRule_RuleLimitExceeded verifies the previously-unenforced
// RuleLimitExceededException cap on the number of sampling rules per account.
func TestCreateSamplingRule_RuleLimitExceeded(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(b)

	// Seed up to the limit directly (bypassing validation) for speed; the Default
	// rule already counts toward the limit.
	for i := b.SamplingRuleCount(); i < maxSamplingRules; i++ {
		b.AddSamplingRuleInternal(SamplingRule{RuleName: fmt.Sprintf("seed-%d", i), Priority: 1})
	}

	rec := doWhiteboxRequest(t, h, "/CreateSamplingRule", map[string]any{
		"SamplingRule": map[string]any{"RuleName": "one-too-many", "Priority": 1, "FixedRate": 0.1},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "RuleLimitExceededException", resp["__type"])
}
