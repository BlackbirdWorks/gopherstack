package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAPIKeys_SummaryShape locks that ListAPIKeys' APIKeySummary items
// never carry a Scope key. The real deserializer
// (awsAwsjson11_deserializeDocumentAPIKeySummary) doesn't recognise it, so an
// SDK client can't observe the leak -- only a raw-body assertion can
// (gopherstack, over-wide response class census).
func TestListAPIKeys_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	_, err := client.CreateAPIKey(t.Context(), &wafv2sdk.CreateAPIKeyInput{
		Scope:        types.ScopeRegional,
		TokenDomains: []string{"example.com"},
	})
	require.NoError(t, err)

	rec := doWafv2Request(t, h, "ListAPIKeys", map[string]any{"Scope": "REGIONAL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, ok := resp["APIKeySummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)
	item, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	assert.NotContains(t, item, "Scope", "APIKeySummary has no Scope member")
	assert.Contains(t, item, "TokenDomains")
	assert.Contains(t, item, "CreationTimestamp")
}

// TestGetDecryptedAPIKey_SummaryShape locks the same Scope leak fix for
// GetDecryptedAPIKeyOutput, found alongside ListAPIKeys' (same shape family).
func TestGetDecryptedAPIKey_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
		"Scope":        "REGIONAL",
		"TokenDomains": []string{"example.com"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	apiKey, ok := createResp["APIKey"].(string)
	require.True(t, ok)

	rec := doWafv2Request(t, h, "GetDecryptedAPIKey", map[string]any{
		"Scope":  "REGIONAL",
		"APIKey": apiKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotContains(t, resp, "Scope", "GetDecryptedAPIKeyOutput has no Scope member")
	assert.Contains(t, resp, "TokenDomains")
	assert.Contains(t, resp, "CreationTimestamp")
}
