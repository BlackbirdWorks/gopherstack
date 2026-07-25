package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestServiceLinkedAnalyzerLifecycle verifies Create/Delete service-linked analyzer.
func TestServiceLinkedAnalyzerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
		name string
	}{
		{
			name: "create_service_linked_analyzer",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPut, "/service-linked-analyzer", map[string]any{
					"type": "ACCOUNT_UNUSED_ACCESS",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.True(t, strings.HasPrefix(resp["arn"], "arn:aws:access-analyzer:"))
			},
		},
		{
			name: "delete_service_linked_analyzer",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				mustAnalyzer(t, b, "sla-to-delete")

				rec := doRequest(t, h, http.MethodDelete, "/service-linked-analyzer/sla-to-delete", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_missing_analyzer",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodDelete, "/service-linked-analyzer/no-such", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestUpdateAnalyzer verifies PUT /analyzer/{name} updates an analyzer.
func TestUpdateAnalyzer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "update_existing",
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				mustAnalyzer(t, b, "upd-analyzer")

				return "upd-analyzer"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update_missing",
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return "no-such-analyzer"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			name := tt.setupFn(b)

			rec := doRequest(t, h, http.MethodPut, "/analyzer/"+name, map[string]any{
				"configuration": map[string]any{},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			// UpdateAnalyzerOutput carries only "configuration" -- unlike
			// CreateAnalyzer/CreateServiceLinkedAnalyzer, it has no "arn" member.
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, hasArn := resp["arn"]
			assert.False(t, hasArn, `UpdateAnalyzerOutput has no "arn" member`)
		})
	}
}

// TestAnalyzerConfiguration verifies the AnalyzerConfiguration union
// (accepted by CreateAnalyzer/UpdateAnalyzer, returned by
// GetAnalyzer/UpdateAnalyzer, omitted by ListAnalyzers) is stored and
// echoed back opaquely rather than silently dropped.
func TestAnalyzerConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("create_then_get_roundtrips_configuration", func(t *testing.T) {
		t.Parallel()

		b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
		h := accessanalyzer.NewHandler(b)

		rec := doRequest(t, h, http.MethodPut, "/analyzer", map[string]any{
			"analyzerName": "cfg-analyzer",
			"type":         "ACCOUNT_UNUSED_ACCESS",
			"configuration": map[string]any{
				"unusedAccess": map[string]any{"unusedAccessAge": 90},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		getRec := doRequest(t, h, http.MethodGet, "/analyzer/cfg-analyzer", nil)
		require.Equal(t, http.StatusOK, getRec.Code)

		var getResp map[string]any
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
		analyzer, ok := getResp["analyzer"].(map[string]any)
		require.True(t, ok)

		cfg, ok := analyzer["configuration"].(map[string]any)
		require.True(t, ok, "GetAnalyzer must include configuration when one was specified")
		unused, ok := cfg["unusedAccess"].(map[string]any)
		require.True(t, ok)
		assert.InDelta(t, float64(90), unused["unusedAccessAge"], 0.0001)

		// ListAnalyzers omits "configuration" even when the analyzer has one.
		listRec := doRequest(t, h, http.MethodGet, "/analyzer", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		analyzers, ok := listResp["analyzers"].([]any)
		require.True(t, ok)
		require.Len(t, analyzers, 1)
		summary, ok := analyzers[0].(map[string]any)
		require.True(t, ok)
		_, hasCfg := summary["configuration"]
		assert.False(t, hasCfg, "ListAnalyzers must omit configuration")
	})

	t.Run("update_replaces_configuration", func(t *testing.T) {
		t.Parallel()

		b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
		h := accessanalyzer.NewHandler(b)
		mustAnalyzer(t, b, "cfg-update-analyzer")

		rec := doRequest(t, h, http.MethodPut, "/analyzer/cfg-update-analyzer", map[string]any{
			"configuration": map[string]any{
				"unusedAccess": map[string]any{"unusedAccessAge": 30},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		cfg, ok := resp["configuration"].(map[string]any)
		require.True(t, ok)
		unused, ok := cfg["unusedAccess"].(map[string]any)
		require.True(t, ok)
		assert.InDelta(t, float64(30), unused["unusedAccessAge"], 0.0001)
	})
}

// TestCreateAnalyzerInlineArchiveRules verifies CreateAnalyzer's optional
// "archiveRules" array creates real archive rules on the new analyzer, the
// same as a follow-up CreateArchiveRule call would.
func TestCreateAnalyzerInlineArchiveRules(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)

	rec := doRequest(t, h, http.MethodPut, "/analyzer", map[string]any{
		"analyzerName": "inline-ar-analyzer",
		"type":         "ACCOUNT",
		"archiveRules": []map[string]any{
			{
				"ruleName": "auto-archive-test",
				"filter": map[string]any{
					"resourceType": map[string]any{"eq": []string{"AWS::S3::Bucket"}},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rules, err := b.ListArchiveRules("inline-ar-analyzer")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "auto-archive-test", rules[0].RuleName)
}
