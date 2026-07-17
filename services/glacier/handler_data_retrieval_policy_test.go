package glacier_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataRetrievalPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
		wantPolicy bool
	}{
		{
			name:       "get_policy",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantPolicy: true,
		},
		{
			name:       "set_policy",
			method:     http.MethodPut,
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":10737418240}]}}`
			rec := doRequest(t, h, tt.method, "/-/policies/data-retrieval", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantPolicy {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Policy")
			}
		})
	}
}

// ----------------------------------------
// ExtractOperation and ExtractResource
// ----------------------------------------

func TestDataRetrievalPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "set_then_get",
			policy: `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":1073741824}]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(
				t,
				h,
				http.MethodPut,
				"/"+testAccountID+"/policies/data-retrieval",
				tt.policy,
			)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doRequest(t, h, http.MethodGet, "/"+testAccountID+"/policies/data-retrieval", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.NotEmpty(t, got)
		})
	}
}

func TestDataRetrievalPolicy_DefaultFreeTier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStrategy string
	}{
		{name: "default_free_tier", wantStrategy: "FreeTier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/policies/data-retrieval", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

			policy, ok := got["Policy"].(map[string]any)
			require.True(t, ok)
			rules, ok := policy["Rules"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, rules)

			firstRule, ok := rules[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStrategy, firstRule["Strategy"])
		})
	}
}

func TestDataRetrievalPolicy_StrategyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		wantStatus int
	}{
		{
			name:       "free_tier_valid",
			policy:     `{"Policy":{"Rules":[{"Strategy":"FreeTier"}]}}`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "none_valid",
			policy:     `{"Policy":{"Rules":[{"Strategy":"None"}]}}`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "bytes_per_hour_with_value_valid",
			policy:     `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":1048576}]}}`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "bytes_per_hour_without_value_rejected",
			policy:     `{"Policy":{"Rules":[{"Strategy":"BytesPerHour"}]}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bytes_per_hour_zero_rejected",
			policy:     `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":0}]}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown_strategy_rejected",
			policy:     `{"Policy":{"Rules":[{"Strategy":"Unlimited"}]}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_rules_valid",
			policy:     `{"Policy":{"Rules":[]}}`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "invalid_json_rejected",
			policy:     `{not json}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, http.MethodPut,
				"/"+testAccountID+"/policies/data-retrieval", tt.policy)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestDataRetrievalPolicy_GetDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/policies/data-retrieval", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policy := resp["Policy"].(map[string]any)
	rules := policy["Rules"].([]any)
	require.Len(t, rules, 1)

	rule := rules[0].(map[string]any)
	assert.Equal(t, "FreeTier", rule["Strategy"])
}

// -------------------------------------------------------------------------
// Issue 19: ProvisionedCapacity limit (max 2 per account)
// -------------------------------------------------------------------------

func TestDataRetrievalPolicy_BytesPerHour(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bytesPerHour int64
		wantOK       bool
	}{
		{name: "valid_bytes_per_hour", bytesPerHour: 1073741824, wantOK: true},
		{name: "zero_bytes_per_hour_rejected", bytesPerHour: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := fmt.Sprintf(`{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":%d}]}}`,
				tt.bytesPerHour)

			rec := doRequestWithHeaders(t, h, http.MethodPut,
				"/"+testAccountID+"/policies/data-retrieval", body, nil)
			if tt.wantOK {
				assert.Equal(t, http.StatusNoContent, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestDataRetrievalPolicy_SetAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
	}{
		{name: "set_none_and_get", strategy: "None"},
		{name: "set_free_tier_and_get", strategy: "FreeTier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := `{"Policy":{"Rules":[{"Strategy":"` + tt.strategy + `"}]}}`
			rec := doRequestWithHeaders(t, h, http.MethodPut,
				"/"+testAccountID+"/policies/data-retrieval", body, nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/policies/data-retrieval", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policy := resp["Policy"].(map[string]any)
			rules := policy["Rules"].([]any)
			require.NotEmpty(t, rules)
			rule := rules[0].(map[string]any)
			assert.Equal(t, tt.strategy, rule["Strategy"])
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. ProvisionedCapacity
// ─────────────────────────────────────────────────────────────────────────────
