package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

func TestSupport_DescribeTrustedAdvisorChecks(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(t, h, "DescribeTrustedAdvisorChecks", map[string]any{
		"language": "en",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := decodeSupportResponse(t, rec)
	checks, ok := resp["checks"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, checks)
}

// TestSupport_DescribeTrustedAdvisorChecks_LanguageSet verifies the full
// documented Trusted Advisor language set is accepted -- distinct from (and
// larger than) the 4-language set Support case handling accepts. Real AWS
// documents 11 codes for DescribeTrustedAdvisorChecks; reusing the
// case-language validator here previously rejected valid requests such as
// language=fr.
func TestSupport_DescribeTrustedAdvisorChecks_LanguageSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		language string
		wantCode int
	}{
		{"english", "en", http.StatusOK},
		{"french", "fr", http.StatusOK},
		{"german", "de", http.StatusOK},
		{"spanish", "es", http.StatusOK},
		{"indonesian", "id", http.StatusOK},
		{"italian", "it", http.StatusOK},
		{"brazilian_portuguese", "pt_BR", http.StatusOK},
		{"traditional_chinese", "zh_TW", http.StatusOK},
		{"unsupported_code", "xx", http.StatusBadRequest},
		{"empty", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeTrustedAdvisorChecks", map[string]any{"language": tt.language})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSupport_DescribeTrustedAdvisorCheckRefreshStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *support.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus string
		checkID    string
		wantCode   int
	}{
		{
			name:       "check_not_refreshed",
			body:       map[string]any{"checkIds": []string{"Pfx0RwqBli"}},
			wantCode:   http.StatusOK,
			wantStatus: "none",
			checkID:    "Pfx0RwqBli",
		},
		{
			name: "check_after_refresh",
			setup: func(b *support.InMemoryBackend) {
				_, err := b.RefreshTrustedAdvisorCheck("Pfx0RwqBli")
				if err != nil {
					panic(err)
				}
			},
			body:       map[string]any{"checkIds": []string{"Pfx0RwqBli"}},
			wantCode:   http.StatusOK,
			wantStatus: "enqueued",
			checkID:    "Pfx0RwqBli",
		},
		{
			name:     "empty_check_ids",
			body:     map[string]any{"checkIds": []string{}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := support.NewInMemoryBackend()
			h := support.NewHandler(b)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckRefreshStatuses", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantStatus != "" {
				resp := decodeSupportResponse(t, rec)
				statuses, ok := resp["statuses"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, statuses)
				status, ok := statuses[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.checkID, status["checkId"])
				assert.Equal(t, tt.wantStatus, status["status"])
			}
		})
	}
}

func TestSupport_DescribeTrustedAdvisorCheckResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "valid_check_id",
			body:     map[string]any{"checkId": "Pfx0RwqBli", "language": "en"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_check_id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckResult", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeSupportResponse(t, rec)
				result, ok := resp["result"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, result["checkId"])
				assert.NotEmpty(t, result["status"])
				assert.NotEmpty(t, result["timestamp"])
				assert.NotNil(t, result["resourcesSummary"])
			}
		})
	}
}

func TestSupport_DescribeTrustedAdvisorCheckSummaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name:     "two_checks",
			body:     map[string]any{"checkIds": []string{"Pfx0RwqBli", "DAvU99Dc4C"}},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name:     "empty_list",
			body:     map[string]any{"checkIds": []string{}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckSummaries", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeSupportResponse(t, rec)
				summaries, ok := resp["summaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, tt.wantLen)
			}
		})
	}
}

func TestSupport_RefreshTrustedAdvisorCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "valid_check_id",
			body:     map[string]any{"checkId": "Pfx0RwqBli"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_check_id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "RefreshTrustedAdvisorCheck", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeSupportResponse(t, rec)
				status, ok := resp["status"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Pfx0RwqBli", status["checkId"])
				assert.Equal(t, "enqueued", status["status"])
				assert.NotZero(t, status["millisUntilNextRefreshable"])
			}
		})
	}
}

func TestSupport_RefreshAndDescribeRefreshStatus(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	// Refresh a check.
	refreshRec := doSupportRequest(t, h, "RefreshTrustedAdvisorCheck", map[string]any{
		"checkId": "DAvU99Dc4C",
	})
	require.Equal(t, http.StatusOK, refreshRec.Code)

	// Describe refresh status.
	statusRec := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckRefreshStatuses", map[string]any{
		"checkIds": []string{"DAvU99Dc4C"},
	})
	require.Equal(t, http.StatusOK, statusRec.Code)

	resp := decodeSupportResponse(t, statusRec)
	statuses, ok := resp["statuses"].([]any)
	require.True(t, ok)
	require.Len(t, statuses, 1)
	status, ok := statuses[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "DAvU99Dc4C", status["checkId"])
	assert.Equal(t, "enqueued", status["status"])
}

// TestSupport_DescribeTrustedAdvisorCheckResult_DefaultStatus verifies cost_optimizing checks default to "warning".
func TestSupport_DescribeTrustedAdvisorCheckResult_DefaultStatus(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	tests := []struct {
		name       string
		checkID    string
		wantStatus string
	}{
		// DAvU99Dc4C = "Low Utilization Amazon EC2 Instances" (cost_optimizing)
		{"cost_optimizing_warning", "DAvU99Dc4C", "warning"},
		// N430c450f2 = "Unassociated Elastic IP Addresses" (cost_optimizing)
		{"cost_optimizing_eip", "N430c450f2", "warning"},
		// hjLMh88uM8 = "MFA on Root Account" (security)
		{"security_ok", "hjLMh88uM8", "ok"},
		// Pfx0RwqBli = "Service Limits" (service_limits)
		{"service_limits_ok", "Pfx0RwqBli", "ok"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := b.DescribeTrustedAdvisorCheckResult(tc.checkID, "en")
			require.NotNil(t, result)
			assert.Equal(t, tc.checkID, result.CheckID)
			assert.Equal(t, tc.wantStatus, result.Status)
		})
	}
}

// TestSupport_DescribeTrustedAdvisorCheckSummaries_DeriveFromResults verifies summaries reflect check result state.
func TestSupport_DescribeTrustedAdvisorCheckSummaries_DeriveFromResults(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	// With no stored results, summaries default to "ok".
	summaries := b.DescribeTrustedAdvisorCheckSummaries([]string{"DAvU99Dc4C", "hjLMh88uM8"})
	require.Len(t, summaries, 2)

	for _, s := range summaries {
		assert.Equal(t, "ok", s.Status)
		assert.False(t, s.HasFlaggedResources)
	}
}

// TestSupport_RefreshTrustedAdvisorCheck_PollProgression verifies enqueued->processing->success transition.
// Subtests are ordered and share backend state — they must NOT run in parallel.
//
//nolint:paralleltest // subtests share sequential backend state (poll counter); cannot run in parallel
func TestSupport_RefreshTrustedAdvisorCheck_PollProgression(t *testing.T) {
	b := support.NewInMemoryBackend()
	checkID := "hjLMh88uM8"

	_, err := b.RefreshTrustedAdvisorCheck(checkID)
	require.NoError(t, err)

	// Poll 1: status must be enqueued.
	statuses := b.DescribeTrustedAdvisorCheckRefreshStatuses([]string{checkID})
	require.Len(t, statuses, 1)
	assert.Equal(t, "enqueued", statuses[0].Status)

	// Poll 2: must advance to processing.
	statuses = b.DescribeTrustedAdvisorCheckRefreshStatuses([]string{checkID})
	require.Len(t, statuses, 1)
	assert.Equal(t, "processing", statuses[0].Status)

	// Poll 3: must settle to success.
	statuses = b.DescribeTrustedAdvisorCheckRefreshStatuses([]string{checkID})
	require.Len(t, statuses, 1)
	assert.Equal(t, "success", statuses[0].Status)

	// Poll 4: must remain success (write lock skipped — pure read path).
	statuses = b.DescribeTrustedAdvisorCheckRefreshStatuses([]string{checkID})
	require.Len(t, statuses, 1)
	assert.Equal(t, "success", statuses[0].Status)
}

// TestSupport_DescribeTrustedAdvisorCheckRefreshStatuses_UnknownCheck verifies unknown checks return "none".
func TestSupport_DescribeTrustedAdvisorCheckRefreshStatuses_UnknownCheck(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	statuses := b.DescribeTrustedAdvisorCheckRefreshStatuses([]string{"unknown-check-id"})
	require.Len(t, statuses, 1)
	assert.Equal(t, "none", statuses[0].Status)
	assert.Equal(t, int64(0), statuses[0].MillisUntilNextRefreshable)
}

// TestSupport_RefreshTrustedAdvisorCheck_StatusEvictionAtCap verifies the refresh status cap evicts when full.
func TestSupport_RefreshTrustedAdvisorCheck_StatusEvictionAtCap(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	checks := b.DescribeTrustedAdvisorChecks()

	// Stress the cap boundary using synthetic IDs.
	for i := range 200 {
		for _, c := range checks {
			syntheticID := c.ID + "-" + c.Category + "-" + string(rune('a'+i%26))
			_, _ = b.RefreshTrustedAdvisorCheck(syntheticID)
		}
	}

	// Original checks must still be refreshable without panic.
	for _, c := range checks {
		status, refreshErr := b.RefreshTrustedAdvisorCheck(c.ID)
		require.NoError(t, refreshErr)
		assert.NotEmpty(t, status.CheckID)
	}
}

func TestSupport_TrustedAdvisor_ValidationAndRefreshLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "checks require language",
			action:   "DescribeTrustedAdvisorChecks",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown refresh check",
			action:   "RefreshTrustedAdvisorCheck",
			body:     map[string]any{"checkId": "unknown"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "summaries require ids",
			action:   "DescribeTrustedAdvisorCheckSummaries",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "result validates id",
			action:   "DescribeTrustedAdvisorCheckResult",
			body:     map[string]any{"checkId": "unknown"},
			wantCode: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doSupportRequest(t, newTestSupportHandler(t), tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}

	h := newTestSupportHandler(t)
	refresh := doSupportRequest(t, h, "RefreshTrustedAdvisorCheck", map[string]any{"checkId": "Pfx0RwqBli"})
	require.Equal(t, http.StatusOK, refresh.Code)
	for _, expected := range []string{"enqueued", "processing", "success"} {
		status := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckRefreshStatuses", map[string]any{
			"checkIds": []string{"Pfx0RwqBli"},
		})
		require.Equal(t, http.StatusOK, status.Code)
		statuses := decodeSupportResponse(t, status)["statuses"].([]any)
		assert.Equal(t, expected, statuses[0].(map[string]any)["status"])
	}
	result := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckResult", map[string]any{
		"checkId": "Pfx0RwqBli", "language": "en",
	})
	assert.NotNil(t, decodeSupportResponse(t, result)["result"].(map[string]any)["categorySpecificSummary"])
}

// TestSupport_DescribeTrustedAdvisorChecks_MinimumCount verifies at least 8 checks returned.
func TestSupport_DescribeTrustedAdvisorChecks_MinimumCount(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	checks := b.DescribeTrustedAdvisorChecks()
	assert.GreaterOrEqual(t, len(checks), 8, "expected at least 8 Trusted Advisor checks")
}

// TestSupport_DescribeTrustedAdvisorChecks_Categories verifies multiple categories present.
func TestSupport_DescribeTrustedAdvisorChecks_Categories(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	checks := b.DescribeTrustedAdvisorChecks()

	cats := make(map[string]bool)
	for _, c := range checks {
		cats[c.Category] = true
	}

	assert.True(t, cats["security"], "expected security category")
	assert.True(t, cats["cost_optimizing"], "expected cost_optimizing category")
	assert.True(t, cats["fault_tolerance"], "expected fault_tolerance category")
}

// TestSupport_RefreshTrustedAdvisorCheck_StatusCount verifies counter increments.
func TestSupport_RefreshTrustedAdvisorCheck_StatusCount(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	_, err := b.RefreshTrustedAdvisorCheck("check-A")
	require.NoError(t, err)

	_, err = b.RefreshTrustedAdvisorCheck("check-B")
	require.NoError(t, err)

	assert.Equal(t, 2, support.CheckRefreshStatusCount(b))
}
