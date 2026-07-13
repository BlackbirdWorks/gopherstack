package support_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

func TestSupport_DescribeAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *support.InMemoryBackend)
		body         map[string]any
		name         string
		wantFileName string
		wantCode     int
	}{
		{
			name: "found",
			setup: func(b *support.InMemoryBackend) {
				b.AddAttachmentInternal(&support.Attachment{
					AttachmentID: "att-001",
					FileName:     "error.log",
					Data:         []byte("stack trace"),
				})
			},
			body:         map[string]any{"attachmentId": "att-001"},
			wantCode:     http.StatusOK,
			wantFileName: "error.log",
		},
		{
			name:     "not_found",
			setup:    nil,
			body:     map[string]any{"attachmentId": "att-missing"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_attachment_id",
			setup:    nil,
			body:     map[string]any{},
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

			rec := doSupportRequest(t, h, "DescribeAttachment", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantFileName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				att, ok := resp["attachment"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantFileName, att["fileName"])
			}
		})
	}
}

func TestSupport_DescribeCreateCaseOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "all_fields",
			body: map[string]any{
				"issueType":    "technical",
				"serviceCode":  "amazon-s3",
				"categoryCode": "general-guidance",
				"language":     "en",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_body",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeCreateCaseOptions", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["communicationTypes"])
				assert.Equal(t, "available", resp["languageAvailability"])
			}
		})
	}
}

func TestSupport_DescribeServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantContainsCode string
		wantCode         int
		wantMinCount     int
	}{
		{
			name:         "all_services",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantMinCount: 1,
		},
		{
			name:             "filtered_by_service_code",
			body:             map[string]any{"serviceCodeList": []string{"amazon-s3"}},
			wantCode:         http.StatusOK,
			wantMinCount:     1,
			wantContainsCode: "amazon-s3",
		},
		{
			name:         "filter_no_match",
			body:         map[string]any{"serviceCodeList": []string{"nonexistent-service"}},
			wantCode:     http.StatusOK,
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeServices", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			services, ok := resp["services"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(services), tt.wantMinCount)

			if tt.wantContainsCode != "" {
				found := false
				for _, s := range services {
					svc, svcOK := s.(map[string]any)
					require.True(t, svcOK)

					if svc["code"] == tt.wantContainsCode {
						found = true

						break
					}
				}
				assert.True(t, found, "expected service code %s in response", tt.wantContainsCode)
			}
		})
	}
}

func TestSupport_DescribeSeverityLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "english",
			body:     map[string]any{"language": "en"},
			wantCode: http.StatusOK,
		},
		{
			name:     "no_language",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeSeverityLevels", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			levels, ok := resp["severityLevels"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, levels)

			// Verify at minimum "low" and "critical" are present.
			codes := make([]string, 0, len(levels))
			for _, l := range levels {
				lv, lvOK := l.(map[string]any)
				require.True(t, lvOK)
				codes = append(codes, lv["code"].(string))
			}
			assert.Contains(t, codes, "low")
			assert.Contains(t, codes, "critical")
		})
	}
}

func TestSupport_DescribeSupportedLanguages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "all_params",
			body: map[string]any{
				"issueType":    "technical",
				"serviceCode":  "amazon-s3",
				"categoryCode": "general-guidance",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_body",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeSupportedLanguages", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			langs, ok := resp["supportedLanguages"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, langs)

			// English should be in the list.
			found := false
			for _, l := range langs {
				lv, lvOK := l.(map[string]any)
				require.True(t, lvOK)

				if lv["code"] == "en" {
					found = true

					break
				}
			}
			assert.True(t, found, "expected English (code=en) in supported languages")
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
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
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
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
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
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
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
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
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

	var resp map[string]any
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &resp))
	statuses, ok := resp["statuses"].([]any)
	require.True(t, ok)
	require.Len(t, statuses, 1)
	status, ok := statuses[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "DAvU99Dc4C", status["checkId"])
	assert.Equal(t, "enqueued", status["status"])
}
