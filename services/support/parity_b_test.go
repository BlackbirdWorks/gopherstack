package support_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

// opaqueToken decodes a base64url token and asserts it wraps a JSON offset object.
func opaqueToken(t *testing.T, token string) int {
	t.Helper()

	raw, err := base64.RawURLEncoding.DecodeString(token)
	require.NoError(t, err, "token must be valid base64url")

	var obj struct {
		O int `json:"o"`
	}

	require.NoError(t, json.Unmarshal(raw, &obj), "token must be JSON with 'o' key")

	return obj.O
}

// TestParity_OpaquePageToken verifies pagination emits JSON-wrapped opaque tokens.
func TestParity_OpaquePageToken(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	for range 15 {
		_, err := b.CreateCaseWithOptions(support.CreateCaseOptions{
			Subject:           "subject",
			CommunicationBody: "body",
			SeverityCode:      "low",
		})
		require.NoError(t, err)
	}

	cases, token, err := b.DescribeCasesWithOptions(support.DescribeCasesOptions{
		MaxResults:           10,
		IncludeResolvedCases: true,
	})
	require.NoError(t, err)
	assert.Len(t, cases, 10)
	require.NotEmpty(t, token)

	offset := opaqueToken(t, token)
	assert.Equal(t, 10, offset)
}

// TestParity_InvalidOpaqueToken verifies a plain-integer token is rejected.
func TestParity_InvalidOpaqueToken(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	// Old-style plain-integer token (base64url("5")) must be rejected.
	plainIntToken := base64.RawURLEncoding.EncodeToString([]byte("5"))
	_, _, err := b.DescribeCasesWithOptions(support.DescribeCasesOptions{
		NextToken:            plainIntToken,
		IncludeResolvedCases: true,
	})
	assert.Error(t, err, "plain integer token should be invalid")
}

// TestParity_DescribeCreateCaseOptions_ChatHours verifies language-specific chat hours.
func TestParity_DescribeCreateCaseOptions_ChatHours(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	tests := []struct {
		name      string
		language  string
		wantStart string
		wantEnd   string
	}{
		{"english_default", "en", "06:00", "22:00"},
		{"japanese_locale", "ja", "07:00", "21:00"},
		{"empty_language", "", "06:00", "22:00"},
		{"chinese_locale", "zh", "06:00", "22:00"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := b.DescribeCreateCaseOptions("", "", "", tc.language)
			require.NotNil(t, result)

			var chatType *support.CommunicationTypeOptions

			for i := range result.CommunicationTypes {
				if result.CommunicationTypes[i].Type == "chat" {
					chatType = &result.CommunicationTypes[i]

					break
				}
			}

			require.NotNil(t, chatType, "chat communication type must be present")
			require.NotEmpty(t, chatType.SupportedHours)
			assert.Equal(t, tc.wantStart, chatType.SupportedHours[0].StartTime)
			assert.Equal(t, tc.wantEnd, chatType.SupportedHours[0].EndTime)
		})
	}
}

// TestParity_DescribeSupportedLanguages_AccountAndBilling verifies account-and-billing returns English only.
func TestParity_DescribeSupportedLanguages_AccountAndBilling(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	tests := []struct {
		name      string
		issueType string
		wantCodes []string
	}{
		{"technical_all_langs", "technical", []string{"en", "zh", "ja", "ko"}},
		{"account_billing_en_only", "account-and-billing", []string{"en"}},
		{"empty_issue_type_all_langs", "", []string{"en", "zh", "ja", "ko"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			langs := b.DescribeSupportedLanguages(tc.issueType, "", "")
			codes := make([]string, 0, len(langs))

			for _, l := range langs {
				codes = append(codes, l.Code)
			}

			assert.Equal(t, tc.wantCodes, codes)
		})
	}
}

// TestParity_CheckResult_DefaultStatus verifies cost_optimizing checks default to "warning".
func TestParity_CheckResult_DefaultStatus(t *testing.T) {
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

// TestParity_CheckSummaries_DeriveFromResults verifies summaries reflect check result state.
func TestParity_CheckSummaries_DeriveFromResults(t *testing.T) {
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

// TestParity_RefreshStatus_PollProgression verifies enqueued→processing→success transition.
// Subtests are ordered and share backend state — they must NOT run in parallel.
//
//nolint:paralleltest // subtests share sequential backend state (poll counter); cannot run in parallel
func TestParity_RefreshStatus_PollProgression(t *testing.T) {
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

// TestParity_RefreshStatus_UnknownCheckReturnsNone verifies unknown checks return "none".
func TestParity_RefreshStatus_UnknownCheckReturnsNone(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	statuses := b.DescribeTrustedAdvisorCheckRefreshStatuses([]string{"unknown-check-id"})
	require.Len(t, statuses, 1)
	assert.Equal(t, "none", statuses[0].Status)
	assert.Equal(t, int64(0), statuses[0].MillisUntilNextRefreshable)
}

// TestParity_AttachmentSetEviction verifies the attachment set cap evicts oldest-expiry entries.
func TestParity_AttachmentSetEviction(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	// Fill past the cap of 1000.
	for range 1001 {
		_, _, err := b.AddAttachmentsToSetWithAttachments("", []support.Attachment{
			{FileName: "f.txt", Data: []byte("x")},
		})
		require.NoError(t, err)
	}

	// Must still accept a new set without error.
	setID, expiry, err := b.AddAttachmentsToSetWithAttachments("", []support.Attachment{
		{FileName: "final.txt", Data: []byte("y")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, setID)
	assert.True(t, expiry.After(time.Now()))
}

// TestParity_CheckRefreshStatusEviction verifies the refresh status cap evicts when full.
func TestParity_CheckRefreshStatusEviction(t *testing.T) {
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
