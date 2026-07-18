package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

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
			resp := decodeSupportResponse(t, rec)
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

// TestSupport_DescribeSupportedLanguages_AccountAndBilling verifies account-and-billing returns English only.
func TestSupport_DescribeSupportedLanguages_AccountAndBilling(t *testing.T) {
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
