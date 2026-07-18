package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

			resp := decodeSupportResponse(t, rec)
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

// TestSupport_DescribeSeverityLevels_Localization verifies severity level
// names are localized per language.
func TestSupport_DescribeSeverityLevels_Localization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   map[string]any
		want   string
	}{
		{
			name:   "severity Japanese",
			action: "DescribeSeverityLevels",
			body:   map[string]any{"language": "ja"},
			want:   "一般的なガイダンス",
		},
		{
			name:   "severity English",
			action: "DescribeSeverityLevels",
			body:   map[string]any{"language": "en"},
			want:   "General guidance",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, newTestSupportHandler(t), tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			levels := decodeSupportResponse(t, rec)["severityLevels"].([]any)
			assert.Equal(t, tt.want, levels[0].(map[string]any)["name"])
		})
	}
}
