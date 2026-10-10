package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListAlerts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantCode  int
		wantItems bool
	}{
		{
			name:      "with_arn",
			query:     "resourceArn=arn:aws:mediatailor:us-east-1:000000000000:channel/c1",
			wantCode:  http.StatusOK,
			wantItems: true,
		},
		{name: "missing_arn", query: "", wantCode: http.StatusBadRequest},
		{name: "max_results_only", query: "maxResults=5", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithQuery(t, h, http.MethodGet, "/alerts", tt.query)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if !tt.wantItems {
				assert.Equal(t, "BadRequestException", rec.Header().Get("X-Amzn-Errortype"))

				return
			}

			items, _ := resp["Items"].([]any)
			assert.NotNil(t, items)
			assert.Empty(t, items)
		})
	}
}
