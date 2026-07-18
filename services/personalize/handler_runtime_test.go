package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_GetRecommendations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         map[string]any
		name          string
		wantItemCount int
	}{
		{
			name: "default_25_items",
			input: map[string]any{
				"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/my-campaign",
				"userId":      "user-123",
			},
			wantItemCount: 25,
		},
		{
			name: "explicit_numResults_5",
			input: map[string]any{
				"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/my-campaign",
				"userId":      "user-456",
				"numResults":  float64(5),
			},
			wantItemCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := personalizeHandler(t)
			personalizeCreateCampaign(t, h, "my-campaign")
			rec := personalizeRuntimeDo(t, h, "GetRecommendations", tt.input)

			require.Equal(t, http.StatusOK, rec.Code)
			m := personalizeUnmarshal(t, rec)
			assert.NotEmpty(t, m["recommendationId"])
			itemList, ok := m["itemList"].([]any)
			require.True(t, ok)
			assert.Len(t, itemList, tt.wantItemCount)
			first := itemList[0].(map[string]any)
			assert.NotEmpty(t, first["itemId"])
			_, hasScore := first["score"]
			assert.True(t, hasScore)
		})
	}
}

func TestPersonalize_GetPersonalizedRanking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputList []any
	}{
		{
			name:      "three_items_returned_ranked",
			inputList: []any{"item-a", "item-b", "item-c"},
		},
		{
			name:      "single_item",
			inputList: []any{"item-x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := personalizeHandler(t)
			personalizeCreateCampaign(t, h, "my-campaign")
			rec := personalizeRuntimeDo(t, h, "GetPersonalizedRanking", map[string]any{
				"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/my-campaign",
				"userId":      "user-789",
				"inputList":   tt.inputList,
			})

			require.Equal(t, http.StatusOK, rec.Code)
			m := personalizeUnmarshal(t, rec)
			assert.NotEmpty(t, m["recommendationId"])
			ranked, ok := m["personalizedRanking"].([]any)
			require.True(t, ok)
			assert.Len(t, ranked, len(tt.inputList))
			for i, v := range ranked {
				item := v.(map[string]any)
				assert.Equal(t, tt.inputList[i], item["itemId"])
				_, hasScore := item["score"]
				assert.True(t, hasScore)
			}
		})
	}
}

// --- Error paths ---
