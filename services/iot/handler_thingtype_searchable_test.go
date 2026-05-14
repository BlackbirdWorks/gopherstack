package iot_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIoT_ThingTypes_SearchableAttributes validates that searchableAttributes
// supplied at CreateThingType time are persisted and surfaced verbatim by
// DescribeThingType, matching the AWS IoT contract for ThingTypeProperties.
func TestIoT_ThingTypes_SearchableAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		thingTypeName  string
		searchable     []string
		wantSearchable []string
	}{
		{
			name:           "single_attribute",
			thingTypeName:  "tt-single",
			searchable:     []string{"serial"},
			wantSearchable: []string{"serial"},
		},
		{
			name:           "multiple_attributes",
			thingTypeName:  "tt-multi",
			searchable:     []string{"serial", "manufacturer", "model"},
			wantSearchable: []string{"serial", "manufacturer", "model"},
		},
		{
			name:          "empty_attributes",
			thingTypeName: "tt-none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newIoTHandler(t)

			req := map[string]any{
				"thingTypeProperties": map[string]any{
					"thingTypeDescription": "test",
					"searchableAttributes": tt.searchable,
				},
			}

			rec := doIoTRequest(t, h, http.MethodPost, "/thing-types/"+tt.thingTypeName, req)
			require.GreaterOrEqual(t, rec.Code, 200)
			require.Less(t, rec.Code, 300)

			rec = doIoTRequest(t, h, http.MethodGet, "/thing-types/"+tt.thingTypeName, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ThingTypeProperties struct {
					SearchableAttributes []string `json:"searchableAttributes"`
				} `json:"thingTypeProperties"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantSearchable, resp.ThingTypeProperties.SearchableAttributes)
		})
	}
}
