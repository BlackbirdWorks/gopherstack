package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuickSight_CreateDataSet_AutoIngestion verifies that CreateDataSet only
// reports IngestionArn/IngestionId (per the real CreateDataSetOutput shape,
// which documents both fields as "triggered as a result of dataset creation
// if the import mode is SPICE") when the dataset's import mode actually
// triggers one, and that the reported ingestion is a real, describable
// backend resource rather than a fabricated "auto" placeholder that 404s on
// lookup.
func TestQuickSight_CreateDataSet_AutoIngestion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantIngestion   bool
		wantDescribeErr bool
	}{
		{
			name: "SPICE dataset triggers a real, describable ingestion",
			body: map[string]any{
				"DataSetId":  "spice-set",
				"Name":       "SpiceSet",
				"ImportMode": "SPICE",
			},
			wantIngestion: true,
		},
		{
			name: "DIRECT_QUERY dataset triggers no ingestion",
			body: map[string]any{
				"DataSetId":  "dq-set",
				"Name":       "DQSet",
				"ImportMode": "DIRECT_QUERY",
			},
			wantIngestion: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), tc.body)
			require.Equal(t, http.StatusCreated, rec.Code)
			created := parseBody(t, rec)

			ingestionArn, hasArn := created["IngestionArn"]
			ingestionID, hasID := created["IngestionId"]

			if !tc.wantIngestion {
				assert.False(t, hasArn, "IngestionArn must be omitted when no ingestion is triggered")
				assert.False(t, hasID, "IngestionId must be omitted when no ingestion is triggered")

				return
			}

			require.True(t, hasArn, "IngestionArn must be present for a SPICE dataset")
			require.True(t, hasID, "IngestionId must be present for a SPICE dataset")
			assert.NotEqual(
				t,
				"auto",
				ingestionID,
				"ingestion ID must be a real resource ID, not a fabricated placeholder",
			)

			dataSetID, _ := tc.body["DataSetId"].(string)
			describePath := accountPath(fmt.Sprintf("/data-sets/%s/ingestions/%s", dataSetID, ingestionID))
			descRec := doRequest(t, h, http.MethodGet, describePath, nil)
			require.Equal(
				t,
				http.StatusOK,
				descRec.Code,
				"the ingestion reported by CreateDataSet must be independently describable",
			)

			desc := parseBody(t, descRec)
			ing, ok := desc["Ingestion"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, ingestionArn, ing["Arn"])
			assert.Equal(t, ingestionID, ing["IngestionId"])
		})
	}
}
