package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_WriteRecordsRequiresMeasureName verifies that WriteRecords rejects
// records with an empty MeasureName. Real AWS returns ValidationException for
// records missing this required field; the emulator previously accepted them
// silently, masking misconfigured callers.
func TestParity_WriteRecordsRequiresMeasureName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		commonAttributes map[string]any
		name             string
		records          []map[string]any
		wantCode         int
	}{
		{
			name: "record_without_measure_name_rejected",
			records: []map[string]any{
				{
					"MeasureValue":     "42.0",
					"MeasureValueType": "DOUBLE",
					"Time":             "1719820800000",
					"TimeUnit":         "MILLISECONDS",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "measure_name_from_common_attributes_accepted",
			commonAttributes: map[string]any{
				"MeasureName":      "cpu_usage",
				"MeasureValueType": "DOUBLE",
			},
			records: []map[string]any{
				{
					"MeasureValue": "85.5",
					"Time":         "1719820800000",
					"TimeUnit":     "MILLISECONDS",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "record_with_measure_name_accepted",
			records: []map[string]any{
				{
					"MeasureName":      "temperature",
					"MeasureValue":     "36.6",
					"MeasureValueType": "DOUBLE",
					"Time":             "1719820800000",
					"TimeUnit":         "MILLISECONDS",
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			dbRec := doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "parity-db-" + tt.name})
			require.Equal(t, http.StatusOK, dbRec.Code)

			tblRec := doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "parity-db-" + tt.name,
				"TableName":    "parity-tbl",
			})
			require.Equal(t, http.StatusOK, tblRec.Code)

			body := map[string]any{
				"DatabaseName": "parity-db-" + tt.name,
				"TableName":    "parity-tbl",
				"Records":      tt.records,
			}

			if tt.commonAttributes != nil {
				body["CommonAttributes"] = tt.commonAttributes
			}

			rec := doRequest(t, h, "WriteRecords", body)
			assert.Equal(t, tt.wantCode, rec.Code, "WriteRecords status for case %q", tt.name)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))

				assert.NotEmpty(t, errResp["__type"], "error response must include __type")
			}
		})
	}
}
