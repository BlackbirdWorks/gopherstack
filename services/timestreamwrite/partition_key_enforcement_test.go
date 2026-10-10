package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_WriteRecords_RequiredPartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enforcement  string
		dimensions   []map[string]string
		wantStatus   int
		wantRejected bool
	}{
		{
			name: "required_present", enforcement: "REQUIRED", wantStatus: http.StatusOK,
			dimensions: []map[string]string{{"Name": "region", "Value": "us"}},
		},
		{
			name: "required_missing", enforcement: "REQUIRED", wantStatus: http.StatusBadRequest, wantRejected: true,
			dimensions: []map[string]string{{"Name": "host", "Value": "h1"}},
		},
		{
			name: "optional_missing", enforcement: "OPTIONAL", wantStatus: http.StatusOK,
			dimensions: []map[string]string{{"Name": "host", "Value": "h1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(
				t,
				http.StatusOK,
				doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mydb"}).Code,
			)
			ct := doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "mydb", "TableName": "tbl",
				"Schema": map[string]any{"CompositePartitionKey": []map[string]any{
					{"Type": "DIMENSION", "Name": "region", "EnforcementInRecord": tt.enforcement},
				}},
			})
			require.Equal(t, http.StatusOK, ct.Code, ct.Body.String())

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "mydb", "TableName": "tbl",
				"Records": []map[string]any{{
					"MeasureName": "cpu", "MeasureValue": "1", "MeasureValueType": "DOUBLE",
					"Time": recentTimeMillis(0), "TimeUnit": "MILLISECONDS", "Dimensions": tt.dimensions,
				}},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRejected {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Contains(t, out["RejectedRecords"].([]any)[0].(map[string]any)["Reason"], "region")
			}
		})
	}
}
