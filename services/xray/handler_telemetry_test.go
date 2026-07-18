package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutTelemetryRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/TelemetryRecords", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestPutTelemetryRecords_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TelemetryRecords", map[string]any{
		"TelemetryRecords": []map[string]any{
			{
				"Timestamp":              float64(time.Now().Unix()),
				"SegmentsReceivedCount":  100,
				"SegmentsSentCount":      95,
				"SegmentsSpilloverCount": 5,
				"SegmentsRejectedCount":  0,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// Response should be an empty JSON object.
	assert.Empty(t, resp)
}

func TestPutTelemetryRecords_AllFieldsAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []map[string]any
	}{
		{
			name: "single record all fields",
			records: []map[string]any{
				{
					"Timestamp":              float64(time.Now().Unix()),
					"SegmentsReceivedCount":  100,
					"SegmentsSentCount":      95,
					"SegmentsSpilloverCount": 3,
					"SegmentsRejectedCount":  2,
				},
			},
		},
		{
			name: "multiple records",
			records: []map[string]any{
				{
					"Timestamp":             float64(time.Now().Unix() - 60),
					"SegmentsReceivedCount": 50,
					"SegmentsSentCount":     50,
				},
				{
					"Timestamp":             float64(time.Now().Unix()),
					"SegmentsReceivedCount": 75,
					"SegmentsSentCount":     70,
				},
			},
		},
		{
			name:    "empty records list accepted",
			records: []map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TelemetryRecords", map[string]any{
				"TelemetryRecords": tt.records,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp, "response must be an empty JSON object")
		})
	}
}
