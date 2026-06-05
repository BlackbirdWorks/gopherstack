package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Gap A: MeasureValueType validation
//
// Real AWS rejects WriteRecords requests containing an unknown MeasureValueType
// with ValidationException. The handler previously forwarded any string to the
// backend without checking the value.
// ---------------------------------------------------------------------------

func TestBatch2_WriteRecords_InvalidMeasureValueType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		measureValueType string
		wantStatus       int
	}{
		{
			name:             "DOUBLE is valid",
			measureValueType: "DOUBLE",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "BIGINT is valid",
			measureValueType: "BIGINT",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "BOOLEAN is valid",
			measureValueType: "BOOLEAN",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "VARCHAR is valid",
			measureValueType: "VARCHAR",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "TIMESTAMP is valid",
			measureValueType: "TIMESTAMP",
			wantStatus:       http.StatusOK,
		},
		{
			name:             "unknown type rejected",
			measureValueType: "INVALID",
			wantStatus:       http.StatusBadRequest,
		},
		{
			name:             "lowercase rejected",
			measureValueType: "double",
			wantStatus:       http.StatusBadRequest,
		},
		{
			name:             "mixed case rejected",
			measureValueType: "Double",
			wantStatus:       http.StatusBadRequest,
		},
		{
			name:             "empty string passes (field optional)",
			measureValueType: "",
			wantStatus:       http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "mvt-db"})
			doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "mvt-db", "TableName": "mvt-tbl"})

			record := map[string]any{
				"MeasureName":  "cpu",
				"MeasureValue": "1.5",
				"Time":         "1609459200000",
				"TimeUnit":     "MILLISECONDS",
			}
			if tt.measureValueType != "" {
				record["MeasureValueType"] = tt.measureValueType
			}

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "mvt-db",
				"TableName":    "mvt-tbl",
				"Records":      []any{record},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "MeasureValueType=%q", tt.measureValueType)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap B: TimeUnit validation
//
// Real AWS rejects WriteRecords requests with an unrecognised TimeUnit with
// ValidationException. The handler previously accepted any string (the backend
// defaulted to MILLISECONDS for unknown values, silently masking the error).
// ---------------------------------------------------------------------------

func TestBatch2_WriteRecords_InvalidTimeUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		timeUnit   string
		wantStatus int
	}{
		{
			name:       "SECONDS is valid",
			timeUnit:   "SECONDS",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MILLISECONDS is valid",
			timeUnit:   "MILLISECONDS",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MICROSECONDS is valid",
			timeUnit:   "MICROSECONDS",
			wantStatus: http.StatusOK,
		},
		{
			name:       "NANOSECONDS is valid",
			timeUnit:   "NANOSECONDS",
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown unit rejected",
			timeUnit:   "HOURS",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "lowercase rejected",
			timeUnit:   "milliseconds",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty string passes (field optional)",
			timeUnit:   "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "tu-db"})
			doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "tu-db", "TableName": "tu-tbl"})

			record := map[string]any{
				"MeasureName":      "metric",
				"MeasureValue":     "42",
				"MeasureValueType": "BIGINT",
				"Time":             "1609459200000",
			}
			if tt.timeUnit != "" {
				record["TimeUnit"] = tt.timeUnit
			}

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "tu-db",
				"TableName":    "tu-tbl",
				"Records":      []any{record},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "TimeUnit=%q", tt.timeUnit)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap C: Dimension count per record
//
// Real AWS limits each record to 128 dimensions. Exceeding this limit returns
// ValidationException. The handler previously forwarded oversized records to
// the backend without checking.
// ---------------------------------------------------------------------------

func TestBatch2_WriteRecords_DimensionCountLimit(t *testing.T) {
	t.Parallel()

	makeDimensions := func(n int) []map[string]string {
		dims := make([]map[string]string, n)
		for i := range dims {
			dims[i] = map[string]string{
				"Name":  "dim" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
				"Value": "v",
			}
		}

		return dims
	}

	tests := []struct {
		name       string
		dimCount   int
		wantStatus int
	}{
		{
			name:       "127 dimensions (below limit)",
			dimCount:   127,
			wantStatus: http.StatusOK,
		},
		{
			name:       "128 dimensions (at limit)",
			dimCount:   128,
			wantStatus: http.StatusOK,
		},
		{
			name:       "129 dimensions (exceeds limit)",
			dimCount:   129,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "200 dimensions (well over limit)",
			dimCount:   200,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "dim-db"})
			doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "dim-db", "TableName": "dim-tbl"})

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "dim-db",
				"TableName":    "dim-tbl",
				"Records": []any{
					map[string]any{
						"MeasureName":      "m",
						"MeasureValue":     "1",
						"MeasureValueType": "BIGINT",
						"Time":             "1609459200000",
						"TimeUnit":         "MILLISECONDS",
						"Dimensions":       makeDimensions(tt.dimCount),
					},
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "dimCount=%d", tt.dimCount)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// TestBatch2_WriteRecords_InvalidTypeViaCommonAttributes verifies that an invalid
// MeasureValueType in CommonAttributes is caught when it propagates to a record
// that does not override it.
func TestBatch2_WriteRecords_InvalidTypeViaCommonAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ca-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "ca-db", "TableName": "ca-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "ca-db",
		"TableName":    "ca-tbl",
		"CommonAttributes": map[string]any{
			"MeasureValueType": "NOT_A_TYPE",
			"TimeUnit":         "MILLISECONDS",
		},
		"Records": []any{
			map[string]any{
				"MeasureName":  "cpu",
				"MeasureValue": "1.0",
				"Time":         "1609459200000",
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
}

// TestBatch2_WriteRecords_InvalidTimeUnitViaCommonAttributes verifies that an
// invalid TimeUnit in CommonAttributes is caught when it propagates to a record.
func TestBatch2_WriteRecords_InvalidTimeUnitViaCommonAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "catu-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "catu-db", "TableName": "catu-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "catu-db",
		"TableName":    "catu-tbl",
		"CommonAttributes": map[string]any{
			"MeasureValueType": "DOUBLE",
			"TimeUnit":         "DAYS",
		},
		"Records": []any{
			map[string]any{
				"MeasureName":  "load",
				"MeasureValue": "0.5",
				"Time":         "1609459200000",
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ValidationException", body["__type"])
}

// TestBatch2_WriteRecords_RecordOverridesCommonInvalidType verifies that a record
// with a valid MeasureValueType overrides an invalid one in CommonAttributes,
// and the request succeeds.
func TestBatch2_WriteRecords_RecordOverridesCommonInvalidType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "ovr-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "ovr-db", "TableName": "ovr-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "ovr-db",
		"TableName":    "ovr-tbl",
		"CommonAttributes": map[string]any{
			"TimeUnit": "MILLISECONDS",
		},
		"Records": []any{
			map[string]any{
				"MeasureName":      "cpu",
				"MeasureValue":     "1.0",
				"MeasureValueType": "DOUBLE",
				"Time":             "1609459200000",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestBatch2_WriteRecords_MultiMeasureValueType verifies that MULTI is a valid
// MeasureValueType and is accepted by the handler.
func TestBatch2_WriteRecords_MultiMeasureValueType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "multi-db"})
	doRequest(t, h, "CreateTable", map[string]any{"DatabaseName": "multi-db", "TableName": "multi-tbl"})

	rec := doRequest(t, h, "WriteRecords", map[string]any{
		"DatabaseName": "multi-db",
		"TableName":    "multi-tbl",
		"Records": []any{
			map[string]any{
				"MeasureName":      "metrics",
				"MeasureValueType": "MULTI",
				"Time":             "1609459200000",
				"TimeUnit":         "MILLISECONDS",
				"MeasureValues": []map[string]string{
					{"Name": "cpu", "Value": "0.5", "Type": "DOUBLE"},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
