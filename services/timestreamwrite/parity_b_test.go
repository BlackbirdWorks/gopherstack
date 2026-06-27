package timestreamwrite_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// ─────────────────────────────────────────────────────────────
// Gap 1: Dimension name/value required validation
// ─────────────────────────────────────────────────────────────

func TestParity_WriteRecords_DimensionNameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		dimension  map[string]any
		wantStatus int
	}{
		{
			name:       "empty_dimension_name_rejected",
			dimension:  map[string]any{"name": "", "value": "host-1"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_dimension_value_rejected",
			dimension:  map[string]any{"name": "host", "value": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_dimension_accepted",
			dimension:  map[string]any{"name": "host", "value": "server-1"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "dim-val-db"})
			doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "dim-val-db",
				"TableName":    "dim-val-tbl",
			})

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "dim-val-db",
				"TableName":    "dim-val-tbl",
				"Records": []map[string]any{
					{
						"MeasureName":      "cpu",
						"MeasureValue":     "42.0",
						"MeasureValueType": "DOUBLE",
						"Time":             "1719820800000",
						"TimeUnit":         "MILLISECONDS",
						"Dimensions":       []map[string]any{tt.dimension},
					},
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 2: MULTI measure type constraints
// ─────────────────────────────────────────────────────────────

func TestParity_WriteRecords_MULTIConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		record     map[string]any
		wantStatus int
	}{
		{
			name: "MULTI_without_MeasureValues_rejected",
			record: map[string]any{
				"MeasureName":      "multi-m",
				"MeasureValueType": "MULTI",
				"Time":             "1719820800000",
				"TimeUnit":         "MILLISECONDS",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "MULTI_with_MeasureValue_non_empty_rejected",
			record: map[string]any{
				"MeasureName":      "multi-m",
				"MeasureValue":     "should-be-empty",
				"MeasureValueType": "MULTI",
				"Time":             "1719820800000",
				"TimeUnit":         "MILLISECONDS",
				"MeasureValues": []map[string]any{
					{"name": "a", "value": "1", "type": "DOUBLE"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "MULTI_with_MeasureValues_and_empty_MeasureValue_accepted",
			record: map[string]any{
				"MeasureName":      "multi-m",
				"MeasureValueType": "MULTI",
				"Time":             "1719820800000",
				"TimeUnit":         "MILLISECONDS",
				"MeasureValues": []map[string]any{
					{"name": "cpu", "value": "42.0", "type": "DOUBLE"},
					{"name": "mem", "value": "1024", "type": "BIGINT"},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "multi-db"})
			doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "multi-db",
				"TableName":    "multi-tbl",
			})

			rec := doRequest(t, h, "WriteRecords", map[string]any{
				"DatabaseName": "multi-db",
				"TableName":    "multi-tbl",
				"Records":      []map[string]any{tt.record},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 3: DatabaseName minimum length (3 chars)
// ─────────────────────────────────────────────────────────────

func TestParity_CreateDatabase_MinimumNameLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dbName     string
		wantStatus int
	}{
		{name: "one_char_rejected", dbName: "a", wantStatus: http.StatusBadRequest},
		{name: "two_char_rejected", dbName: "ab", wantStatus: http.StatusBadRequest},
		{name: "three_char_accepted", dbName: "abc", wantStatus: http.StatusOK},
		{name: "four_char_accepted", dbName: "abcd", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": tt.dbName})
			assert.Equal(t, tt.wantStatus, rec.Code, "DatabaseName=%q", tt.dbName)

			if tt.wantStatus == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "ValidationException", body["__type"])
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 4: CreateTable RetentionProperties defaults (6h / 73d)
// ─────────────────────────────────────────────────────────────

func TestParity_CreateTable_DefaultRetentionProperties(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		wantHours  int64
		wantDays   int64
		createBody map[string]any
	}{
		{
			name:       "no_retention_specified_returns_defaults",
			wantHours:  6,
			wantDays:   73,
			createBody: map[string]any{},
		},
		{
			name:      "explicit_retention_overrides_defaults",
			wantHours: 12,
			wantDays:  180,
			createBody: map[string]any{
				"RetentionProperties": map[string]any{
					"MemoryStoreRetentionPeriodInHours":  12,
					"MagneticStoreRetentionPeriodInDays": 180,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "def-ret-db"})

			body := map[string]any{
				"DatabaseName": "def-ret-db",
				"TableName":    "def-ret-tbl",
			}

			maps.Copy(body, tt.createBody)

			rec := doRequest(t, h, "CreateTable", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			tbl := out["Table"].(map[string]any)
			rp := tbl["RetentionProperties"].(map[string]any)
			assert.InDelta(t, float64(tt.wantHours), rp["MemoryStoreRetentionPeriodInHours"], 0)
			assert.InDelta(t, float64(tt.wantDays), rp["MagneticStoreRetentionPeriodInDays"], 0)
		})
	}
}

func TestParity_CreateTable_DefaultRetentionViaBackend(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name      string
		inp       *timestreamwrite.CreateTableInput
		wantHours int64
		wantDays  int64
	}{
		{
			name:      "nil_input_returns_defaults",
			inp:       nil,
			wantHours: 6,
			wantDays:  73,
		},
		{
			name:      "non_nil_input_nil_retention_returns_defaults",
			inp:       &timestreamwrite.CreateTableInput{},
			wantHours: 6,
			wantDays:  73,
		},
		{
			name: "explicit_retention_preserved",
			inp: &timestreamwrite.CreateTableInput{
				RetentionProperties: &timestreamwrite.RetentionProperties{
					MemoryStoreRetentionPeriodInHours:  24,
					MagneticStoreRetentionPeriodInDays: 365,
				},
			},
			wantHours: 24,
			wantDays:  365,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamwrite.NewInMemoryBackend()
			_, err := b.CreateDatabase("def-b-db", nil)
			require.NoError(t, err)

			tbl, err := b.CreateTable("def-b-db", "def-b-tbl", nil, tt.inp)
			require.NoError(t, err)
			require.NotNil(t, tbl.RetentionProperties)
			assert.Equal(t, tt.wantHours, tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours)
			assert.Equal(t, tt.wantDays, tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 5: DescribeEndpoints CachePeriodInMinutes = 1440
// ─────────────────────────────────────────────────────────────

func TestParity_DescribeEndpoints_CachePeriodIs1440(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	endpoints := out["Endpoints"].([]any)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.InDelta(t, float64(1440), ep["CachePeriodInMinutes"], 0,
		"CachePeriodInMinutes must match real AWS value of 1440")
}

// ─────────────────────────────────────────────────────────────
// Gap 6: CreateBatchLoadTask DataSourceConfiguration required
// ─────────────────────────────────────────────────────────────

func TestParity_CreateBatchLoadTask_DataSourceConfigRequired(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "missing_DataSourceConfiguration_rejected",
			body: map[string]any{
				"TargetDatabaseName": "blt-req-db",
				"TargetTableName":    "blt-req-tbl",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_DataSourceConfiguration_accepted",
			body: map[string]any{
				"TargetDatabaseName": "blt-req-db",
				"TargetTableName":    "blt-req-tbl",
				"DataSourceConfiguration": map[string]any{
					"DataFormat":                "CSV",
					"DataSourceS3Configuration": map[string]any{"BucketName": "my-bucket"},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "blt-req-db"})
			doRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "blt-req-db",
				"TableName":    "blt-req-tbl",
			})

			rec := doRequest(t, h, "CreateBatchLoadTask", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusBadRequest {
				var errBody map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
				assert.Equal(t, "ValidationException", errBody["__type"])
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Gap 7: ResumeBatchLoadTask — FAILED and PROGRESS_STOPPED resumable;
// PENDING_RESUME is NOT resumable (it is an internal intermediate state).
// ─────────────────────────────────────────────────────────────

func TestParity_ResumeBatchLoadTask_ResumableStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "FAILED_is_resumable", status: timestreamwrite.BatchLoadStatusFailed, wantErr: false},
		{name: "PROGRESS_STOPPED_is_resumable", status: timestreamwrite.BatchLoadStatusProgressStopped, wantErr: false},
		{name: "PENDING_RESUME_is_not_resumable", status: timestreamwrite.BatchLoadStatusPendingResume, wantErr: true},
		{name: "IN_PROGRESS_is_not_resumable", status: timestreamwrite.BatchLoadStatusInProgress, wantErr: true},
		{name: "SUCCEEDED_is_not_resumable", status: timestreamwrite.BatchLoadStatusSucceeded, wantErr: true},
		{name: "CREATED_is_not_resumable", status: timestreamwrite.BatchLoadStatusCreated, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamwrite.NewInMemoryBackend()
			b.AddBatchLoadTaskInternal(&timestreamwrite.BatchLoadTask{
				TaskID:             "resume-test-task",
				TargetDatabaseName: "any-db",
				TargetTableName:    "any-tbl",
				TaskStatus:         tt.status,
			})

			err := b.ResumeBatchLoadTask("resume-test-task")
			if tt.wantErr {
				require.ErrorIs(t, err, timestreamwrite.ErrInvalidBatchLoadStatus)
			} else {
				require.NoError(t, err)
				resumed, descErr := b.DescribeBatchLoadTask("resume-test-task")
				require.NoError(t, descErr)
				assert.Equal(t, timestreamwrite.BatchLoadStatusCreated, resumed.TaskStatus)
			}
		})
	}
}
