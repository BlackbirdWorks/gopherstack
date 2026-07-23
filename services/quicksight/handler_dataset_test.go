package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- DataSet extras tests ---- //nolint:godot // existing issue.
func TestQuickSight_DataSetExtras(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Need a dataset to exist first
	rec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "ds1", "Name": "Dataset1", "ImportMode": "SPICE",
	})
	require.True(t, rec.Code == http.StatusOK || rec.Code == http.StatusCreated, "create dataset: %d", rec.Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "describe dataset permissions",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "DataSetId",
		},
		{
			name:       "update dataset permissions",
			method:     http.MethodPost,
			path:       accountPath("/data-sets/ds1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSetId",
		},
		{
			name:       "put dataset refresh properties",
			method:     http.MethodPut,
			path:       accountPath("/data-sets/ds1/refresh-properties"),
			body:       map[string]any{"DataSetRefreshProperties": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe dataset refresh properties",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/refresh-properties"),
			wantStatus: http.StatusOK,
			wantKey:    "DataSetRefreshProperties",
		},
		{
			name:   "create refresh schedule",
			method: http.MethodPost,
			path:   accountPath("/data-sets/ds1/refresh-schedules"),
			body: map[string]any{
				"Schedule": map[string]any{"ScheduleId": "sched1", "RefreshType": "FULL_REFRESH"},
			},
			wantStatus: http.StatusOK,
			wantKey:    "ScheduleId",
		},
		{
			name:       "describe refresh schedule",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/refresh-schedules/sched1"),
			wantStatus: http.StatusOK,
			wantKey:    "RefreshSchedule",
		},
		{
			name:       "list refresh schedules",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/refresh-schedules"),
			wantStatus: http.StatusOK,
			wantKey:    "RefreshSchedules",
		},
		{
			name:   "update refresh schedule",
			method: http.MethodPut,
			path:   accountPath("/data-sets/ds1/refresh-schedules"),
			body: map[string]any{
				"Schedule": map[string]any{"ScheduleId": "sched1", "RefreshType": "INCREMENTAL_REFRESH"},
			},
			wantStatus: http.StatusOK,
			wantKey:    "ScheduleId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}

// ---- DataSet tests ----

func TestQuickSight_DataSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "CreateDataSet returns 201",
			method: http.MethodPost,
			path:   accountPath("/data-sets"),
			body: map[string]any{
				"DataSetId":  "set1",
				"Name":       "My Dataset",
				"ImportMode": "SPICE",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "set1", body["DataSetId"])
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:dataset/set1")
			},
		},
		{
			name:   "CreateDataSet duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/data-sets"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "dup", "Name": "x",
				})
			},
			body:     map[string]any{"DataSetId": "dup", "Name": "x"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "CreateDataSet default ImportMode is SPICE",
			method:   http.MethodPost,
			path:     accountPath("/data-sets"),
			body:     map[string]any{"DataSetId": "set-default-mode", "Name": "x"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.NotEmpty(t, body["DataSetId"])
			},
		},
		{
			name:   "DescribeDataSet returns dataset",
			method: http.MethodGet,
			path:   accountPath("/data-sets/set2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "set2", "Name": "S2", "ImportMode": "DIRECT_QUERY",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ds, ok := body["DataSet"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "DIRECT_QUERY", ds["ImportMode"])
			},
		},
		{
			name:     "DescribeDataSet missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/data-sets/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteDataSet removes dataset",
			method: http.MethodDelete,
			path:   accountPath("/data-sets/set3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "set3", "Name": "x",
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateDataSet on SPICE dataset reports new IngestionArn/IngestionId",
			method: http.MethodPut,
			path:   accountPath("/data-sets/set-spice-update"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "set-spice-update", "Name": "x", "ImportMode": "SPICE",
				})
			},
			body:     map[string]any{"Name": "renamed", "ImportMode": "SPICE"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.NotEmpty(t, body["IngestionArn"])
				assert.NotEmpty(t, body["IngestionId"])
			},
		},
		{
			name:   "UpdateDataSet on DIRECT_QUERY dataset omits IngestionArn/IngestionId",
			method: http.MethodPut,
			path:   accountPath("/data-sets/set-dq-update"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "set-dq-update", "Name": "x", "ImportMode": "DIRECT_QUERY",
				})
			},
			body:     map[string]any{"Name": "renamed", "ImportMode": "DIRECT_QUERY"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.NotContains(t, body, "IngestionArn")
				assert.NotContains(t, body, "IngestionId")
			},
		},
		{
			name:   "ListDataSets returns datasets",
			method: http.MethodGet,
			path:   accountPath("/data-sets"),
			setup: func(h *quicksight.Handler) {
				for _, id := range []string{"ls1", "ls2", "ls3"} {
					doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
						"DataSetId": id, "Name": id,
					})
				}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["DataSetSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- Ingestion tests ----

func TestQuickSight_Ingestions(t *testing.T) {
	t.Parallel()

	createDataSet := func(h *quicksight.Handler, id string) {
		doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
			"DataSetId": id, "Name": id,
		})
	}

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateIngestion returns 201 with RUNNING status",
			method:   http.MethodPut,
			path:     accountPath("/data-sets/dset1/ingestions/ing1"),
			setup:    func(h *quicksight.Handler) { createDataSet(h, "dset1") },
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ing1", body["IngestionId"])
				assert.Equal(t, "RUNNING", body["IngestionStatus"])
			},
		},
		{
			name:     "CreateIngestion on missing dataset returns 404",
			method:   http.MethodPut,
			path:     accountPath("/data-sets/notexist/ingestions/ing1"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "CreateIngestion duplicate returns 409",
			method: http.MethodPut,
			path:   accountPath("/data-sets/dset2/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset2")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset2/ingestions/ing1"), nil)
			},
			wantCode: http.StatusConflict,
		},
		{
			name:   "CancelIngestion sets status to CANCELLED",
			method: http.MethodDelete,
			path:   accountPath("/data-sets/dset3/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset3")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset3/ingestions/ing1"), nil)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeIngestion after cancel shows CANCELLED",
			method: http.MethodGet,
			path:   accountPath("/data-sets/dset4/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset4")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset4/ingestions/ing1"), nil)
				doRequest(t, h, http.MethodDelete, accountPath("/data-sets/dset4/ingestions/ing1"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ing, ok := body["Ingestion"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "CANCELLED", ing["IngestionStatus"])
			},
		},
		{
			name:   "CancelIngestion on already-CANCELLED ingestion returns 409",
			method: http.MethodDelete,
			path:   accountPath("/data-sets/dset-cancel-twice/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset-cancel-twice")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset-cancel-twice/ingestions/ing1"), nil)
				doRequest(t, h, http.MethodDelete, accountPath("/data-sets/dset-cancel-twice/ingestions/ing1"), nil)
			},
			wantCode: http.StatusConflict,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ConflictException", body["Code"])
			},
		},
		{
			name:   "ListIngestions returns ingestions for dataset",
			method: http.MethodGet,
			path:   accountPath("/data-sets/dset5/ingestions"),
			setup: func(h *quicksight.Handler) {
				// createDataSet defaults to ImportMode SPICE, which itself
				// triggers one auto-ingestion (see CreateDataSetOutput's
				// IngestionArn/IngestionId docs) in addition to the two
				// explicitly created below.
				createDataSet(h, "dset5")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset5/ingestions/i1"), nil)
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset5/ingestions/i2"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["Ingestions"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// TestQuickSight_CancelIngestion_CompletedAutoIngestion locks the fix for
// the gap noted in PARITY.md: CancelIngestion used to unconditionally
// overwrite IngestionStatus to CANCELLED even for an ingestion already in a
// terminal state. Here the terminal-state ingestion under test is the
// COMPLETED auto-ingestion CreateDataSet triggers for a SPICE dataset (see
// TestQuickSight_DataSets/CreateDataSet), which real AWS also would refuse
// to cancel.
func TestQuickSight_CancelIngestion_CompletedAutoIngestion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "dset-completed", "Name": "x", "ImportMode": "SPICE",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)
	createBody := parseBody(t, createRec)
	ingestionID, ok := createBody["IngestionId"].(string)
	require.True(t, ok, "CreateDataSet response missing IngestionId for a SPICE dataset")
	require.NotEmpty(t, ingestionID)

	describeRec := doRequest(
		t, h, http.MethodGet, accountPath("/data-sets/dset-completed/ingestions/"+ingestionID), nil,
	)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	ing, ok := describeBody["Ingestion"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "COMPLETED", ing["IngestionStatus"])

	cancelRec := doRequest(
		t, h, http.MethodDelete, accountPath("/data-sets/dset-completed/ingestions/"+ingestionID), nil,
	)
	assert.Equal(t, http.StatusConflict, cancelRec.Code)
	cancelBody := parseBody(t, cancelRec)
	assert.Equal(t, "ConflictException", cancelBody["Code"])

	// The ingestion's status must remain COMPLETED -- the rejected cancel
	// must not have mutated it.
	describeAgainRec := doRequest(
		t, h, http.MethodGet, accountPath("/data-sets/dset-completed/ingestions/"+ingestionID), nil,
	)
	require.Equal(t, http.StatusOK, describeAgainRec.Code)
	ingAgain, ok := parseBody(t, describeAgainRec)["Ingestion"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "COMPLETED", ingAgain["IngestionStatus"])
}
