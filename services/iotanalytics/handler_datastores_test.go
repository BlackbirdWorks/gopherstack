package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAndDescribeDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		wantStatus    int
	}{
		{
			name:          "success",
			datastoreName: "test_datastore",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "empty_name",
			datastoreName: "",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datastores", map[string]string{
				"datastoreName": tt.datastoreName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

// TestHandler_Datastores covers datastore CRUD operations.
func TestHandler_Datastores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		op            string
		wantStatus    int
	}{
		{
			name:          "create_success",
			datastoreName: "my_ds",
			op:            "create",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "list",
			datastoreName: "list_ds",
			op:            "list",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "describe_success",
			datastoreName: "desc_ds",
			op:            "describe",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "update_success",
			datastoreName: "update_ds",
			op:            "update",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "delete_success",
			datastoreName: "delete_ds",
			op:            "delete",
			wantStatus:    http.StatusNoContent,
		},
		{
			name:          "describe_not_found",
			datastoreName: "no_such_ds",
			op:            "describe_only",
			wantStatus:    http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.op {
			case "create":
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/datastores",
					map[string]string{"datastoreName": tt.datastoreName},
				)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "list":
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/datastores",
					map[string]string{"datastoreName": tt.datastoreName},
				)
				require.Equal(t, http.StatusOK, rec.Code)
				listRec := doRequest(t, h, http.MethodGet, "/datastores", nil)
				assert.Equal(t, tt.wantStatus, listRec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["datastoreSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

			case "describe":
				doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": tt.datastoreName})
				rec := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "update":
				doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": tt.datastoreName})
				rec := doRequest(t, h, http.MethodPut, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete":
				doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": tt.datastoreName})
				rec := doRequest(t, h, http.MethodDelete, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "describe_only":
				rec := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_ErrAlreadyExists_Datastore verifies creating a duplicate datastore returns 409.
func TestHandler_ErrAlreadyExists_Datastore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/datastores", map[string]any{"datastoreName": "dup_ds"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/datastores", map[string]any{"datastoreName": "dup_ds"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestHandler_DescribeDatastore_NewFields verifies datastore describe response.
func TestHandler_DescribeDatastore_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		body          map[string]any
		wantFields    []string
	}{
		{
			name:          "basic_has_datastore_wrapper",
			datastoreName: "desc_ds1",
			body:          map[string]any{"datastoreName": "desc_ds1"},
			wantFields:    []string{"datastore"},
		},
		{
			name:          "with_json_format",
			datastoreName: "desc_ds2",
			body: map[string]any{
				"datastoreName": "desc_ds2",
				"fileFormatConfiguration": map[string]any{
					"jsonConfiguration": map[string]any{},
				},
			},
			wantFields: []string{"datastore"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/datastores", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "describe response must contain field %q", field)
			}
		})
	}
}

// TestHandler_CreateDatastore_RetentionPeriodInResponse verifies CreateDatastore returns retentionPeriod.
func TestHandler_CreateDatastore_RetentionPeriodInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantRetention bool
	}{
		{
			name: "with_retention_period",
			body: map[string]any{
				"datastoreName":   "retds1",
				"retentionPeriod": map[string]any{"numberOfDays": 7},
			},
			wantRetention: true,
		},
		{
			name:          "without_retention_period",
			body:          map[string]any{"datastoreName": "retds2"},
			wantRetention: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datastores", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, hasRP := resp["retentionPeriod"]
			assert.Equal(t, tt.wantRetention, hasRP)
		})
	}
}

// TestHandler_DescribeDatastore_IncludeStatistics verifies DescribeDatastore returns statistics when requested.
func TestHandler_DescribeDatastore_IncludeStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		includeStats   string
		wantStatistics bool
	}{
		{
			name:           "include_statistics_true",
			includeStats:   "true",
			wantStatistics: true,
		},
		{
			name:           "no_statistics",
			includeStats:   "",
			wantStatistics: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": "statsds"})

			path := "/datastores/statsds"
			if tt.includeStats != "" {
				path += "?includeStatistics=" + tt.includeStats
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ds, _ := resp["datastore"].(map[string]any)
			_, hasStats := ds["statistics"]
			assert.Equal(t, tt.wantStatistics, hasStats)
		})
	}
}

// TestHandler_ListDatastores_DatastoreStorage verifies datastoreStorage appears in ListDatastores summaries.
func TestHandler_ListDatastores_DatastoreStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"datastoreName": "storeds",
		"datastoreStorage": map[string]any{
			"serviceManagedS3": map[string]any{},
		},
	}
	doRequest(t, h, http.MethodPost, "/datastores", body)

	rec := doRequest(t, h, http.MethodGet, "/datastores", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, _ := resp["datastoreSummaries"].([]any)
	require.Len(t, summaries, 1)
	summary, _ := summaries[0].(map[string]any)
	_, hasStorage := summary["datastoreStorage"]
	assert.True(t, hasStorage, "datastoreStorage must appear in ListDatastores summary")
}
