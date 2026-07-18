package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- DataSource extras tests ---- //nolint:godot // existing issue.
func TestQuickSight_DataSourceExtras(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Need a data source
	rec := doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
		"DataSourceId": "src1", "Name": "Source1", "Type": "S3",
	})
	require.True(t, rec.Code == http.StatusOK || rec.Code == http.StatusCreated, "create data source: %d", rec.Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "describe data source permissions",
			method:     http.MethodGet,
			path:       accountPath("/data-sources/src1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "DataSourceId",
		},
		{
			name:       "update data source permissions",
			method:     http.MethodPost,
			path:       accountPath("/data-sources/src1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSourceId",
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

// ---- DataSource tests ----

func TestQuickSight_DataSources(t *testing.T) {
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
			name:   "CreateDataSource returns ARN and 201",
			method: http.MethodPost,
			path:   accountPath("/data-sources"),
			body: map[string]any{
				"DataSourceId": "ds1",
				"Name":         "My Source",
				"Type":         "ATHENA",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:datasource/ds1")
				assert.Equal(t, "ds1", body["DataSourceId"])
				assert.Equal(t, "CREATION_SUCCESSFUL", body["CreationStatus"])
			},
		},
		{
			name:   "CreateDataSource duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/data-sources"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "dup", "Name": "x", "Type": "ATHENA",
				})
			},
			body:     map[string]any{"DataSourceId": "dup", "Name": "x", "Type": "ATHENA"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeDataSource returns source",
			method: http.MethodGet,
			path:   accountPath("/data-sources/ds2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "ds2", "Name": "S2", "Type": "S3",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ds, ok := body["DataSource"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "ds2", ds["DataSourceId"])
				assert.Equal(t, "S3", ds["Type"])
			},
		},
		{
			name:     "DescribeDataSource missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/data-sources/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateDataSource changes name",
			method: http.MethodPut,
			path:   accountPath("/data-sources/ds3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "ds3", "Name": "old-name", "Type": "ATHENA",
				})
			},
			body:     map[string]any{"Name": "new-name"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ds3", body["DataSourceId"])
				assert.Equal(t, "UPDATE_SUCCESSFUL", body["UpdateStatus"])
			},
		},
		{
			name:   "DeleteDataSource removes source",
			method: http.MethodDelete,
			path:   accountPath("/data-sources/ds4"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "ds4", "Name": "x", "Type": "ATHENA",
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListDataSources returns sources",
			method: http.MethodGet,
			path:   accountPath("/data-sources"),
			setup: func(h *quicksight.Handler) {
				for _, id := range []string{"ls1", "ls2"} {
					doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
						"DataSourceId": id, "Name": id, "Type": "ATHENA",
					})
				}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["DataSources"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 2)
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
