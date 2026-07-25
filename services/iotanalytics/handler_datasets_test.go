package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAndDescribeDataset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		wantStatus  int
	}{
		{
			name:        "success",
			datasetName: "test_dataset",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "empty_name",
			datasetName: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{
				"datasetName": tt.datasetName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DatasetContentLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		seed        bool
		wantCreate  int
		wantGet     int
		wantList    int
		wantDelete  int
	}{
		{
			name:        "full_lifecycle",
			datasetName: "my_dataset",
			seed:        true,
			wantCreate:  http.StatusOK,
			wantGet:     http.StatusOK,
			wantList:    http.StatusOK,
			wantDelete:  http.StatusNoContent,
		},
		{
			name:        "dataset_not_found",
			datasetName: "nonexistent",
			seed:        false,
			wantCreate:  http.StatusNotFound,
			wantGet:     http.StatusNotFound,
			wantList:    http.StatusNotFound,
			wantDelete:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			createRec := doRequest(t, h, http.MethodPost, "/datasets/"+tt.datasetName+"/content", nil)
			assert.Equal(t, tt.wantCreate, createRec.Code)

			getRec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName+"/content", nil)
			assert.Equal(t, tt.wantGet, getRec.Code)

			listRec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName+"/contents", nil)
			assert.Equal(t, tt.wantList, listRec.Code)

			var versionID string

			if tt.wantList == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["datasetContentSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

				if len(summaries) > 0 {
					if entry, ok2 := summaries[0].(map[string]any); ok2 {
						versionID, _ = entry["version"].(string)
					}
				}
			}

			deletePath := "/datasets/" + tt.datasetName + "/content"
			if versionID != "" {
				deletePath += "?versionId=" + versionID
			}

			deleteRec := doRequest(t, h, http.MethodDelete, deletePath, nil)
			assert.Equal(t, tt.wantDelete, deleteRec.Code)
		})
	}
}

// TestHandler_CreateDatasetContent_ExplicitVersionID verifies CreateDatasetContent honors an
// explicit versionId in the request body (CreateDatasetContentInput.VersionId in the real
// SDK) instead of always generating a random one, and rejects a duplicate versionId against
// the same dataset with 409 rather than silently overwriting the existing content version.
func TestHandler_CreateDatasetContent_ExplicitVersionID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "explicit_ver_ds"})

	rec := doRequest(t, h, http.MethodPost, "/datasets/explicit_ver_ds/content", map[string]string{
		"versionId": "my-custom-version",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-custom-version", resp["versionId"])

	getRec := doRequest(t, h, http.MethodGet, "/datasets/explicit_ver_ds/content?versionId=my-custom-version", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	dupRec := doRequest(t, h, http.MethodPost, "/datasets/explicit_ver_ds/content", map[string]string{
		"versionId": "my-custom-version",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
}

// TestHandler_GetDatasetContent_MagicVersionStrings verifies GetDatasetContent honors the
// AWS-documented "$LATEST" and "$LATEST_SUCCEEDED" versionId sentinels (uppercase, as sent
// verbatim by the SDK), not just an omitted versionId.
func TestHandler_GetDatasetContent_MagicVersionStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		versionID string
	}{
		{name: "omitted", versionID: ""},
		{name: "latest", versionID: "$LATEST"},
		{name: "latest_succeeded", versionID: "$LATEST_SUCCEEDED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "magicver_ds"})
			createRec := doRequest(t, h, http.MethodPost, "/datasets/magicver_ds/content", nil)
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			wantVersion, _ := createResp["versionId"].(string)
			require.NotEmpty(t, wantVersion)

			path := "/datasets/magicver_ds/content"
			if tt.versionID != "" {
				path += "?versionId=" + tt.versionID
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, wantVersion, resp["versionId"])
		})
	}
}

// TestHandler_DeleteDatasetContent_OmittedVersionDeletesOnlyLatest verifies that omitting
// versionId (equivalent to the AWS default "$LATEST_SUCCEEDED") removes exactly one content
// version -- the most recently created -- not every version for the dataset.
func TestHandler_DeleteDatasetContent_OmittedVersionDeletesOnlyLatest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "delonlylatest_ds"})

	var firstVersion string

	for i := range 3 {
		rec := doRequest(t, h, http.MethodPost, "/datasets/delonlylatest_ds/content", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		if i == 0 {
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			firstVersion, _ = resp["versionId"].(string)
			require.NotEmpty(t, firstVersion)
		}
	}

	deleteRec := doRequest(t, h, http.MethodDelete, "/datasets/delonlylatest_ds/content", nil)
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/datasets/delonlylatest_ds/contents", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, _ := listResp["datasetContentSummaries"].([]any)
	require.Len(t, summaries, 2, "omitted versionId must delete exactly one version")

	var remainingHasFirst bool

	for _, s := range summaries {
		summary, _ := s.(map[string]any)
		if summary["version"] == firstVersion {
			remainingHasFirst = true
		}
	}

	assert.True(t, remainingHasFirst, "the two oldest versions must remain; only the newest is deleted")
}

// TestHandler_ListDatasetContents_PaginationAcrossPages verifies that paging through
// ListDatasetContents with a small maxResults returns every content version exactly once,
// with no duplicates or gaps, across repeated backend calls driven by successive nextTokens.
func TestHandler_ListDatasetContents_PaginationAcrossPages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "pageall_ds"})

	wantVersions := make(map[string]bool)

	for range 5 {
		rec := doRequest(t, h, http.MethodPost, "/datasets/pageall_ds/content", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		vid, _ := resp["versionId"].(string)
		require.NotEmpty(t, vid)
		wantVersions[vid] = true
	}

	seen := make(map[string]bool)
	path := "/datasets/pageall_ds/contents?maxResults=2"

	for path != "" {
		rec := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		summaries, _ := resp["datasetContentSummaries"].([]any)
		for _, s := range summaries {
			summary, _ := s.(map[string]any)
			version, _ := summary["version"].(string)
			require.False(t, seen[version], "version %q must not be returned twice across pages", version)
			seen[version] = true
		}

		nextToken, _ := resp["nextToken"].(string)
		if nextToken == "" {
			break
		}

		path = "/datasets/pageall_ds/contents?maxResults=2&nextToken=" + nextToken
	}

	assert.Len(t, seen, len(wantVersions), "every created version must be returned exactly once across pages")

	for vid := range wantVersions {
		assert.True(t, seen[vid], "version %q must appear in some page", vid)
	}
}

// TestHandler_ListDatasetContents_ScheduleTime verifies each dataset content summary carries
// a scheduleTime field (AWS docs: "the time the creation of the dataset contents was
// scheduled to start", distinct from creationTime) and that it matches creationTime for a
// directly (non-schedule-triggered) created content version.
func TestHandler_ListDatasetContents_ScheduleTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "sched_ds"})

	rec := doRequest(t, h, http.MethodPost, "/datasets/sched_ds/content", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/datasets/sched_ds/contents", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	summaries, _ := resp["datasetContentSummaries"].([]any)
	require.Len(t, summaries, 1)

	summary, _ := summaries[0].(map[string]any)
	require.Contains(t, summary, "scheduleTime")
	assert.InDelta(t, summary["creationTime"], summary["scheduleTime"], 1)
}

// TestHandler_ListDatasetContents_ScheduledFilters verifies the scheduledBefore and
// scheduledOnOrAfter query filters on ListDatasetContents (ListDatasetContentsInput fields
// documented against DatasetContentSummary.ScheduleTime).
func TestHandler_ListDatasetContents_ScheduledFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "sched_filter_ds"})

	rec := doRequest(t, h, http.MethodPost, "/datasets/sched_filter_ds/content", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	future := url.QueryEscape(time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	past := url.QueryEscape(time.Now().Add(-time.Hour).UTC().Format(time.RFC3339))

	tests := []struct {
		name    string
		query   string
		wantLen int
	}{
		{name: "scheduledBefore_future_includes", query: "?scheduledBefore=" + future, wantLen: 1},
		{name: "scheduledBefore_past_excludes", query: "?scheduledBefore=" + past, wantLen: 0},
		{name: "scheduledOnOrAfter_past_includes", query: "?scheduledOnOrAfter=" + past, wantLen: 1},
		{name: "scheduledOnOrAfter_future_excludes", query: "?scheduledOnOrAfter=" + future, wantLen: 0},
		{name: "no_filter_includes", query: "", wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			listRec := doRequest(t, h, http.MethodGet, "/datasets/sched_filter_ds/contents"+tt.query, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			summaries, _ := resp["datasetContentSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

// TestHandler_Datasets covers dataset CRUD operations.
func TestHandler_Datasets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		op          string
		wantStatus  int
	}{
		{
			name:        "list",
			datasetName: "list_dataset",
			op:          "list",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "describe_success",
			datasetName: "desc_dataset",
			op:          "describe",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "update_success",
			datasetName: "update_dataset",
			op:          "update",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "delete_success",
			datasetName: "delete_dataset",
			op:          "delete",
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "describe_not_found",
			datasetName: "no_such_dataset",
			op:          "describe_only",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.op {
			case "list":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				listRec := doRequest(t, h, http.MethodGet, "/datasets", nil)
				assert.Equal(t, tt.wantStatus, listRec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["datasetSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

			case "describe":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				rec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "update":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				rec := doRequest(t, h, http.MethodPut, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				rec := doRequest(t, h, http.MethodDelete, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "describe_only":
				rec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_ErrAlreadyExists_Dataset verifies creating a duplicate dataset returns 409.
func TestHandler_ErrAlreadyExists_Dataset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]any{"datasetName": "dup_set"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/datasets", map[string]any{"datasetName": "dup_set"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestHandler_DatasetContent_GetVersionSpecific tests retrieving a specific version.
func TestHandler_DatasetContent_GetVersionSpecific(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	datasetName := "version_test_dataset"

	// Create dataset.
	rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": datasetName})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create content.
	createRec := doRequest(t, h, http.MethodPost, "/datasets/"+datasetName+"/content", nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	versionID, ok := createResp["versionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, versionID)

	// Get by specific version ID.
	getRec := doRequest(t, h, http.MethodGet, "/datasets/"+datasetName+"/content?versionId="+versionID, nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Get non-existent version ID.
	getRec = doRequest(t, h, http.MethodGet, "/datasets/"+datasetName+"/content?versionId=nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)

	// Delete specific version.
	deleteRec := doRequest(t, h, http.MethodDelete, "/datasets/"+datasetName+"/content?versionId="+versionID, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code)

	// Get non-existent after delete.
	getRec = doRequest(t, h, http.MethodGet, "/datasets/"+datasetName+"/content", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

// TestHandler_DescribeDataset_NewFields verifies dataset describe response fields.
func TestHandler_DescribeDataset_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		body        map[string]any
		wantFields  []string
	}{
		{
			name:        "basic_has_dataset_wrapper",
			datasetName: "desc_set1",
			body:        map[string]any{"datasetName": "desc_set1"},
			wantFields:  []string{"dataset"},
		},
		{
			name:        "with_versioning",
			datasetName: "desc_set2",
			body: map[string]any{
				"datasetName": "desc_set2",
				"versioningConfiguration": map[string]any{
					"unlimited": true,
				},
			},
			wantFields: []string{"dataset"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/datasets", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "describe response must contain field %q", field)
			}
		})
	}
}

// TestHandler_GetDatasetContent_VersionAndEntries verifies GetDatasetContent returns versionId and entries.
func TestHandler_GetDatasetContent_VersionAndEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "contds"})
	doRequest(t, h, http.MethodPost, "/datasets/contds/content", nil)

	rec := doRequest(t, h, http.MethodGet, "/datasets/contds/content", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["versionId"], "GetDatasetContent must return versionId")
	_, hasEntries := resp["entries"]
	assert.True(t, hasEntries, "GetDatasetContent must return entries array")
}

// TestHandler_DeleteDatasetContent_RequiresVersionId verifies DeleteDatasetContent requires versionId.
func TestHandler_DeleteDatasetContent_RequiresVersionId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "no_version_id_deletes_all",
			path:       "/datasets/delcontds/content",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "with_version_id_returns_204",
			path:       "", // set dynamically
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "delcontds"})
			createRec := doRequest(t, h, http.MethodPost, "/datasets/delcontds/content", nil)
			require.Equal(t, http.StatusOK, createRec.Code)

			deletePath := tt.path
			if deletePath == "" {
				var cr map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
				vid, _ := cr["versionId"].(string)
				require.NotEmpty(t, vid)
				deletePath = "/datasets/delcontds/content?versionId=" + vid
			}

			rec := doRequest(t, h, http.MethodDelete, deletePath, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListDatasetContents_Pagination verifies ListDatasetContents returns a nextToken when more results exist.
func TestHandler_ListDatasetContents_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "pageds"})

	for range 3 {
		doRequest(t, h, http.MethodPost, "/datasets/pageds/content", nil)
	}

	rec := doRequest(t, h, http.MethodGet, "/datasets/pageds/contents?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["nextToken"], "nextToken must be set when results are truncated")
	summaries, _ := resp["datasetContentSummaries"].([]any)
	assert.Len(t, summaries, 2)
}
