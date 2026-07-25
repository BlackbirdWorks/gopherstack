package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCWAlarmTemplateGroup_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns 201 with id and name",
			wantCode: http.StatusCreated,
			body:     map[string]any{"name": "test-cw-group"},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "test-cw-group", resp["name"])
				assert.NotEmpty(t, resp["createdAt"])
				assert.NotEmpty(t, resp["modifiedAt"])
			},
		},
		{
			name:     "create without name returns 400",
			wantCode: http.StatusBadRequest,
			body:     map[string]any{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/prod/cloudwatch-alarm-template-groups",
				tc.body,
			)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCWAlarmTemplateGroup_GetUpdateListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/cloudwatch-alarm-template-groups",
		map[string]any{
			"name": "cw-group-1",
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)

	// Get
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/prod/cloudwatch-alarm-template-groups/"+id,
		map[string]any{
			"name": "cw-group-updated",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "cw-group-updated", updated["name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["cloudWatchAlarmTemplateGroups"].([]any)
	assert.Len(t, items, 1)

	// Delete (204)
	rec = doRequest(t, h, http.MethodDelete, "/prod/cloudwatch-alarm-template-groups/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCWAlarmTemplateGroup_ListTemplateCount locks in a fix for a gap where
// ListCloudWatchAlarmTemplateGroups always reused the Get/Create/Update
// shape, which has NO "templateCount" field on the real API (verified
// against the SDK deserializer) -- but the real ListCloudWatchAlarmTemplate
// GroupsOutput's per-item Summary shape DOES have one. Get/Create/Update
// must still omit it; only List computes and includes the live count.
func TestCWAlarmTemplateGroup_ListTemplateCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
		map[string]any{"name": "cw-group-count"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	groupID := created["id"].(string)
	_, hasCount := created["templateCount"]
	assert.False(t, hasCount, "real Create/Get/UpdateCloudWatchAlarmTemplateGroupOutput has no templateCount field")

	for i := range 2 {
		rec = doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
			"name": "cw-tmpl-count-" + string(rune('a'+i)), "groupIdentifier": groupID,
			"metricName": "InputLossSeconds",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items := decodeBody(t, rec.Body.Bytes())["cloudWatchAlarmTemplateGroups"].([]any)
	require.Len(t, items, 1)

	group := items[0].(map[string]any)
	assert.InDelta(t, float64(2), group["templateCount"], 0)
}

func TestCWAlarmTemplate_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns 201 with metric fields",
			wantCode: http.StatusCreated,
			body: map[string]any{
				"name": "cw-template-1", "metricName": "InputLossSeconds",
				"namespace": "MediaLive", "statistic": "Sum",
				"comparisonOperator": "GreaterThanThreshold", "threshold": 0.0,
				"evaluationPeriods": 1.0, "period": 300.0,
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "InputLossSeconds", resp["metricName"])
				assert.NotEmpty(t, resp["createdAt"])
				assert.NotEmpty(t, resp["modifiedAt"])
				_, hasNamespace := resp["namespace"]
				assert.False(t, hasNamespace, "real GetCloudWatchAlarmTemplateOutput has no namespace field")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCWAlarmTemplate_GetUpdateListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
		"name": "cw-template-1", "metricName": "OutputLossSeconds",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by Name
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates/cw-template-1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/cloudwatch-alarm-templates/"+id, map[string]any{
		"metricName": "ActiveAlerts",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "ActiveAlerts", updated["metricName"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["cloudWatchAlarmTemplates"].([]any)
	assert.Len(t, items, 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/cloudwatch-alarm-templates/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}
