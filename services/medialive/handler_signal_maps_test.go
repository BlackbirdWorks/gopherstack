package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignalMap_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns 201 with id and CREATE_COMPLETE status",
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "CREATE_COMPLETE", resp["status"])
				monitorDeployment, _ := resp["monitorDeployment"].(map[string]any)
				assert.Equal(t, "NOT_DEPLOYED", monitorDeployment["status"])
				assert.NotEmpty(t, resp["createdAt"])
				assert.NotEmpty(t, resp["modifiedAt"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
				"name":                   "test-signal-map",
				"discoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:channel:abc123",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestSignalMap_GetListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Create
	rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
		"name": "sig-map-1",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by Name
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/sig-map-1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["signalMaps"].([]any)
	assert.Len(t, items, 1)

	// StartUpdateSignalMap (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/signal-maps/"+id, map[string]any{
		"name": "sig-map-updated",
	})
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// StartMonitorDeployment
	rec = doRequest(t, h, http.MethodPost, "/prod/signal-maps/"+id+"/monitor-deployment", nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var deployResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deployResp))
	deployMonitorDeployment, _ := deployResp["monitorDeployment"].(map[string]any)
	assert.Equal(t, "DEPLOYMENT_COMPLETE", deployMonitorDeployment["status"])

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// Get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestListSignalMaps_OmitsGetOnlyFields verifies gopherstack-uult: ListSignalMaps
// must emit only types.SignalMapSummary's members (arn, createdAt, id,
// monitorDeploymentStatus, name, status, description, modifiedAt, tags) --
// medialive@v1.101.4 types/types.go:7724-7765. discoveryEntryPointArn,
// cloudWatchAlarmTemplateGroupIds and eventBridgeRuleTemplateGroupIds are
// Get/Create/StartUpdate-only and must not leak. tags DOES belong on the
// summary (unlike most siblings in this sweep) -- SignalMapSummary carries
// it per deserializers.go:48922-48924, so it must NOT be dropped.
func TestListSignalMaps_OmitsGetOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
		"name":                   "sm-scoped",
		"discoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:channel:abc123",
		"tags":                   map[string]any{"env": "prod"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SignalMaps []map[string]any `json:"signalMaps"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SignalMaps, 1)

	item := out.SignalMaps[0]
	keys := make([]string, 0, len(item))
	for k := range item {
		keys = append(keys, k)
	}
	assert.ElementsMatch(t,
		[]string{
			"arn", "id", "name", "description", "status",
			"monitorDeploymentStatus", "createdAt", "modifiedAt", "tags",
		},
		keys,
	)
	assert.Contains(t, item, "tags")
	assert.NotContains(t, item, "discoveryEntryPointArn")
	assert.NotContains(t, item, "cloudWatchAlarmTemplateGroupIds")
	assert.NotContains(t, item, "eventBridgeRuleTemplateGroupIds")
}

func TestStartDeleteMonitorDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{"name": "sm-1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	id := created["id"].(string)
	assert.NotEmpty(t, created["createdAt"])
	assert.NotEmpty(t, created["modifiedAt"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/"+id+"/monitor-deployment", nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	deleteMonitorDeployment, _ := decodeBody(t, rec.Body.Bytes())["monitorDeployment"].(map[string]any)
	assert.Equal(t, "DELETE_COMPLETE", deleteMonitorDeployment["status"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/missing/monitor-deployment", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
