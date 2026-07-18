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
			name:     "create returns 201 with id and SUCCEEDED status",
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "SUCCEEDED", resp["status"])
				assert.Equal(t, "NOT_DEPLOYED", resp["monitorDeploymentStatus"])
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
	assert.Equal(t, "DEPLOYED", deployResp["monitorDeploymentStatus"])

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// Get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
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
	assert.Equal(t, "DELETING", decodeBody(t, rec.Body.Bytes())["monitorDeploymentStatus"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/missing/monitor-deployment", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
