package redshift_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestServerless_SnapshotCopyConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "scc-ns"})

	rec := doServerlessOp(t, h, "CreateSnapshotCopyConfiguration", map[string]any{
		"namespaceName":           "scc-ns",
		"destinationRegion":       "us-west-2",
		"snapshotRetentionPeriod": 7,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(rec.Body), &createResp, jsonx.V1()))
	cfg, _ := createResp["snapshotCopyConfiguration"].(map[string]any)
	require.NotNil(t, cfg)
	assert.Equal(t, "scc-ns", cfg["namespaceName"])
	assert.Equal(t, "us-west-2", cfg["destinationRegion"])
	assert.InEpsilon(t, float64(7), cfg["snapshotRetentionPeriod"], 0)
	id, _ := cfg["snapshotCopyConfigurationId"].(string)
	require.NotEmpty(t, id)
	assert.NotEmpty(t, cfg["snapshotCopyConfigurationArn"])

	rec = doServerlessOp(t, h, "ListSnapshotCopyConfigurations", map[string]any{"namespaceName": "scc-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(rec.Body), &listResp, jsonx.V1()))
	list, _ := listResp["snapshotCopyConfigurations"].([]any)
	require.Len(t, list, 1)

	rec = doServerlessOp(t, h, "UpdateSnapshotCopyConfiguration", map[string]any{
		"snapshotCopyConfigurationId": id,
		"snapshotRetentionPeriod":     30,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(rec.Body), &updateResp, jsonx.V1()))
	cfg, _ = updateResp["snapshotCopyConfiguration"].(map[string]any)
	require.NotNil(t, cfg)
	assert.InEpsilon(t, float64(30), cfg["snapshotRetentionPeriod"], 0)

	rec = doServerlessOp(t, h, "DeleteSnapshotCopyConfiguration", map[string]any{
		"snapshotCopyConfigurationId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]any
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(rec.Body), &deleteResp, jsonx.V1()))
	cfg, _ = deleteResp["snapshotCopyConfiguration"].(map[string]any)
	require.NotNil(t, cfg, "DeleteSnapshotCopyConfigurationResponse echoes the deleted object on the real wire")
	assert.Equal(t, id, cfg["snapshotCopyConfigurationId"])

	rec = doServerlessOp(t, h, "ListSnapshotCopyConfigurations", map[string]any{"namespaceName": "scc-ns"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(rec.Body), &listResp, jsonx.V1()))
	list, _ = listResp["snapshotCopyConfigurations"].([]any)
	assert.Empty(t, list)
}

func TestServerless_SnapshotCopyConfiguration_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name:       "create missing namespace",
			op:         "CreateSnapshotCopyConfiguration",
			body:       map[string]any{"namespaceName": "no-such-ns", "destinationRegion": "us-west-2"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "update unknown id",
			op:         "UpdateSnapshotCopyConfiguration",
			body:       map[string]any{"snapshotCopyConfigurationId": "nope", "snapshotRetentionPeriod": 3},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "delete unknown id",
			op:         "DeleteSnapshotCopyConfiguration",
			body:       map[string]any{"snapshotCopyConfigurationId": "nope"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, tt.op, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(rec.Body), &errResp, jsonx.V1()))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}
