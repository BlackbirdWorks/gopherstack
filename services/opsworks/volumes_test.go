package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestVolumes verifies volume registration lifecycle.
func TestVolumes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterVolume returns VolumeId",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-1234",
					"StackId":     stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["VolumeId"])
			},
		},
		{
			name: "DescribeVolumes returns registered volume",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-5678",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "DescribeVolumes", map[string]any{
					"VolumeIds": []string{volumeID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				vols := parseJSON(t, rec.Body.Bytes())["Volumes"].([]any)
				require.Len(t, vols, 1)
				vol := vols[0].(map[string]any)
				assert.Equal(t, "vol-5678", vol["Ec2VolumeId"])
			},
		},
		{
			name: "AssignVolume and UpdateVolume",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-abc",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "AssignVolume", map[string]any{
					"VolumeId":   volumeID,
					"InstanceId": instanceID,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "UpdateVolume", map[string]any{
					"VolumeId":   volumeID,
					"Name":       "my-volume",
					"MountPoint": "/data",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "UnassignVolume returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-def",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)
				doTarget(t, h, "AssignVolume", map[string]any{
					"VolumeId": volumeID, "InstanceId": instanceID,
				})

				rec = doTarget(t, h, "UnassignVolume", map[string]any{"VolumeId": volumeID})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DeregisterVolume removes volume",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-ghi",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "DeregisterVolume", map[string]any{"VolumeId": volumeID})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeVolumes", map[string]any{
					"VolumeIds": []string{volumeID},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}
