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
				// The real types.Volume has no StackId member; a previous
				// pass invented one and put it on the wire.
				assert.NotContains(t, vol, "StackId")
			},
		},
		{
			name: "DescribeVolumes filters by StackId",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				otherStackID := createTestStack(t, h)

				doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-in-stack",
					"StackId":     stackID,
				})
				doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-in-other-stack",
					"StackId":     otherStackID,
				})

				rec := doTarget(t, h, "DescribeVolumes", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				vols := parseJSON(t, rec.Body.Bytes())["Volumes"].([]any)
				require.Len(t, vols, 1)
				assert.Equal(t, "vol-in-stack", vols[0].(map[string]any)["Ec2VolumeId"])
			},
		},
		{
			name: "AssignVolume rejects an instance from a different stack",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				otherStackID := createTestStack(t, h)
				otherLayerID := createTestLayer(t, h, otherStackID)
				otherInstanceID := createTestInstance(t, h, otherStackID, otherLayerID)

				rec := doTarget(t, h, "RegisterVolume", map[string]any{
					"Ec2VolumeId": "vol-cross-stack",
					"StackId":     stackID,
				})
				volumeID := parseJSON(t, rec.Body.Bytes())["VolumeId"].(string)

				rec = doTarget(t, h, "AssignVolume", map[string]any{
					"VolumeId":   volumeID,
					"InstanceId": otherInstanceID,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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

// TestRegisterVolumeValidation verifies RegisterVolume rejects a missing
// StackId with ValidationException rather than falling through to the
// stack-lookup's ResourceNotFoundException. StackId is "This member is
// required" on the real RegisterVolumeInput (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_RegisterVolume.go).
func TestRegisterVolumeValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "RegisterVolume", map[string]any{"Ec2VolumeId": "vol-1234"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// TestAssignVolumeValidation verifies AssignVolume rejects a missing
// VolumeId with ValidationException rather than falling through to the
// volume-lookup's ResourceNotFoundException. VolumeId is "This member is
// required" on the real AssignVolumeInput (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_AssignVolume.go /
// validateOpAssignVolumeInput); InstanceId is not.
func TestAssignVolumeValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "AssignVolume", map[string]any{"InstanceId": "some-instance"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}
