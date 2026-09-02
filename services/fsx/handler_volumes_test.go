package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_Volume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		volType  string
		volName  string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create ONTAP volume",
			volType:  "ONTAP",
			volName:  "vol1",
			wantCode: http.StatusOK,
		},
		{
			name:     "create OPENZFS volume",
			volType:  "OPENZFS",
			volName:  "vol2",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing VolumeType returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, tc.volType)
				body = map[string]any{
					"VolumeType": tc.volType,
					"Name":       tc.volName,
				}

				switch tc.volType {
				case "ONTAP":
					svmID := createSVM(t, h, fsID, "svm1")
					body["OntapConfiguration"] = map[string]any{"StorageVirtualMachineId": svmID}
				case "OPENZFS":
					body["OpenZFSConfiguration"] = map[string]any{
						"ParentVolumeId": openZFSRootVolumeID(t, h, fsID),
					}
				}
			} else {
				body = map[string]any{"Name": "vol1"}
			}

			rec := doFSxRequest(t, h, "CreateVolume", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				v := out["Volume"].(map[string]any)
				assert.Contains(t, v["VolumeId"].(string), "fsvol-")
				assert.Equal(t, "AVAILABLE", v["Lifecycle"])
				assert.Equal(t, tc.volType, v["VolumeType"])
			}
		})
	}
}

func TestFSx_VolumeLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		assert.Equal(t, 1, fsx.VolumeCount(b))

		// describe
		rec := doFSxRequest(t, h, "DescribeVolumes", map[string]any{
			"VolumeIds": []string{volID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["Volumes"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateVolume", map[string]any{
			"VolumeId": volID,
			"Name":     "vol-renamed",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "vol-renamed", ur["Volume"].(map[string]any)["Name"])

		// delete
		rec3 := doFSxRequest(t, h, "DeleteVolume", map[string]any{"VolumeId": volID})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, volID, del["VolumeId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.VolumeCount(b))
	})
}

func TestFSx_CreateVolumeFromBackup(t *testing.T) {
	t.Parallel()

	t.Run("creates volume from backup", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		fsID := createFS(t, h, "ONTAP")
		backupID := createFSandBackup(t, h, "ONTAP")

		svmRec := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
			"FileSystemId": fsID,
			"Name":         "svm-for-restore",
		})
		require.Equal(t, http.StatusOK, svmRec.Code)
		var svmOut map[string]any
		require.NoError(t, json.Unmarshal(svmRec.Body.Bytes(), &svmOut))
		svmID := svmOut["StorageVirtualMachine"].(map[string]any)["StorageVirtualMachineId"].(string)

		rec := doFSxRequest(t, h, "CreateVolumeFromBackup", map[string]any{
			"BackupId": backupID,
			"Name":     "restored-vol",
			"OntapConfiguration": map[string]any{
				"StorageVirtualMachineId": svmID,
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		v := out["Volume"].(map[string]any)
		assert.Contains(t, v["VolumeId"].(string), "fsvol-")
		assert.Equal(t, "restored-vol", v["Name"])
		gotOntap := v["OntapConfiguration"].(map[string]any)
		assert.Equal(t, svmID, gotOntap["StorageVirtualMachineId"],
			"real types.Volume carries the SVM nested under OntapConfiguration, not a top-level field")
		assert.Equal(t, fsID, v["FileSystemId"], "FileSystemId must be derived from the resolved SVM")
	})

	t.Run("unknown backup returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		fsID := createFS(t, h, "ONTAP")
		svmRec := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
			"FileSystemId": fsID,
			"Name":         "svm-for-restore",
		})
		require.Equal(t, http.StatusOK, svmRec.Code)
		var svmOut map[string]any
		require.NoError(t, json.Unmarshal(svmRec.Body.Bytes(), &svmOut))
		svmID := svmOut["StorageVirtualMachine"].(map[string]any)["StorageVirtualMachineId"].(string)

		rec := doFSxRequest(t, h, "CreateVolumeFromBackup", map[string]any{
			"BackupId": "backup-does-not-exist",
			"Name":     "restored-vol",
			"OntapConfiguration": map[string]any{
				"StorageVirtualMachineId": svmID,
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing OntapConfiguration returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		backupID := createFSandBackup(t, h, "ONTAP")

		rec := doFSxRequest(t, h, "CreateVolumeFromBackup", map[string]any{
			"BackupId": backupID,
			"Name":     "restored-vol",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestFSx_RestoreVolumeFromSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("restore volume from snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		// create snapshot
		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapID := cr["Snapshot"].(map[string]any)["SnapshotId"].(string)

		// restore
		rec2 := doFSxRequest(t, h, "RestoreVolumeFromSnapshot", map[string]any{
			"VolumeId":   volID,
			"SnapshotId": snapID,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
		assert.Equal(t, volID, out["VolumeId"])
		assert.NotEmpty(t, out["Lifecycle"])
		actions := out["AdministrativeActions"].([]any)
		require.Len(t, actions, 1)
		action := actions[0].(map[string]any)
		assert.Equal(t, "VOLUME_RESTORE", action["AdministrativeActionType"])
		assert.Equal(t, volID, action["TargetVolumeValues"].(map[string]any)["VolumeId"])
	})
}

// TestCreateVolume_RealRequestShape proves gopherstack's CreateVolume reads
// the real CreateVolumeInput wire shape (fsx@v1.68.4 api_op_CreateVolume.go):
// there is no top-level FileSystemId/StorageVirtualMachineId at all --
// StorageVirtualMachineId lives nested under OntapConfiguration (required for
// VolumeType=ONTAP) and OpenZFSConfiguration.ParentVolumeId is the equivalent
// anchor for VolumeType=OPENZFS. Before this fix, gopherstack read top-level
// FileSystemId/StorageVirtualMachineId fields a real client never populates,
// so a real CreateVolume call silently produced a volume with no SVM/file
// system association at all instead of failing or resolving it correctly.
func TestCreateVolume_RealRequestShape(t *testing.T) {
	t.Parallel()

	t.Run(
		"ONTAP volume resolves FileSystemId/StorageVirtualMachineId from OntapConfiguration",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			client := newTestFSxClient(t, h)

			fsOut, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeOntap,
				SubnetIds:       []string{"subnet-0123abcd", "subnet-0456efab"},
				StorageCapacity: aws.Int32(1024),
				OntapConfiguration: &types.CreateFileSystemOntapConfiguration{
					DeploymentType:     types.OntapDeploymentTypeMultiAz1,
					PreferredSubnetId:  aws.String("subnet-0123abcd"),
					ThroughputCapacity: aws.Int32(128),
				},
			})
			require.NoError(t, err)

			svmOut, err := client.CreateStorageVirtualMachine(
				t.Context(),
				&fsxsdk.CreateStorageVirtualMachineInput{
					FileSystemId: fsOut.FileSystem.FileSystemId,
					Name:         aws.String("svm1"),
				},
			)
			require.NoError(t, err)

			volOut, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
				VolumeType: types.VolumeTypeOntap,
				Name:       aws.String("vol1"),
				OntapConfiguration: &types.CreateOntapVolumeConfiguration{
					StorageVirtualMachineId: svmOut.StorageVirtualMachine.StorageVirtualMachineId,
				},
			})
			require.NoError(t, err)
			// FileSystemId resolving correctly proves the SVM reference was
			// looked up for real (an unknown/dropped SVM would have failed
			// lookup or left this empty). Real types.Volume nests
			// StorageVirtualMachineId under OntapConfiguration (see
			// TestVolume_StorageVirtualMachineIdWireShape in
			// wire_field_fixes_test.go for the dedicated wire-shape proof);
			// asserted here too since this test already has the SVM ID in hand.
			assert.Equal(
				t,
				aws.ToString(fsOut.FileSystem.FileSystemId),
				aws.ToString(volOut.Volume.FileSystemId),
			)
			require.NotNil(t, volOut.Volume.OntapConfiguration)
			assert.Equal(
				t,
				aws.ToString(svmOut.StorageVirtualMachine.StorageVirtualMachineId),
				aws.ToString(volOut.Volume.OntapConfiguration.StorageVirtualMachineId),
			)
		},
	)

	t.Run(
		"OPENZFS volume resolves FileSystemId from OpenZFSConfiguration.ParentVolumeId",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			client := newTestFSxClient(t, h)

			fsOut, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeOpenzfs,
				SubnetIds:       []string{"subnet-0123abcd"},
				StorageCapacity: aws.Int32(64),
				OpenZFSConfiguration: &types.CreateFileSystemOpenZFSConfiguration{
					DeploymentType:     types.OpenZFSDeploymentTypeSingleAz1,
					ThroughputCapacity: aws.Int32(64),
				},
			})
			require.NoError(t, err)
			rootVolumeID := fsOut.FileSystem.OpenZFSConfiguration.RootVolumeId
			require.NotEmpty(t, aws.ToString(rootVolumeID))

			volOut, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
				VolumeType: types.VolumeTypeOpenzfs,
				Name:       aws.String("child-vol"),
				OpenZFSConfiguration: &types.CreateOpenZFSVolumeConfiguration{
					ParentVolumeId: rootVolumeID,
				},
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				aws.ToString(fsOut.FileSystem.FileSystemId),
				aws.ToString(volOut.Volume.FileSystemId),
			)
		},
	)

	t.Run(
		"ONTAP volume with no OntapConfiguration returns MissingVolumeConfiguration",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			client := newTestFSxClient(t, h)

			_, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
				VolumeType: types.VolumeTypeOntap,
				Name:       aws.String("vol1"),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "MissingVolumeConfiguration")
		},
	)

	t.Run(
		"ONTAP volume with unknown StorageVirtualMachineId returns StorageVirtualMachineNotFound",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			client := newTestFSxClient(t, h)

			_, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
				VolumeType: types.VolumeTypeOntap,
				Name:       aws.String("vol1"),
				OntapConfiguration: &types.CreateOntapVolumeConfiguration{
					StorageVirtualMachineId: aws.String("svm-does-not-exist"),
				},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "StorageVirtualMachineNotFound")
		},
	)
}
