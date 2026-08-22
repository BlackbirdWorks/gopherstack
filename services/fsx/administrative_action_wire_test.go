package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVolumeOps_AdministrativeActionsWireShape proves gopherstack's
// RestoreVolumeFromSnapshot and CopySnapshotAndUpdateVolume responses are wrapped
// under the real op's own keys -- "AdministrativeActions"/"Lifecycle"/"VolumeId" at
// the response root -- rather than under a "Volume" key that neither op's real
// Output struct has at all (fsx@v1.68.4 api_op_RestoreVolumeFromSnapshot.go:
// RestoreVolumeFromSnapshotOutput{AdministrativeActions, Lifecycle, VolumeId};
// api_op_CopySnapshotAndUpdateVolume.go: CopySnapshotAndUpdateVolumeOutput{same
// three}; deserializers.go:17381/15903 confirm the live per-op deserializer switch
// has no case "Volume" for either op). A real client wrapping the response under
// "Volume" would silently drop every field into the deserializer's default branch.
func TestVolumeOps_AdministrativeActionsWireShape(t *testing.T) {
	t.Parallel()

	t.Run("RestoreVolumeFromSnapshot", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestFSxClient(t, h)

		volOut, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
			VolumeType: types.VolumeTypeOntap,
			Name:       aws.String("restore-vol"),
		})
		require.NoError(t, err)

		snapOut, err := client.CreateSnapshot(t.Context(), &fsxsdk.CreateSnapshotInput{
			Name:     aws.String("restore-snap"),
			VolumeId: volOut.Volume.VolumeId,
		})
		require.NoError(t, err)

		out, err := client.RestoreVolumeFromSnapshot(t.Context(), &fsxsdk.RestoreVolumeFromSnapshotInput{
			VolumeId:   volOut.Volume.VolumeId,
			SnapshotId: snapOut.Snapshot.SnapshotId,
		})
		require.NoError(t, err)

		assert.Equal(t, aws.ToString(volOut.Volume.VolumeId), aws.ToString(out.VolumeId))
		assert.NotEmpty(t, out.Lifecycle)
		require.Len(t, out.AdministrativeActions, 1)
		assert.Equal(t,
			types.AdministrativeActionTypeVolumeRestore, out.AdministrativeActions[0].AdministrativeActionType)
		require.NotNil(t, out.AdministrativeActions[0].TargetVolumeValues)
		assert.Equal(t, aws.ToString(volOut.Volume.VolumeId),
			aws.ToString(out.AdministrativeActions[0].TargetVolumeValues.VolumeId))
	})

	t.Run("CopySnapshotAndUpdateVolume", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestFSxClient(t, h)

		volOut, err := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
			VolumeType: types.VolumeTypeOntap,
			Name:       aws.String("copy-vol"),
		})
		require.NoError(t, err)

		snapOut, err := client.CreateSnapshot(t.Context(), &fsxsdk.CreateSnapshotInput{
			Name:     aws.String("copy-snap"),
			VolumeId: volOut.Volume.VolumeId,
		})
		require.NoError(t, err)

		out, err := client.CopySnapshotAndUpdateVolume(t.Context(), &fsxsdk.CopySnapshotAndUpdateVolumeInput{
			VolumeId:          volOut.Volume.VolumeId,
			SourceSnapshotARN: aws.String(aws.ToString(snapOut.Snapshot.ResourceARN)),
		})
		require.NoError(t, err)

		assert.Equal(t, aws.ToString(volOut.Volume.VolumeId), aws.ToString(out.VolumeId))
		assert.NotEmpty(t, out.Lifecycle)
		require.Len(t, out.AdministrativeActions, 1)
		assert.Equal(
			t,
			types.AdministrativeActionTypeVolumeUpdateWithSnapshot,
			out.AdministrativeActions[0].AdministrativeActionType,
		)
		require.NotNil(t, out.AdministrativeActions[0].TargetVolumeValues)
		assert.Equal(t, aws.ToString(volOut.Volume.VolumeId),
			aws.ToString(out.AdministrativeActions[0].TargetVolumeValues.VolumeId))
	})
}
