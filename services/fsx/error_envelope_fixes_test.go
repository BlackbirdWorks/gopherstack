package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/require"
)

// These tests drive the real SDK client against operations whose own
// deserializeOpError<Op> switch (fsx@v1.68.4 deserializers.go) does not
// declare the code this backend was sending (gopherstack-6flj/uox6
// error-envelope sweep).

// TestCopySnapshotAndUpdateVolume_NotFound_MapsToBadRequest: this op's own
// switch is exactly [BadRequest, IncompatibleParameterError,
// InternalServerError, ServiceLimitExceeded] -- neither VolumeNotFound nor
// SnapshotNotFound is declared, even though both are real, legitimately
// declared types for other fsx ops (VolumeNotFound: CreateAndAttachS3AccessPoint,
// CreateBackup, CreateSnapshot, DeleteVolume, DescribeBackups and others;
// SnapshotNotFound: DeleteSnapshot, DescribeSnapshots, UpdateSnapshot).
// BadRequest ("a generic error indicating a failure with a client request",
// types/errors.go) is this op's own declared generic-client-error type and
// is already how the rest of this service answers validation-shaped
// conditions with no more specific declared type (see errValidation's doc
// comment in errors.go) -- so it is the correct substitution here, not an
// invented code.
func TestCopySnapshotAndUpdateVolume_NotFound_MapsToBadRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)
	ctx := t.Context()

	volID := createVolume(t, h, "", "ONTAP", "csuv-vol")

	t.Run("unknown snapshot", func(t *testing.T) {
		t.Parallel()

		_, err := client.CopySnapshotAndUpdateVolume(ctx, &fsxsdk.CopySnapshotAndUpdateVolumeInput{
			VolumeId:          aws.String(volID),
			SourceSnapshotARN: aws.String("arn:aws:fsx:us-east-1:000000000000:snapshot/fs-nonexistent"),
		})
		require.Error(t, err)

		var badReq *types.BadRequest
		require.ErrorAs(t, err, &badReq,
			"expected *types.BadRequest (the op's own declared type), got: %v", err)
	})

	t.Run("unknown volume", func(t *testing.T) {
		t.Parallel()

		_, err := client.CopySnapshotAndUpdateVolume(ctx, &fsxsdk.CopySnapshotAndUpdateVolumeInput{
			VolumeId:          aws.String("fsvol-nonexistent"),
			SourceSnapshotARN: aws.String("arn:aws:fsx:us-east-1:000000000000:snapshot/fs-nonexistent"),
		})
		require.Error(t, err)

		var badReq *types.BadRequest
		require.ErrorAs(t, err, &badReq,
			"expected *types.BadRequest (the op's own declared type), got: %v", err)
	})
}

// TestRestoreVolumeFromSnapshot_SnapshotNotFound_MapsToBadRequest: this op's
// own switch is exactly [BadRequest, InternalServerError, VolumeNotFound] --
// VolumeNotFound is declared (its own VolumeNotFound emission is correct and
// untouched) but SnapshotNotFound is not, unlike SnapshotNotFound's
// legitimate declarers DeleteSnapshot/DescribeSnapshots/UpdateSnapshot. Same
// BadRequest substitution as CopySnapshotAndUpdateVolume above.
func TestRestoreVolumeFromSnapshot_SnapshotNotFound_MapsToBadRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)
	ctx := t.Context()

	volID := createVolume(t, h, "", "ONTAP", "rvfs-vol")

	_, err := client.RestoreVolumeFromSnapshot(ctx, &fsxsdk.RestoreVolumeFromSnapshotInput{
		VolumeId:   aws.String(volID),
		SnapshotId: aws.String("fsvolsnap-nonexistent"),
	})
	require.Error(t, err)

	var badReq *types.BadRequest
	require.ErrorAs(t, err, &badReq,
		"expected *types.BadRequest (the op's own declared type), got: %v", err)
}

// TestTagResource_TagLimitExceeded_MapsToBadRequest: TagResource's own
// switch is [BadRequest, InternalServerError, NotServiceResourceError,
// ResourceDoesNotSupportTagging, ResourceNotFound] -- no ServiceLimitExceeded,
// unlike its legitimate declarers CopyBackup/CopySnapshotAndUpdateVolume/
// CreateBackup/CreateDataRepositoryAssociation/CreateDataRepositoryTask.
// Same BadRequest substitution: neither NotServiceResourceError nor
// ResourceDoesNotSupportTagging fit "too many tags" by their own doc
// comments, so BadRequest is the only reasonable declared type left.
func TestTagResource_TagLimitExceeded_MapsToBadRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)
	ctx := t.Context()

	volID := createVolume(t, h, "", "ONTAP", "tag-limit-vol")

	described, err := client.DescribeVolumes(ctx, &fsxsdk.DescribeVolumesInput{VolumeIds: []string{volID}})
	require.NoError(t, err)
	require.Len(t, described.Volumes, 1)
	volumeARN := described.Volumes[0].ResourceARN

	tags := make([]types.Tag, 51)
	for i := range tags {
		key := "k" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		tags[i] = types.Tag{Key: aws.String(key), Value: aws.String("v")}
	}

	_, err = client.TagResource(ctx, &fsxsdk.TagResourceInput{
		ResourceARN: volumeARN,
		Tags:        tags,
	})
	require.Error(t, err)

	var badReq *types.BadRequest
	require.ErrorAs(t, err, &badReq,
		"expected *types.BadRequest (the op's own declared type), got: %v", err)
}

// TestS3AccessPointAttachment_NotFound_RealClient: DescribeS3AccessPointAttachments
// and DetachAndDeleteS3AccessPoint both declare S3AccessPointAttachmentNotFound
// in their own switch (fsx@v1.68.4 deserializers.go), not InvalidRequest --
// the wire code the shared ErrS3AccessPointNotFound sentinel carried, which
// only fits CreateAndAttachS3AccessPoint's own declared set.
func TestS3AccessPointAttachment_NotFound_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)
	ctx := t.Context()

	assertAttachmentNotFound := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)

		var apiErr *types.S3AccessPointAttachmentNotFound
		require.ErrorAs(t, err, &apiErr,
			"expected *types.S3AccessPointAttachmentNotFound (the op's own declared type), got: %v", err)
	}

	t.Run("describe", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeS3AccessPointAttachments(ctx, &fsxsdk.DescribeS3AccessPointAttachmentsInput{
			Names: []string{"no-such-attachment"},
		})
		assertAttachmentNotFound(t, err)
	})

	t.Run("detach and delete", func(t *testing.T) {
		t.Parallel()

		_, err := client.DetachAndDeleteS3AccessPoint(ctx, &fsxsdk.DetachAndDeleteS3AccessPointInput{
			Name: aws.String("no-such-attachment"),
		})
		assertAttachmentNotFound(t, err)
	})
}
