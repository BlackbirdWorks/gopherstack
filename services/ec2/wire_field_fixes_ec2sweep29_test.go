package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeAggregateIdFormat_Statuses_RealClient covers
// handleDescribeAggregateIDFormat, whose response wrapped the status list
// under "statuses" instead of the "statusSet" element
// awsEc2query_deserializeOpDocumentDescribeAggregateIdFormatOutput actually
// matches (ec2@v1.319.1 deserializers.go:196919), so a real client always
// decoded Statuses as empty regardless of what the backend returned.
func TestDescribeAggregateIdFormat_Statuses_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	out, err := client.DescribeAggregateIdFormat(t.Context(), &ec2sdk.DescribeAggregateIdFormatInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Statuses, "Statuses empty - pre-fix wrapper key was \"statuses\" not \"statusSet\"")
}

// TestDescribePrincipalIdFormat_Principals_RealClient covers
// handleDescribePrincipalIDFormat, which wrapped its list under "principals"
// instead of the "principalSet" element
// awsEc2query_deserializeOpDocumentDescribePrincipalIdFormatOutput actually
// matches (ec2@v1.319.1 deserializers.go:203012), and flattened each entry
// to a bare IdFormat instead of the PrincipalIdFormat{Arn, Statuses} shape
// awsEc2query_deserializeDocumentPrincipalIdFormat expects
// (deserializers.go:143696). A real client always decoded Principals as
// empty regardless of what the backend returned.
func TestDescribePrincipalIdFormat_Principals_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	out, err := client.DescribePrincipalIdFormat(t.Context(), &ec2sdk.DescribePrincipalIdFormatInput{})
	require.NoError(t, err)
	require.Len(t, out.Principals, 1, "Principals empty - pre-fix wrapper key was \"principals\" not \"principalSet\"")
	assert.NotEmpty(t, out.Principals[0].Statuses, "nested Statuses empty - pre-fix flattened item shape dropped it")
}

// TestDescribeExportTasks_InstanceExportDetails_RealClient covers
// handleCreateInstanceExportTask/handleDescribeExportTasks, whose exportTaskItem
// wrapped the instance details under "instanceExportDetails" instead of the
// "instanceExport" element awsEc2query_deserializeDocumentExportTask actually
// matches (ec2@v1.319.1 deserializers.go:100167), so a real client always
// decoded InstanceExportDetails as a zero value regardless of what the
// backend returned.
func TestDescribeExportTasks_InstanceExportDetails_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-sweep29", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instanceID := insts[0].ID

	createOut, err := client.CreateInstanceExportTask(t.Context(), &ec2sdk.CreateInstanceExportTaskInput{
		InstanceId:        aws.String(instanceID),
		TargetEnvironment: types.ExportEnvironmentVmware,
		ExportToS3Task: &types.ExportToS3TaskSpecification{
			DiskImageFormat: types.DiskImageFormatVmdk,
			ContainerFormat: types.ContainerFormatOva,
			S3Bucket:        aws.String("sweep29-bucket"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ExportTask.InstanceExportDetails,
		"InstanceExportDetails nil - pre-fix wrapper key was \"instanceExportDetails\" not \"instanceExport\"")
	assert.Equal(t, instanceID, aws.ToString(createOut.ExportTask.InstanceExportDetails.InstanceId))

	describeOut, err := client.DescribeExportTasks(t.Context(), &ec2sdk.DescribeExportTasksInput{})
	require.NoError(t, err)
	require.Len(t, describeOut.ExportTasks, 1)
	require.NotNil(t, describeOut.ExportTasks[0].InstanceExportDetails)
	assert.Equal(t, instanceID, aws.ToString(describeOut.ExportTasks[0].InstanceExportDetails.InstanceId))
}

// TestDescribeInstanceImageMetadata_ImageMetadata_RealClient covers
// handleDescribeInstanceImageMetadata, whose instanceImageMetadataItem put
// ImageId/ImageState directly on the top-level element instead of nesting
// them under "imageMetadata", the ImageMetadata sub-object
// awsEc2query_deserializeDocumentInstanceImageMetadata actually decodes
// (ec2@v1.319.1 deserializers.go:112881, imageMetadata shape at
// deserializers.go:107294), so a real client always decoded ImageMetadata
// as nil regardless of what the backend returned.
func TestDescribeInstanceImageMetadata_ImageMetadata_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-sweep29", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instanceID := insts[0].ID

	out, err := client.DescribeInstanceImageMetadata(
		t.Context(), &ec2sdk.DescribeInstanceImageMetadataInput{InstanceIds: []string{instanceID}},
	)
	require.NoError(t, err)
	require.Len(t, out.InstanceImageMetadata, 1)

	meta := out.InstanceImageMetadata[0]
	assert.Equal(t, instanceID, aws.ToString(meta.InstanceId))
	require.NotNil(t, meta.ImageMetadata,
		"ImageMetadata nil - pre-fix imageId/imageState sat at the top level, not nested under imageMetadata")
	assert.Equal(t, "ami-sweep29", aws.ToString(meta.ImageMetadata.ImageId))
	assert.NotEmpty(t, meta.ImageMetadata.State, "ImageMetadata.State empty - pre-fix wrong nesting")
}

// TestDescribeLockedSnapshots_LockDuration_RealClient covers
// handleDescribeLockedSnapshots, whose snapshotLockItem emitted the lock
// duration under "lockDurationDays" instead of "lockDuration", the key
// awsEc2query_deserializeDocumentLockedSnapshotsInfo actually matches
// (ec2@v1.319.1 deserializers.go:132176), so a real client always decoded
// LockDuration as nil regardless of what the backend returned.
func TestDescribeLockedSnapshots_LockDuration_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)
	snap, err := b.CreateSnapshot(vol.ID, "sweep29-lock")
	require.NoError(t, err)

	_, err = b.LockSnapshot(snap.SnapshotID, "compliance", 45)
	require.NoError(t, err)

	out, err := client.DescribeLockedSnapshots(t.Context(), &ec2sdk.DescribeLockedSnapshotsInput{
		SnapshotIds: []string{snap.SnapshotID},
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	require.NotNil(t, out.Snapshots[0].LockDuration,
		"LockDuration nil - pre-fix wrapper key was \"lockDurationDays\" not \"lockDuration\"")
	assert.EqualValues(t, 45, *out.Snapshots[0].LockDuration)
}

// TestDescribeIamInstanceProfileAssociations_ProfileID_RealClient covers
// handleDescribeIamInstanceProfileAssociations, whose iamProfileSpec emitted
// the profile's second member under "name" instead of "id", the key
// awsEc2query_deserializeDocumentIamInstanceProfile actually matches
// (ec2@v1.319.1 deserializers.go:105766), so a real client always decoded
// IamInstanceProfile.Id as nil regardless of what the backend returned.
func TestDescribeIamInstanceProfileAssociations_ProfileID_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-sweep29", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instanceID := insts[0].ID

	assocOut, err := client.AssociateIamInstanceProfile(t.Context(), &ec2sdk.AssociateIamInstanceProfileInput{
		InstanceId: aws.String(instanceID),
		IamInstanceProfile: &types.IamInstanceProfileSpecification{
			Arn: aws.String("arn:aws:iam::000000000000:instance-profile/sweep29-role"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.IamInstanceProfileAssociation)
	require.NotNil(t, assocOut.IamInstanceProfileAssociation.IamInstanceProfile)
	assert.NotEmpty(
		t, aws.ToString(assocOut.IamInstanceProfileAssociation.IamInstanceProfile.Id),
		"IamInstanceProfile.Id empty - pre-fix wrapper key was \"name\" not \"id\"",
	)

	descOut, err := client.DescribeIamInstanceProfileAssociations(
		t.Context(), &ec2sdk.DescribeIamInstanceProfileAssociationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, descOut.IamInstanceProfileAssociations, 1)
	require.NotNil(t, descOut.IamInstanceProfileAssociations[0].IamInstanceProfile)
	assert.NotEmpty(t, aws.ToString(descOut.IamInstanceProfileAssociations[0].IamInstanceProfile.Id))
}

// TestDescribeImportSnapshotTasks_SnapshotTaskDetail_RealClient covers
// handleImportSnapshot/handleDescribeImportSnapshotTasks, whose
// importSnapshotTaskItem/importSnapshotResponse put status directly at the
// top level instead of nesting it under "snapshotTaskDetail", the element
// awsEc2query_deserializeDocumentImportSnapshotTask actually matches
// (ec2@v1.319.1 deserializers.go:109707, detail shape at
// deserializers.go:158042) -- there is no top-level "status" case at all,
// so a real client always decoded SnapshotTaskDetail as nil regardless of
// what the backend returned.
func TestDescribeImportSnapshotTasks_SnapshotTaskDetail_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	importOut, err := client.ImportSnapshot(t.Context(), &ec2sdk.ImportSnapshotInput{
		Description: aws.String("sweep29-import"),
	})
	require.NoError(t, err)
	require.NotNil(t, importOut.SnapshotTaskDetail,
		"SnapshotTaskDetail nil - pre-fix status sat at the top level, not nested under snapshotTaskDetail")
	assert.NotEmpty(t, aws.ToString(importOut.SnapshotTaskDetail.Status))

	describeOut, err := client.DescribeImportSnapshotTasks(t.Context(), &ec2sdk.DescribeImportSnapshotTasksInput{
		ImportTaskIds: []string{aws.ToString(importOut.ImportTaskId)},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.ImportSnapshotTasks, 1)
	require.NotNil(t, describeOut.ImportSnapshotTasks[0].SnapshotTaskDetail)
	assert.NotEmpty(t, aws.ToString(describeOut.ImportSnapshotTasks[0].SnapshotTaskDetail.Status))
}
