package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func newTestSlice33DataSyncClient(t *testing.T) *datasyncsdk.Client {
	t.Helper()

	backend := datasync.NewInMemoryBackend("123456789012", "us-east-1")

	return newTestDataSyncClient(t, datasync.NewHandler(backend))
}

// TestSlice33_DataSync_RealClient drives every gopherstack-n3zi
// typed-slice-33 uncovered datasync op through the real aws-sdk-go-v2
// client (newTestDataSyncClient, shared with wire_field_fixes_test.go).
func TestSlice33_DataSync_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testAzureBlobRealClient, "azureblob"},
		{testFsxLustreWindowsRealClient, "fsx_lustre_windows"},
		{testFsxOntapOpenZfsRealClient, "fsx_ontap_openzfs"},
		{testHdfsRealClient, "hdfs"},
		{testSmbRealClient, "smb"},
		{testObjectStorageAndEfsRealClient, "objectstorage_and_efs"},
		{testLocationLifecycleAndTagsRealClient, "location_lifecycle_and_tags"},
		{testAgentAndTaskRealClient, "agent_and_task"},
		{testTaskExecutionRealClient, "task_execution"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testAzureBlobRealClient covers CreateLocationAzureBlob,
// DescribeLocationAzureBlob, UpdateLocationAzureBlob.
func testAzureBlobRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	createOut, err := client.CreateLocationAzureBlob(ctx, &datasyncsdk.CreateLocationAzureBlobInput{
		ContainerUrl:       aws.String("https://myaccount.blob.core.windows.net/mycontainer"),
		AuthenticationType: types.AzureBlobAuthenticationTypeSas,
		SasConfiguration:   &types.AzureBlobSasConfiguration{Token: aws.String("sv=2020&ss=b&sig=abc")},
	})
	require.NoError(t, err)
	locationARN := aws.ToString(createOut.LocationArn)

	descOut, err := client.DescribeLocationAzureBlob(ctx, &datasyncsdk.DescribeLocationAzureBlobInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AzureBlobAuthenticationTypeSas, descOut.AuthenticationType)

	_, err = client.UpdateLocationAzureBlob(ctx, &datasyncsdk.UpdateLocationAzureBlobInput{
		LocationArn:  aws.String(locationARN),
		Subdirectory: aws.String("/updated-subdir"),
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeLocationAzureBlob(ctx, &datasyncsdk.DescribeLocationAzureBlobInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descOut2.LocationUri), "updated-subdir")
}

// testFsxLustreWindowsRealClient covers CreateLocationFsxLustre,
// DescribeLocationFsxLustre, UpdateLocationFsxLustre,
// CreateLocationFsxWindows, DescribeLocationFsxWindows,
// UpdateLocationFsxWindows.
func testFsxLustreWindowsRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	lustreOut, err := client.CreateLocationFsxLustre(ctx, &datasyncsdk.CreateLocationFsxLustreInput{
		FsxFilesystemArn:  aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-lustre1"),
		SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
	})
	require.NoError(t, err)
	lustreARN := aws.ToString(lustreOut.LocationArn)

	lustreDescOut, err := client.DescribeLocationFsxLustre(ctx, &datasyncsdk.DescribeLocationFsxLustreInput{
		LocationArn: aws.String(lustreARN),
	})
	require.NoError(t, err)
	require.Len(t, lustreDescOut.SecurityGroupArns, 1)
	assert.Contains(t, aws.ToString(lustreDescOut.LocationUri), "fs-lustre1")

	_, err = client.UpdateLocationFsxLustre(ctx, &datasyncsdk.UpdateLocationFsxLustreInput{
		LocationArn:  aws.String(lustreARN),
		Subdirectory: aws.String("/updated-lustre-subdir"),
	})
	require.NoError(t, err)

	lustreDescOut2, err := client.DescribeLocationFsxLustre(ctx, &datasyncsdk.DescribeLocationFsxLustreInput{
		LocationArn: aws.String(lustreARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(lustreDescOut2.LocationUri), "updated-lustre-subdir")

	winOut, err := client.CreateLocationFsxWindows(ctx, &datasyncsdk.CreateLocationFsxWindowsInput{
		FsxFilesystemArn:  aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-win1"),
		SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
		User:              aws.String("admin"),
	})
	require.NoError(t, err)
	winARN := aws.ToString(winOut.LocationArn)

	winDescOut, err := client.DescribeLocationFsxWindows(ctx, &datasyncsdk.DescribeLocationFsxWindowsInput{
		LocationArn: aws.String(winARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "admin", aws.ToString(winDescOut.User))

	_, err = client.UpdateLocationFsxWindows(ctx, &datasyncsdk.UpdateLocationFsxWindowsInput{
		LocationArn: aws.String(winARN),
		User:        aws.String("admin2"),
	})
	require.NoError(t, err)

	winDescOut2, err := client.DescribeLocationFsxWindows(ctx, &datasyncsdk.DescribeLocationFsxWindowsInput{
		LocationArn: aws.String(winARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "admin2", aws.ToString(winDescOut2.User))
}

// testFsxOntapOpenZfsRealClient covers CreateLocationFsxOntap,
// DescribeLocationFsxOntap, UpdateLocationFsxOntap,
// CreateLocationFsxOpenZfs, DescribeLocationFsxOpenZfs,
// UpdateLocationFsxOpenZfs.
func testFsxOntapOpenZfsRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	ontapOut, err := client.CreateLocationFsxOntap(ctx, &datasyncsdk.CreateLocationFsxOntapInput{
		StorageVirtualMachineArn: aws.String(
			"arn:aws:fsx:us-east-1:123456789012:storage-virtual-machine/fs-ontap1/svm-1",
		),
		SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
		Protocol:          &types.FsxProtocol{NFS: &types.FsxProtocolNfs{}},
	})
	require.NoError(t, err)
	ontapARN := aws.ToString(ontapOut.LocationArn)

	ontapDescOut, err := client.DescribeLocationFsxOntap(ctx, &datasyncsdk.DescribeLocationFsxOntapInput{
		LocationArn: aws.String(ontapARN),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"arn:aws:fsx:us-east-1:123456789012:storage-virtual-machine/fs-ontap1/svm-1",
		aws.ToString(ontapDescOut.StorageVirtualMachineArn),
	)

	_, err = client.UpdateLocationFsxOntap(ctx, &datasyncsdk.UpdateLocationFsxOntapInput{
		LocationArn:  aws.String(ontapARN),
		Subdirectory: aws.String("/updated-ontap-subdir"),
	})
	require.NoError(t, err)

	ontapDescOut2, err := client.DescribeLocationFsxOntap(ctx, &datasyncsdk.DescribeLocationFsxOntapInput{
		LocationArn: aws.String(ontapARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(ontapDescOut2.LocationUri), "updated-ontap-subdir")

	openZfsOut, err := client.CreateLocationFsxOpenZfs(ctx, &datasyncsdk.CreateLocationFsxOpenZfsInput{
		FsxFilesystemArn:  aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-openzfs1"),
		SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
		Protocol:          &types.FsxProtocol{NFS: &types.FsxProtocolNfs{}},
	})
	require.NoError(t, err)
	openZfsARN := aws.ToString(openZfsOut.LocationArn)

	openZfsDescOut, err := client.DescribeLocationFsxOpenZfs(ctx, &datasyncsdk.DescribeLocationFsxOpenZfsInput{
		LocationArn: aws.String(openZfsARN),
	})
	require.NoError(t, err)
	require.NotNil(t, openZfsDescOut.Protocol)
	assert.Contains(t, aws.ToString(openZfsDescOut.LocationUri), "fs-openzfs1")

	_, err = client.UpdateLocationFsxOpenZfs(ctx, &datasyncsdk.UpdateLocationFsxOpenZfsInput{
		LocationArn:  aws.String(openZfsARN),
		Subdirectory: aws.String("/updated-openzfs-subdir"),
	})
	require.NoError(t, err)

	openZfsDescOut2, err := client.DescribeLocationFsxOpenZfs(ctx, &datasyncsdk.DescribeLocationFsxOpenZfsInput{
		LocationArn: aws.String(openZfsARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(openZfsDescOut2.LocationUri), "updated-openzfs-subdir")
}

// testHdfsRealClient covers CreateLocationHdfs, DescribeLocationHdfs,
// UpdateLocationHdfs.
func testHdfsRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	agentOut, err := client.CreateAgent(
		ctx,
		&datasyncsdk.CreateAgentInput{ActivationKey: aws.String("activation-key-1")},
	)
	require.NoError(t, err)
	agentARN := aws.ToString(agentOut.AgentArn)

	createOut, err := client.CreateLocationHdfs(ctx, &datasyncsdk.CreateLocationHdfsInput{
		AgentArns:          []string{agentARN},
		AuthenticationType: types.HdfsAuthenticationTypeSimple,
		SimpleUser:         aws.String("hdfsuser"),
		NameNodes: []types.HdfsNameNode{
			{Hostname: aws.String("namenode.example.com"), Port: aws.Int32(8020)},
		},
	})
	require.NoError(t, err)
	locationARN := aws.ToString(createOut.LocationArn)

	descOut, err := client.DescribeLocationHdfs(ctx, &datasyncsdk.DescribeLocationHdfsInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	require.Len(t, descOut.NameNodes, 1)
	assert.Equal(t, "namenode.example.com", aws.ToString(descOut.NameNodes[0].Hostname))

	_, err = client.UpdateLocationHdfs(ctx, &datasyncsdk.UpdateLocationHdfsInput{
		LocationArn:  aws.String(locationARN),
		Subdirectory: aws.String("/updated-hdfs-subdir"),
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeLocationHdfs(ctx, &datasyncsdk.DescribeLocationHdfsInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descOut2.LocationUri), "updated-hdfs-subdir")
}

// testSmbRealClient covers CreateLocationSmb, DescribeLocationSmb,
// UpdateLocationSmb.
func testSmbRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	agentOut, err := client.CreateAgent(
		ctx,
		&datasyncsdk.CreateAgentInput{ActivationKey: aws.String("activation-key-2")},
	)
	require.NoError(t, err)
	agentARN := aws.ToString(agentOut.AgentArn)

	createOut, err := client.CreateLocationSmb(ctx, &datasyncsdk.CreateLocationSmbInput{
		AgentArns:      []string{agentARN},
		ServerHostname: aws.String("smb.example.com"),
		Subdirectory:   aws.String("/share"),
		User:           aws.String("smbuser"),
	})
	require.NoError(t, err)
	locationARN := aws.ToString(createOut.LocationArn)

	descOut, err := client.DescribeLocationSmb(ctx, &datasyncsdk.DescribeLocationSmbInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "smbuser", aws.ToString(descOut.User))

	_, err = client.UpdateLocationSmb(ctx, &datasyncsdk.UpdateLocationSmbInput{
		LocationArn: aws.String(locationARN),
		User:        aws.String("smbuser2"),
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeLocationSmb(ctx, &datasyncsdk.DescribeLocationSmbInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "smbuser2", aws.ToString(descOut2.User))
}

// testObjectStorageAndEfsRealClient covers DescribeLocationObjectStorage,
// UpdateLocationObjectStorage, UpdateLocationEfs (CreateLocationObjectStorage
// and CreateLocationEfs are already typed-covered elsewhere, used here only
// to seed state).
func testObjectStorageAndEfsRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	objOut, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("obj.example.com"),
		BucketName:     aws.String("obj-bucket"),
	})
	require.NoError(t, err)
	objARN := aws.ToString(objOut.LocationArn)

	objDescOut, err := client.DescribeLocationObjectStorage(ctx, &datasyncsdk.DescribeLocationObjectStorageInput{
		LocationArn: aws.String(objARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(objDescOut.LocationUri), "obj.example.com")

	_, err = client.UpdateLocationObjectStorage(ctx, &datasyncsdk.UpdateLocationObjectStorageInput{
		LocationArn:  aws.String(objARN),
		Subdirectory: aws.String("/updated-obj-subdir"),
	})
	require.NoError(t, err)

	objDescOut2, err := client.DescribeLocationObjectStorage(ctx, &datasyncsdk.DescribeLocationObjectStorageInput{
		LocationArn: aws.String(objARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(objDescOut2.LocationUri), "updated-obj-subdir")

	efsOut, err := client.CreateLocationEfs(ctx, &datasyncsdk.CreateLocationEfsInput{
		Ec2Config: &types.Ec2Config{
			SubnetArn:         aws.String("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"),
			SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
		},
		EfsFilesystemArn: aws.String("arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-1"),
	})
	require.NoError(t, err)
	efsARN := aws.ToString(efsOut.LocationArn)

	_, err = client.UpdateLocationEfs(ctx, &datasyncsdk.UpdateLocationEfsInput{
		LocationArn:  aws.String(efsARN),
		Subdirectory: aws.String("/updated-efs-subdir"),
	})
	require.NoError(t, err)

	efsDescOut, err := client.DescribeLocationEfs(ctx, &datasyncsdk.DescribeLocationEfsInput{
		LocationArn: aws.String(efsARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(efsDescOut.LocationUri), "updated-efs-subdir")
}

// testLocationLifecycleAndTagsRealClient covers DeleteLocation and
// UntagResource.
func testLocationLifecycleAndTagsRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	createOut, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("del.example.com"),
		BucketName:     aws.String("del-bucket"),
		Tags: []types.TagListEntry{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	locationARN := aws.ToString(createOut.LocationArn)

	listTagsOut, err := client.ListTagsForResource(ctx, &datasyncsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	require.Len(t, listTagsOut.Tags, 1)

	_, err = client.UntagResource(ctx, &datasyncsdk.UntagResourceInput{
		ResourceArn: aws.String(locationARN),
		Keys:        []string{"env"},
	})
	require.NoError(t, err)

	listTagsOut2, err := client.ListTagsForResource(ctx, &datasyncsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(locationARN),
	})
	require.NoError(t, err)
	assert.Empty(t, listTagsOut2.Tags)

	_, err = client.DeleteLocation(ctx, &datasyncsdk.DeleteLocationInput{
		LocationArn: aws.String(locationARN),
	})
	require.NoError(t, err)

	_, err = client.DescribeLocationObjectStorage(ctx, &datasyncsdk.DescribeLocationObjectStorageInput{
		LocationArn: aws.String(locationARN),
	})
	require.Error(t, err)
}

// testAgentAndTaskRealClient covers UpdateAgent and UpdateTask.
func testAgentAndTaskRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	agentOut, err := client.CreateAgent(
		ctx,
		&datasyncsdk.CreateAgentInput{ActivationKey: aws.String("activation-key-3")},
	)
	require.NoError(t, err)
	agentARN := aws.ToString(agentOut.AgentArn)

	_, err = client.UpdateAgent(ctx, &datasyncsdk.UpdateAgentInput{
		AgentArn: aws.String(agentARN),
		Name:     aws.String("renamed-agent"),
	})
	require.NoError(t, err)

	agentDescOut, err := client.DescribeAgent(ctx, &datasyncsdk.DescribeAgentInput{AgentArn: aws.String(agentARN)})
	require.NoError(t, err)
	assert.Equal(t, "renamed-agent", aws.ToString(agentDescOut.Name))

	srcOut, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("src.example.com"), BucketName: aws.String("src-bucket"),
	})
	require.NoError(t, err)

	dstOut, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("dst.example.com"), BucketName: aws.String("dst-bucket"),
	})
	require.NoError(t, err)

	taskOut, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
		SourceLocationArn:      srcOut.LocationArn,
		DestinationLocationArn: dstOut.LocationArn,
	})
	require.NoError(t, err)
	taskARN := aws.ToString(taskOut.TaskArn)

	_, err = client.UpdateTask(ctx, &datasyncsdk.UpdateTaskInput{
		TaskArn: aws.String(taskARN),
		Name:    aws.String("renamed-task"),
	})
	require.NoError(t, err)

	taskDescOut, err := client.DescribeTask(ctx, &datasyncsdk.DescribeTaskInput{TaskArn: aws.String(taskARN)})
	require.NoError(t, err)
	assert.Equal(t, "renamed-task", aws.ToString(taskDescOut.Name))
}

// testTaskExecutionRealClient covers CancelTaskExecution and
// UpdateTaskExecution.
func testTaskExecutionRealClient(t *testing.T) {
	t.Helper()

	client := newTestSlice33DataSyncClient(t)
	ctx := t.Context()

	newTask := func(t *testing.T) string {
		t.Helper()

		srcOut, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("te-src.example.com"), BucketName: aws.String("te-src-bucket-" + t.Name()),
		})
		require.NoError(t, err)

		dstOut, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("te-dst.example.com"), BucketName: aws.String("te-dst-bucket-" + t.Name()),
		})
		require.NoError(t, err)

		taskOut, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
			SourceLocationArn:      srcOut.LocationArn,
			DestinationLocationArn: dstOut.LocationArn,
		})
		require.NoError(t, err)

		return aws.ToString(taskOut.TaskArn)
	}

	updTaskARN := newTask(t)

	startOut, err := client.StartTaskExecution(ctx, &datasyncsdk.StartTaskExecutionInput{
		TaskArn: aws.String(updTaskARN),
	})
	require.NoError(t, err)
	execARN := aws.ToString(startOut.TaskExecutionArn)

	_, err = client.UpdateTaskExecution(ctx, &datasyncsdk.UpdateTaskExecutionInput{
		TaskExecutionArn: aws.String(execARN),
		Options:          &types.Options{LogLevel: types.LogLevelBasic},
	})
	require.NoError(t, err)

	cancelTaskARN := newTask(t)

	cancelStartOut, err := client.StartTaskExecution(ctx, &datasyncsdk.StartTaskExecutionInput{
		TaskArn: aws.String(cancelTaskARN),
	})
	require.NoError(t, err)
	cancelExecARN := aws.ToString(cancelStartOut.TaskExecutionArn)

	_, err = client.CancelTaskExecution(ctx, &datasyncsdk.CancelTaskExecutionInput{
		TaskExecutionArn: aws.String(cancelExecARN),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeTaskExecution(ctx, &datasyncsdk.DescribeTaskExecutionInput{
		TaskExecutionArn: aws.String(cancelExecARN),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TaskExecutionStatusError, descOut.Status)
}
