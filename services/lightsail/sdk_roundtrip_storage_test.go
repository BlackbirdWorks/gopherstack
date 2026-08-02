package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/require"
)

// TestKeyPairAndStaticIPRoundTrip exercises family G+H end to end.
func TestKeyPairAndStaticIPRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKeyPair(ctx, &lightsailsdk.CreateKeyPairInput{KeyPairName: aws.String("kp-1")})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.PrivateKeyBase64))
	require.NotEmpty(t, aws.ToString(createOut.PublicKeyBase64))
	require.Equal(t, "kp-1", aws.ToString(createOut.KeyPair.Name))

	getOut, err := client.GetKeyPair(ctx, &lightsailsdk.GetKeyPairInput{KeyPairName: aws.String("kp-1")})
	require.NoError(t, err)
	require.Equal(t, "kp-1", aws.ToString(getOut.KeyPair.Name))

	listOut, err := client.GetKeyPairs(ctx, &lightsailsdk.GetKeyPairsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.KeyPairs, 1)

	dlOut, err := client.DownloadDefaultKeyPair(ctx, &lightsailsdk.DownloadDefaultKeyPairInput{})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(dlOut.PrivateKeyBase64))

	_, err = client.DeleteKeyPair(ctx, &lightsailsdk.DeleteKeyPairInput{KeyPairName: aws.String("kp-1")})
	require.NoError(t, err)

	// Static IPs.
	_, err = client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames: []string{
			"host-a",
		},
		AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId:      aws.String("amazon_linux_2023"),
		BundleId:         aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.AllocateStaticIp(ctx, &lightsailsdk.AllocateStaticIpInput{StaticIpName: aws.String("ip-1")})
	require.NoError(t, err)

	_, err = client.AttachStaticIp(
		ctx,
		&lightsailsdk.AttachStaticIpInput{StaticIpName: aws.String("ip-1"), InstanceName: aws.String("host-a")},
	)
	require.NoError(t, err)

	ipOut, err := client.GetStaticIp(ctx, &lightsailsdk.GetStaticIpInput{StaticIpName: aws.String("ip-1")})
	require.NoError(t, err)
	require.True(t, aws.ToBool(ipOut.StaticIp.IsAttached))

	ipsOut, err := client.GetStaticIps(ctx, &lightsailsdk.GetStaticIpsInput{})
	require.NoError(t, err)
	require.Len(t, ipsOut.StaticIps, 1)

	_, err = client.DetachStaticIp(ctx, &lightsailsdk.DetachStaticIpInput{StaticIpName: aws.String("ip-1")})
	require.NoError(t, err)

	_, err = client.ReleaseStaticIp(ctx, &lightsailsdk.ReleaseStaticIpInput{StaticIpName: aws.String("ip-1")})
	require.NoError(t, err)
}

// TestDiskLifecycleRoundTrip exercises family I+J+K end to end: disks,
// disk snapshots, CopySnapshot, ExportSnapshot, and CreateCloudFormationStack.
func TestDiskLifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames: []string{
			"host-b",
		},
		AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId:      aws.String("amazon_linux_2023"),
		BundleId:         aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateDisk(ctx, &lightsailsdk.CreateDiskInput{
		DiskName: aws.String("disk-1"), AvailabilityZone: aws.String("us-east-1a"), SizeInGb: aws.Int32(16),
	})
	require.NoError(t, err)

	_, err = client.AttachDisk(ctx, &lightsailsdk.AttachDiskInput{
		DiskName: aws.String("disk-1"), InstanceName: aws.String("host-b"), DiskPath: aws.String("/dev/xvdf"),
	})
	require.NoError(t, err)

	diskOut, err := client.GetDisk(ctx, &lightsailsdk.GetDiskInput{DiskName: aws.String("disk-1")})
	require.NoError(t, err)
	require.Equal(t, lightsailtypes.DiskStateInUse, diskOut.Disk.State)

	_, err = client.DetachDisk(ctx, &lightsailsdk.DetachDiskInput{DiskName: aws.String("disk-1")})
	require.NoError(t, err)

	_, err = client.CreateDiskSnapshot(
		ctx,
		&lightsailsdk.CreateDiskSnapshotInput{
			DiskName:         aws.String("disk-1"),
			DiskSnapshotName: aws.String("disk-1-snap"),
		},
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		snap, snapErr := client.GetDiskSnapshot(
			ctx,
			&lightsailsdk.GetDiskSnapshotInput{DiskSnapshotName: aws.String("disk-1-snap")},
		)

		return snapErr == nil && snap.DiskSnapshot.State == lightsailtypes.DiskSnapshotStateCompleted
	}, defaultAsyncWait, defaultAsyncPoll, "disk snapshot never completed")

	_, err = client.CopySnapshot(ctx, &lightsailsdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("disk-1-snap"), TargetSnapshotName: aws.String("disk-1-snap-copy"),
		SourceRegion: lightsailtypes.RegionNameUsEast1,
	})
	require.NoError(t, err)

	copyOut, err := client.GetDiskSnapshots(ctx, &lightsailsdk.GetDiskSnapshotsInput{})
	require.NoError(t, err)
	require.Len(t, copyOut.DiskSnapshots, 2)

	_, err = client.DeleteDiskSnapshot(
		ctx,
		&lightsailsdk.DeleteDiskSnapshotInput{DiskSnapshotName: aws.String("disk-1-snap-copy")},
	)
	require.NoError(t, err)

	_, err = client.DeleteDisk(ctx, &lightsailsdk.DeleteDiskInput{DiskName: aws.String("disk-1")})
	require.NoError(t, err)

	// ExportSnapshot + CreateCloudFormationStack (family K) -- no wired
	// CloudFormation backend in this test, so the record is real but
	// unbacked (see exportcfn.go's doc comment).
	_, err = client.CreateInstanceSnapshot(ctx, &lightsailsdk.CreateInstanceSnapshotInput{
		InstanceName: aws.String("host-b"), InstanceSnapshotName: aws.String("host-b-snap"),
	})
	require.NoError(t, err)

	_, err = client.ExportSnapshot(
		ctx,
		&lightsailsdk.ExportSnapshotInput{SourceSnapshotName: aws.String("host-b-snap")},
	)
	require.NoError(t, err)

	recordsOut, err := client.GetExportSnapshotRecords(ctx, &lightsailsdk.GetExportSnapshotRecordsInput{})
	require.NoError(t, err)
	require.Len(t, recordsOut.ExportSnapshotRecords, 1)

	recordName := aws.ToString(recordsOut.ExportSnapshotRecords[0].Name)

	_, err = client.CreateCloudFormationStack(ctx, &lightsailsdk.CreateCloudFormationStackInput{
		Instances: []lightsailtypes.InstanceEntry{
			{
				SourceName:       aws.String(recordName),
				AvailabilityZone: aws.String("us-east-1a"),
				InstanceType:     aws.String("nano"),
				PortInfoSource:   lightsailtypes.PortInfoSourceTypeClosed,
			},
		},
	})
	require.NoError(t, err)

	stacksOut, err := client.GetCloudFormationStackRecords(ctx, &lightsailsdk.GetCloudFormationStackRecordsInput{})
	require.NoError(t, err)
	require.Len(t, stacksOut.CloudFormationStackRecords, 1)
}

// TestBucketRoundTrip exercises family S end to end.
func TestBucketRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateBucket(
		ctx,
		&lightsailsdk.CreateBucketInput{BucketName: aws.String("my-bucket"), BundleId: aws.String("small_1_0")},
	)
	require.NoError(t, err)

	getOut, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{BucketName: aws.String("my-bucket")})
	require.NoError(t, err)
	require.Len(t, getOut.Buckets, 1)
	require.Equal(t, "OK", aws.ToString(getOut.Buckets[0].State.Code))

	keyOut, err := client.CreateBucketAccessKey(
		ctx,
		&lightsailsdk.CreateBucketAccessKeyInput{BucketName: aws.String("my-bucket")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(keyOut.AccessKey.SecretAccessKey))

	keysOut, err := client.GetBucketAccessKeys(
		ctx,
		&lightsailsdk.GetBucketAccessKeysInput{BucketName: aws.String("my-bucket")},
	)
	require.NoError(t, err)
	require.Len(t, keysOut.AccessKeys, 1)
	require.Empty(t, aws.ToString(keysOut.AccessKeys[0].SecretAccessKey))

	_, err = client.DeleteBucketAccessKey(ctx, &lightsailsdk.DeleteBucketAccessKeyInput{
		BucketName: aws.String("my-bucket"), AccessKeyId: keyOut.AccessKey.AccessKeyId,
	})
	require.NoError(t, err)

	_, err = client.UpdateBucketBundle(
		ctx,
		&lightsailsdk.UpdateBucketBundleInput{BucketName: aws.String("my-bucket"), BundleId: aws.String("medium_1_0")},
	)
	require.NoError(t, err)

	_, err = client.DeleteBucket(ctx, &lightsailsdk.DeleteBucketInput{BucketName: aws.String("my-bucket")})
	require.NoError(t, err)
}
