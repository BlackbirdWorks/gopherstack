package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeVolumes_SDKRoundTrip_StaleCursorResumesPastDeletedItem drives
// DescribeVolumes through the real aws-sdk-go-v2/service/fsx client to prove
// the fsx.paginate fix (services/fsx/store.go): a NextToken naming a volume
// deleted between calls must resume after that volume's sort position, not
// silently reset to page one and re-return an item the caller already saw.
func TestDescribeVolumes_SDKRoundTrip_StaleCursorResumesPastDeletedItem(t *testing.T) {
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

	svmOut, err := client.CreateStorageVirtualMachine(t.Context(), &fsxsdk.CreateStorageVirtualMachineInput{
		FileSystemId: fsOut.FileSystem.FileSystemId,
		Name:         aws.String("svm1"),
	})
	require.NoError(t, err)

	for i := range 3 {
		_, cErr := client.CreateVolume(t.Context(), &fsxsdk.CreateVolumeInput{
			VolumeType: types.VolumeTypeOntap,
			Name:       aws.String("vol" + string(rune('a'+i))),
			OntapConfiguration: &types.CreateOntapVolumeConfiguration{
				StorageVirtualMachineId: svmOut.StorageVirtualMachine.StorageVirtualMachineId,
			},
		})
		require.NoError(t, cErr)
	}

	// Page 1: one item at a time, so page1's NextToken names the second
	// item in VolumeId-sorted order.
	page1, err := client.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Volumes, 1)
	require.NotNil(t, page1.NextToken)

	firstSeenID := aws.ToString(page1.Volumes[0].VolumeId)
	staleToken := aws.ToString(page1.NextToken)

	// Delete the volume the cursor points at before the next page is
	// fetched -- exactly the retention-sweep/deletion trigger described for
	// this bug class.
	_, err = client.DeleteVolume(t.Context(), &fsxsdk.DeleteVolumeInput{VolumeId: aws.String(staleToken)})
	require.NoError(t, err)

	page2, err := client.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{
		MaxResults: aws.Int32(10),
		NextToken:  aws.String(staleToken),
	})
	require.NoError(t, err)

	page2IDs := make([]string, 0, len(page2.Volumes))
	for _, v := range page2.Volumes {
		page2IDs = append(page2IDs, aws.ToString(v.VolumeId))
	}

	assert.NotContains(t, page2IDs, firstSeenID,
		"a stale cursor must not re-return page1's item -- that means pagination reset to page one")
	assert.NotContains(t, page2IDs, staleToken, "the deleted volume itself must not reappear")
	assert.Len(t, page2IDs, 1, "exactly one surviving volume remains after the deleted one")
}
