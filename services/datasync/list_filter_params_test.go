package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// ListLocations declares a Filters member (LocationFilter: Name/Operator/
// Values -- api_op_ListLocations.go, datasync@v1.61.4) that the handler must
// apply against real backend state (LocationType is tracked on every stored
// location) before pagination.
func TestListLocations_FilterByLocationType_RealClient(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestDataSyncClient(t, datasync.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("obj.example.com"),
		BucketName:     aws.String("obj-bucket"),
	})
	require.NoError(t, err)

	_, err = client.CreateLocationEfs(ctx, &datasyncsdk.CreateLocationEfsInput{
		Ec2Config: &types.Ec2Config{
			SubnetArn:         aws.String("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-1"),
			SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:000000000000:security-group/sg-1"},
		},
		EfsFilesystemArn: aws.String("arn:aws:elasticfilesystem:us-east-1:000000000000:file-system/fs-1"),
	})
	require.NoError(t, err)

	listed, err := client.ListLocations(ctx, &datasyncsdk.ListLocationsInput{
		Filters: []types.LocationFilter{
			{
				Name:     types.LocationFilterNameLocationType,
				Operator: types.OperatorEq,
				Values:   []string{"EFS"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, listed.Locations, 1, "Filters must narrow ListLocations to only EFS locations")
	require.Contains(t, aws.ToString(listed.Locations[0].LocationUri), "efs://")
}

// ListTasks declares a Filters member (TaskFilter: Name/Operator/Values --
// api_op_ListTasks.go, datasync@v1.61.4) with filter name LocationId that
// must match a task's source or destination location ARN.
func TestListTasks_FilterByLocationID_RealClient(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestDataSyncClient(t, datasync.NewHandler(backend))
	ctx := t.Context()

	srcA, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("a.example.com"),
		BucketName:     aws.String("a-bucket"),
	})
	require.NoError(t, err)

	dst, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("dst.example.com"),
		BucketName:     aws.String("dst-bucket"),
	})
	require.NoError(t, err)

	srcB, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
		ServerHostname: aws.String("b.example.com"),
		BucketName:     aws.String("b-bucket"),
	})
	require.NoError(t, err)

	wantTask, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
		SourceLocationArn:      srcA.LocationArn,
		DestinationLocationArn: dst.LocationArn,
		Name:                   aws.String("task-a"),
	})
	require.NoError(t, err)

	_, err = client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
		SourceLocationArn:      srcB.LocationArn,
		DestinationLocationArn: dst.LocationArn,
		Name:                   aws.String("task-b"),
	})
	require.NoError(t, err)

	listed, err := client.ListTasks(ctx, &datasyncsdk.ListTasksInput{
		Filters: []types.TaskFilter{
			{
				Name:     types.TaskFilterNameLocationId,
				Operator: types.OperatorEq,
				Values:   []string{aws.ToString(srcA.LocationArn)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, listed.Tasks, 1, "Filters must narrow ListTasks to only tasks touching the given location")
	require.Equal(t, aws.ToString(wantTask.TaskArn), aws.ToString(listed.Tasks[0].TaskArn))
}
