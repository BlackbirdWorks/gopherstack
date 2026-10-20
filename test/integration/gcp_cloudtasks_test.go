package integration_test

import (
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newCloudTasksClient(t *testing.T) *cloudtasks.Client {
	t.Helper()

	client, err := cloudtasks.NewClient(t.Context(), gcpGRPCOpts(t, "8123")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_CloudTasks_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newCloudTasksClient(t)
	ctx := t.Context()
	location := "projects/" + gcpProject + "/locations/us-central1"

	queue, err := client.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
		Parent: location, Queue: &cloudtaskspb.Queue{Name: location + "/queues/" + gcpName("it-q")},
	})
	require.NoError(t, err)
	assert.Equal(t, cloudtaskspb.Queue_RUNNING, queue.GetState())

	paused, err := client.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: queue.GetName()})
	require.NoError(t, err)
	assert.Equal(t, cloudtaskspb.Queue_PAUSED, paused.GetState())

	task, err := client.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &cloudtaskspb.Task{
			ScheduleTime: timestamppb.New(time.Now().Add(24 * time.Hour)),
			MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
				Url: "http://127.0.0.1:9/work", Body: []byte("job"),
			}},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, task.GetName(), queue.GetName()+"/tasks/")

	got, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
		Name: task.GetName(), ResponseView: cloudtaskspb.Task_FULL,
	})
	require.NoError(t, err)
	assert.Equal(t, "job", string(got.GetHttpRequest().GetBody()))

	tasks := 0
	it := client.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: queue.GetName()})

	for {
		if _, nerr := it.Next(); nerr != nil {
			break
		}

		tasks++
	}

	assert.Equal(t, 1, tasks)

	require.NoError(t, client.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{Name: task.GetName()}))
	require.NoError(t, client.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: queue.GetName()}))
}

func TestIntegration_GCP_CloudTasks_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newCloudTasksClient(t)

	_, err := client.GetQueue(t.Context(), &cloudtaskspb.GetQueueRequest{
		Name: "projects/" + gcpProject + "/locations/us-central1/queues/" + gcpName("absent"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
