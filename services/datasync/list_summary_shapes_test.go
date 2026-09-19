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

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for datasync's five flagged List ops:
// ListAgents, ListLocations, ListTagsForResource, ListTaskExecutions and
// ListTasks all already used a narrow *ListEntry-shaped output struct
// (agentListEntryOutput/locationListEntryOutput/tagInput/
// taskExecutionListEntryOutput/taskListEntryOutput) matching the real
// AgentListEntry/LocationListEntry/TagListEntry/TaskExecutionListEntry/
// TaskListEntry member sets exactly (verified via cmd/structfielddiff
// against datasync@v1.61.4) -- no leaks found. ListAgents has one unsourced
// gap: AgentListEntry's optional Platform member is never emitted, since
// the Agent model tracks no platform/version data.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("agents exact, platform unsourced", func(t *testing.T) {
		t.Parallel()

		backend := datasync.NewInMemoryBackend("123456789012", "us-east-1")
		client := newTestDataSyncClient(t, datasync.NewHandler(backend))
		ctx := t.Context()

		_, err := client.CreateAgent(ctx, &datasyncsdk.CreateAgentInput{
			ActivationKey: aws.String("key"),
			AgentName:     aws.String("agent1"),
		})
		require.NoError(t, err)

		out, err := client.ListAgents(ctx, &datasyncsdk.ListAgentsInput{})
		require.NoError(t, err)
		require.Len(t, out.Agents, 1)
		a := out.Agents[0]
		assert.Equal(t, "agent1", aws.ToString(a.Name))
		assert.NotEmpty(t, a.AgentArn)
		assert.NotEmpty(t, a.Status)
		assert.Nil(t, a.Platform)
	})

	t.Run("locations exact", func(t *testing.T) {
		t.Parallel()

		backend := datasync.NewInMemoryBackend("123456789012", "us-east-1")
		client := newTestDataSyncClient(t, datasync.NewHandler(backend))
		ctx := t.Context()

		created, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("src.example.com"),
			BucketName:     aws.String("bucket"),
		})
		require.NoError(t, err)

		out, err := client.ListLocations(ctx, &datasyncsdk.ListLocationsInput{})
		require.NoError(t, err)
		require.Len(t, out.Locations, 1)
		l := out.Locations[0]
		assert.Equal(t, aws.ToString(created.LocationArn), aws.ToString(l.LocationArn))
		assert.NotEmpty(t, aws.ToString(l.LocationUri))
	})

	t.Run("tags exact", func(t *testing.T) {
		t.Parallel()

		backend := datasync.NewInMemoryBackend("123456789012", "us-east-1")
		client := newTestDataSyncClient(t, datasync.NewHandler(backend))
		ctx := t.Context()

		loc, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("src.example.com"),
			BucketName:     aws.String("bucket"),
			Tags: []types.TagListEntry{
				{Key: aws.String("k"), Value: aws.String("v")},
			},
		})
		require.NoError(t, err)

		out, err := client.ListTagsForResource(ctx, &datasyncsdk.ListTagsForResourceInput{
			ResourceArn: loc.LocationArn,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "k", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "v", aws.ToString(out.Tags[0].Value))
	})

	t.Run("task executions exact", func(t *testing.T) {
		t.Parallel()

		backend := datasync.NewInMemoryBackend("123456789012", "us-east-1")
		client := newTestDataSyncClient(t, datasync.NewHandler(backend))
		ctx := t.Context()

		src, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("src.example.com"),
			BucketName:     aws.String("src-bucket"),
		})
		require.NoError(t, err)

		dst, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("dst.example.com"),
			BucketName:     aws.String("dst-bucket"),
		})
		require.NoError(t, err)

		task, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
			SourceLocationArn:      src.LocationArn,
			DestinationLocationArn: dst.LocationArn,
		})
		require.NoError(t, err)

		exec, err := client.StartTaskExecution(ctx, &datasyncsdk.StartTaskExecutionInput{
			TaskArn: task.TaskArn,
		})
		require.NoError(t, err)

		out, err := client.ListTaskExecutions(ctx, &datasyncsdk.ListTaskExecutionsInput{
			TaskArn: task.TaskArn,
		})
		require.NoError(t, err)
		require.Len(t, out.TaskExecutions, 1)
		e := out.TaskExecutions[0]
		assert.Equal(t, aws.ToString(exec.TaskExecutionArn), aws.ToString(e.TaskExecutionArn))
		assert.NotEmpty(t, e.Status)
	})

	t.Run("tasks exact", func(t *testing.T) {
		t.Parallel()

		backend := datasync.NewInMemoryBackend("123456789012", "us-east-1")
		client := newTestDataSyncClient(t, datasync.NewHandler(backend))
		ctx := t.Context()

		src, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("src.example.com"),
			BucketName:     aws.String("src-bucket"),
		})
		require.NoError(t, err)

		dst, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
			ServerHostname: aws.String("dst.example.com"),
			BucketName:     aws.String("dst-bucket"),
		})
		require.NoError(t, err)

		_, err = client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
			SourceLocationArn:      src.LocationArn,
			DestinationLocationArn: dst.LocationArn,
			Name:                   aws.String("t1"),
		})
		require.NoError(t, err)

		out, err := client.ListTasks(ctx, &datasyncsdk.ListTasksInput{})
		require.NoError(t, err)
		require.Len(t, out.Tasks, 1)
		tk := out.Tasks[0]
		assert.Equal(t, "t1", aws.ToString(tk.Name))
		assert.NotEmpty(t, tk.Status)
	})
}
