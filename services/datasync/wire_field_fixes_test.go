package datasync_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// newTestDataSyncClient stands up the real aws-sdk-go-v2 DataSync client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestDataSyncClient(t *testing.T, h *datasync.Handler) *datasyncsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return datasyncsdk.NewFromConfig(cfg, func(o *datasyncsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestTaskMode_RoundTrips_ThroughListAndDescribe_RealClient covers a
// layer-3 bug (gopherstack-g8k9): Task.TaskMode is real, tracked state --
// CreateTask stores it and DescribeTask already emits it correctly (the
// second-op signal) -- but ListTasks' TaskListEntry never carried it
// through, and the identical gap existed one level over for task
// executions: StartTaskExecution/DescribeTaskExecution/ListTaskExecutions
// never captured or emitted TaskMode at all despite the parent task's mode
// being known at execution-start time. Real fields confirmed against
// datasync@v1.61.4 deserializers.go: awsAwsjson11_deserializeDocumentTaskListEntry
// and awsAwsjson11_deserializeDocumentTaskExecutionListEntry both have a
// "TaskMode" case, as does awsAwsjson11_deserializeOpDocumentDescribeTaskExecutionOutput.
func TestTaskMode_RoundTrips_ThroughListAndDescribe_RealClient(t *testing.T) {
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

	createdTask, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
		SourceLocationArn:      src.LocationArn,
		DestinationLocationArn: dst.LocationArn,
		Name:                   aws.String("enhanced-task"),
		TaskMode:               types.TaskModeEnhanced,
	})
	require.NoError(t, err)

	listed, err := client.ListTasks(ctx, &datasyncsdk.ListTasksInput{})
	require.NoError(t, err)
	require.Len(t, listed.Tasks, 1)
	assert.Equal(t, types.TaskModeEnhanced, listed.Tasks[0].TaskMode,
		"ListTasks: TaskMode must round-trip; pre-fix it was always empty")

	started, err := client.StartTaskExecution(ctx, &datasyncsdk.StartTaskExecutionInput{
		TaskArn: createdTask.TaskArn,
	})
	require.NoError(t, err)

	described, err := client.DescribeTaskExecution(ctx, &datasyncsdk.DescribeTaskExecutionInput{
		TaskExecutionArn: started.TaskExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TaskModeEnhanced, described.TaskMode,
		"DescribeTaskExecution: TaskMode must round-trip; pre-fix it was always empty")

	listedExecs, err := client.ListTaskExecutions(ctx, &datasyncsdk.ListTaskExecutionsInput{
		TaskArn: createdTask.TaskArn,
	})
	require.NoError(t, err)
	require.Len(t, listedExecs.TaskExecutions, 1)
	assert.Equal(t, types.TaskModeEnhanced, listedExecs.TaskExecutions[0].TaskMode,
		"ListTaskExecutions: TaskMode must round-trip; pre-fix it was always empty")
}
