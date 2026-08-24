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

// TestNotFound_TypesAsInvalidRequestException_RealClient proves that every
// not-found condition in datasync decodes, through the real SDK client, as
// *types.InvalidRequestException -- never as a ResourceNotFoundException,
// which does not exist anywhere in this service. Confirmed against every one
// of datasync's 53 awsAwsjson11_deserializeOpError<Op> switches (aws-sdk-
// go-v2/service/datasync@v1.61.4 deserializers.go), which type only
// InternalException and InvalidRequestException, and against types/errors.go,
// which defines exactly those two exception structs and no
// ResourceNotFoundException/ResourceExistsException type at all. Before the
// fix, handler.go's handleError mapped ErrNotFound/ErrAlreadyExists to those
// two fabricated wire codes, which every op's own switch falls through to its
// default case for -- decoding as an untyped smithy.GenericAPIError instead
// of a real exception type, for every not-found path in the whole service.
func TestNotFound_TypesAsInvalidRequestException_RealClient(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	h := datasync.NewHandler(backend)
	client := newTestDataSyncClient(t, h)
	ctx := t.Context()

	_, err := client.DescribeTask(ctx, &datasyncsdk.DescribeTaskInput{
		TaskArn: aws.String("arn:aws:datasync:us-east-1:000000000000:task/notexist"),
	})
	require.Error(t, err)

	var invalidRequest *types.InvalidRequestException
	require.ErrorAs(t, err, &invalidRequest)
}
