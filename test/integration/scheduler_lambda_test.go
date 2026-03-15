package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulersdktypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"log/slog"

	"net/http/httptest"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	schedulerpkg "github.com/blackbirdworks/gopherstack/services/scheduler"
)

const (
	// schedulerPollTimeout is the max time to wait for a schedule to fire.
	schedulerPollTimeout = 5 * time.Second
	// schedulerPollInterval is how often to check for Lambda invocations.
	schedulerPollInterval = 50 * time.Millisecond
)

// mockSchedulerLambdaInvoker records Lambda invocations from the Scheduler runner.
type mockSchedulerLambdaInvoker struct {
	calls []string
	mu    sync.Mutex
}

func (m *mockSchedulerLambdaInvoker) InvokeFunction(_ context.Context, name, _ string, _ []byte) ([]byte, int, error) {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	m.mu.Unlock()

	return nil, 200, nil
}

func (m *mockSchedulerLambdaInvoker) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.calls)
}

// TestIntegration_Scheduler_Lambda_Target verifies that a rate-based schedule fires and
// invokes its Lambda target via the Scheduler Runner.
//
// This test uses in-process backends and a mock Lambda invoker (no Docker required).
//
//nolint:paralleltest // in-process test server; avoids port conflicts with parallel tests
func TestIntegration_Scheduler_Lambda_Target(t *testing.T) {
	ctx := t.Context()

	// --- Build backends ---
	schedulerBackend := schedulerpkg.NewInMemoryBackend("000000000000", "us-east-1")
	schedulerHandler := schedulerpkg.NewHandler(schedulerBackend)

	// Wire mock Lambda invoker into the runner.
	mockInvoker := &mockSchedulerLambdaInvoker{}
	schedulerHandler.GetRunner().SetLambdaInvoker(mockInvoker)

	// --- Set up in-process HTTP server ---
	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(schedulerHandler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	// --- Build AWS SDK client ---
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	schedClient := schedulersdk.NewFromConfig(awsCfg, func(o *schedulersdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})

	// --- Step 1: Create a rate(1 second) schedule ---
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-scheduled-fn"

	_, err = schedClient.CreateSchedule(ctx, &schedulersdk.CreateScheduleInput{
		Name:               aws.String("test-rate-schedule"),
		ScheduleExpression: aws.String("rate(1 second)"),
		State:              schedulersdktypes.ScheduleStateEnabled,
		Target: &schedulersdktypes.Target{
			Arn:     aws.String(lambdaARN),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		},
		FlexibleTimeWindow: &schedulersdktypes.FlexibleTimeWindow{
			Mode: schedulersdktypes.FlexibleTimeWindowModeOff,
		},
	})
	require.NoError(t, err)

	// --- Step 2: Start the runner ---
	runCtx, runCancel := context.WithCancel(ctx)
	t.Cleanup(runCancel)
	schedulerHandler.GetRunner().Start(runCtx)

	// --- Step 3: Wait for the schedule to fire at least once ---
	deadline := time.Now().Add(schedulerPollTimeout)

	for time.Now().Before(deadline) {
		if mockInvoker.CallCount() >= 1 {
			break
		}

		time.Sleep(schedulerPollInterval)
	}

	assert.GreaterOrEqual(t, mockInvoker.CallCount(), 1, "Lambda should be invoked by the scheduler")
}
