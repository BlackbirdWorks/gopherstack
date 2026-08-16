package cloudwatchlogs_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_PutSubscriptionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		setup          func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name           string
		group          string
		filterName     string
		filterPattern  string
		destinationArn string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
			},
			group:          "grp",
			filterName:     "my-filter",
			filterPattern:  "",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		},
		{
			name: "update_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:old",
					"",
					"",
				)
			},
			group:          "grp",
			filterName:     "f",
			filterPattern:  "ERROR",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:new",
		},
		{
			name:           "group_not_found",
			group:          "nonexistent",
			filterName:     "f",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:fn",
			wantErr:        cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "limit_exceeded",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f1",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f2",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:b",
					"",
					"",
				)
			},
			group:          "grp",
			filterName:     "f3",
			destinationArn: "arn:aws:lambda:us-east-1:123456789012:function:c",
			wantErr:        cloudwatchlogs.ErrSubscriptionFilterLimitExceed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.PutSubscriptionFilter(
				context.Background(),
				tt.group,
				tt.filterName,
				tt.filterPattern,
				tt.destinationArn,
				"",
				"",
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, err := b.DescribeSubscriptionFilters(
				context.Background(),
				tt.group,
				"",
				"",
				0,
			)
			require.NoError(t, err)

			found := false
			for _, f := range filters {
				if f.FilterName == tt.filterName {
					found = true
					assert.Equal(t, tt.destinationArn, f.DestinationArn)
					assert.Equal(t, tt.filterPattern, f.FilterPattern)
				}
			}
			assert.True(t, found, "filter not found after put")
		})
	}
}

func TestCloudWatchLogsBackend_DescribeSubscriptionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		setup         func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name          string
		group         string
		prefix        string
		nextToken     string
		wantFirstName string
		wantCount     int
		limit         int
	}{
		{
			name: "all_filters",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"filter-a",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"filter-b",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:b",
					"",
					"",
				)
			},
			group:     "grp",
			wantCount: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"prod-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"", "",
				)
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"dev-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:b",
					"",
					"",
				)
			},
			group:         "grp",
			prefix:        "prod",
			wantCount:     1,
			wantFirstName: "prod-filter",
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name: "beyond_end",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"f",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
			},
			group:     "grp",
			nextToken: "999",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			filters, _, err := b.DescribeSubscriptionFilters(
				context.Background(),
				tt.group,
				tt.prefix,
				tt.nextToken,
				tt.limit,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, filters, tt.wantCount)

			if tt.wantFirstName != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstName, filters[0].FilterName)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DeleteSubscriptionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name       string
		group      string
		filterName string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"grp",
					"my-filter",
					"",
					"arn:aws:lambda:us-east-1:123456789012:function:a",
					"",
					"",
				)
			},
			group:      "grp",
			filterName: "my-filter",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
			},
			group:      "grp",
			filterName: "nonexistent",
			wantErr:    cloudwatchlogs.ErrSubscriptionFilterNotFound,
		},
		{
			name:       "group_not_found",
			group:      "nonexistent",
			filterName: "f",
			wantErr:    cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteSubscriptionFilter(context.Background(), tt.group, tt.filterName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, ferr := b.DescribeSubscriptionFilters(
				context.Background(),
				tt.group,
				"",
				"",
				0,
			)
			require.NoError(t, ferr)
			assert.Empty(t, filters)
		})
	}
}

func TestCloudWatchLogsBackend_PutLogEvents_SubscriptionDelivery(t *testing.T) {
	t.Parallel()

	type deliveredPayload struct {
		destinationArn string
		payload        []byte
	}

	var delivered []deliveredPayload

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
		func(_ context.Context, dst string, p []byte) error {
			delivered = append(delivered, deliveredPayload{dst, p})

			return nil
		},
	)

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"my-filter",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:target",
		"",
		"",
	)

	now := time.Now().UnixMilli()
	_, err := b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "hello", Timestamp: now},
		},
	)
	require.NoError(t, err)

	// Wait for the delivery goroutine to finish before asserting.
	b.Drain()

	assert.Len(t, delivered, 1)
	assert.Equal(
		t,
		"arn:aws:lambda:us-east-1:123456789012:function:target",
		delivered[0].destinationArn,
	)
	assert.NotEmpty(t, delivered[0].payload)
}

func TestCloudWatchLogsBackend_PutLogEvents_BoundedWorkerPool(t *testing.T) {
	t.Parallel()

	const (
		numEvents  = 20
		workersCap = 4
	)

	// concurrencyHigh tracks the highest observed concurrent delivery count.
	var mu sync.Mutex
	var inFlight, concurrencyHigh int

	ready := make(chan struct{})

	// reachedCap is closed once workersCap goroutines are simultaneously in the deliverer.
	var atCap sync.Once
	reachedCap := make(chan struct{})

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
		func(ctx context.Context, _ string, _ []byte) error {
			mu.Lock()
			inFlight++
			if inFlight > concurrencyHigh {
				concurrencyHigh = inFlight
			}
			if inFlight >= workersCap {
				atCap.Do(func() { close(reachedCap) })
			}
			mu.Unlock()

			// Hold until the test signals all goroutines to proceed.
			select {
			case <-ready:
			case <-ctx.Done():
			}

			mu.Lock()
			inFlight--
			mu.Unlock()

			return nil
		},
	)

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Limit to workersCap concurrent workers so we can verify the cap is respected.
	b.SetDeliveryWorkers(workersCap)
	b.SetDeliveryTimeout(0) // disable timeout so the hold above doesn't race
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"f",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:fn",
		"",
		"",
	)

	for i := range numEvents {
		_, err := b.PutLogEvents(
			context.Background(),
			"grp",
			"stream",
			"",
			[]cloudwatchlogs.InputLogEvent{
				{Message: fmt.Sprintf("msg-%d", i), Timestamp: int64(i)},
			},
		)
		require.NoError(t, err)
	}

	// Wait until the semaphore is full before inspecting peak concurrency.
	<-reachedCap

	mu.Lock()
	peak := concurrencyHigh
	mu.Unlock()

	// The peak concurrency must not exceed the configured worker cap.
	assert.LessOrEqual(t, peak, workersCap)

	close(ready)
	b.Drain()
}

func TestCloudWatchLogsBackend_PutLogEvents_SubscriptionDelivery_PerDeliveryTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		timeout              time.Duration
		wantFastCtxCancelled bool
	}{
		{
			name:                 "fresh_timeout_per_delivery",
			timeout:              20 * time.Millisecond,
			wantFastCtxCancelled: false,
		},
		{
			name:                 "timeout_disabled",
			timeout:              0,
			wantFastCtxCancelled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const (
				slowDestination = "arn:aws:lambda:us-east-1:123456789012:function:slow"
				fastDestination = "arn:aws:lambda:us-east-1:123456789012:function:fast"
			)

			var (
				mu               sync.Mutex
				fastCtxCancelled bool
			)
			fastCalled := make(chan struct{}, 1)

			deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
				func(ctx context.Context, dst string, _ []byte) error {
					switch dst {
					case slowDestination:
						if tt.timeout <= 0 {
							return nil
						}
						<-ctx.Done()

						return ctx.Err()
					case fastDestination:
						mu.Lock()
						fastCtxCancelled = ctx.Err() != nil
						mu.Unlock()
						select {
						case fastCalled <- struct{}{}:
						default:
						}
					}

					return nil
				},
			)

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			b.SetDeliveryTimeout(tt.timeout)
			b.SetSubscriptionDeliverer(deliverer)

			_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
			_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
			_ = b.PutSubscriptionFilter(
				context.Background(),
				"grp",
				"slow-filter",
				"",
				slowDestination,
				"",
				"",
			)
			_ = b.PutSubscriptionFilter(
				context.Background(),
				"grp",
				"fast-filter",
				"",
				fastDestination,
				"",
				"",
			)

			_, err := b.PutLogEvents(
				context.Background(),
				"grp",
				"stream",
				"",
				[]cloudwatchlogs.InputLogEvent{
					{Message: "hello", Timestamp: 1},
				},
			)
			require.NoError(t, err)

			b.Drain()

			select {
			case <-fastCalled:
			default:
				require.FailNow(t, "expected fast destination delivery call")
			}

			mu.Lock()
			assert.Equal(t, tt.wantFastCtxCancelled, fastCtxCancelled)
			mu.Unlock()
		})
	}
}

func TestCloudWatchLogsBackend_Close_CancelsInFlightDeliveries(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	deliveryCancelled := make(chan struct{}, 1)

	deliverer := cloudwatchlogs.SubscriptionDelivererFunc(
		func(ctx context.Context, _ string, _ []byte) error {
			// Signal that the delivery goroutine has started and is in progress.
			close(started)
			// Block until the context is cancelled.
			<-ctx.Done()
			select {
			case deliveryCancelled <- struct{}{}:
			default:
			}

			return ctx.Err()
		},
	)

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetDeliveryTimeout(0) // disable timeout so Close() is the only cancellation source
	b.SetSubscriptionDeliverer(deliverer)

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_, _ = b.CreateLogStream(context.Background(), "grp", "stream")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"f",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:fn",
		"",
		"",
	)

	_, err := b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "hello", Timestamp: 1},
		},
	)
	require.NoError(t, err)

	// Wait until the goroutine has started and is blocking inside the deliverer before closing.
	<-started

	// Close cancels the lifecycle context and waits for the goroutine to exit.
	b.Close()

	select {
	case <-deliveryCancelled:
		// goroutine observed context cancellation — expected
	default:
		require.FailNow(t, "expected in-flight delivery to be cancelled by Close()")
	}
}

func TestCloudWatchLogsBackend_DeleteLogGroup_ClearsSubscriptionFilters(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	_ = b.PutSubscriptionFilter(
		context.Background(),
		"grp",
		"f",
		"",
		"arn:aws:lambda:us-east-1:123456789012:function:a",
		"",
		"",
	)
	require.NoError(t, b.DeleteLogGroup(context.Background(), "grp"))

	_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
	filters, _, err := b.DescribeSubscriptionFilters(context.Background(), "grp", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, filters)
}

func TestCloudWatchLogsBackend_PutSubscriptionFilter_RoleArnAndDistribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr          error
		name             string
		roleArn          string
		distribution     string
		wantDistribution string
		wantRoleArn      string
	}{
		{
			name:             "default_distribution_is_random",
			wantDistribution: cloudwatchlogs.DistributionRandom,
		},
		{
			name:             "explicit_random",
			distribution:     cloudwatchlogs.DistributionRandom,
			wantDistribution: cloudwatchlogs.DistributionRandom,
		},
		{
			name:             "by_log_stream",
			distribution:     cloudwatchlogs.DistributionByLogStream,
			wantDistribution: cloudwatchlogs.DistributionByLogStream,
		},
		{
			name:         "invalid_distribution",
			distribution: "INVALID",
			wantErr:      cloudwatchlogs.ErrValidation,
		},
		{
			name:             "with_role_arn",
			roleArn:          "arn:aws:iam::123456789012:role/MyRole",
			wantRoleArn:      "arn:aws:iam::123456789012:role/MyRole",
			wantDistribution: cloudwatchlogs.DistributionRandom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			err = b.PutSubscriptionFilter(
				context.Background(),
				"g", "f1", "", "arn:aws:kinesis:us-east-1:123:stream/s",
				tt.roleArn, tt.distribution,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			filters, _, err := b.DescribeSubscriptionFilters(context.Background(), "g", "", "", 50)
			require.NoError(t, err)
			require.Len(t, filters, 1)

			f := filters[0]
			assert.Equal(t, tt.wantDistribution, f.Distribution)
			assert.Equal(t, tt.wantRoleArn, f.RoleArn)
		})
	}
}

func TestCloudWatchLogsBackend_PutSubscriptionFilter_UpdatePreservesFields(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	// Create with role and distribution.
	err = b.PutSubscriptionFilter(
		context.Background(),
		"g", "f1", "ERROR", "arn:aws:kinesis:us-east-1:123:stream/s",
		"arn:aws:iam::123:role/r", cloudwatchlogs.DistributionByLogStream,
	)
	require.NoError(t, err)

	// Update with new pattern.
	err = b.PutSubscriptionFilter(
		context.Background(),
		"g", "f1", "WARN", "arn:aws:kinesis:us-east-1:123:stream/s",
		"arn:aws:iam::123:role/r2", cloudwatchlogs.DistributionRandom,
	)
	require.NoError(t, err)

	filters, _, err := b.DescribeSubscriptionFilters(context.Background(), "g", "", "", 50)
	require.NoError(t, err)
	require.Len(t, filters, 1)

	f := filters[0]
	assert.Equal(t, "WARN", f.FilterPattern)
	assert.Equal(t, "arn:aws:iam::123:role/r2", f.RoleArn)
	assert.Equal(t, cloudwatchlogs.DistributionRandom, f.Distribution)
}
