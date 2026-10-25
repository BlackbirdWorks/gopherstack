package kinesis_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func limitedClient(t *testing.T, shardLimit, onDemandLimit int) *kinesissdk.Client {
	t.Helper()

	b := kinesis.NewInMemoryBackend()
	b.ConfigureLimits(shardLimit, onDemandLimit)

	return newTestKinesisClient(t, newTestHandlerWithBackend(t, b))
}

func mustCreate(t *testing.T, c *kinesissdk.Client, name string, shards int32) error {
	t.Helper()

	_, err := c.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(name), ShardCount: aws.Int32(shards),
	})

	return err
}

func TestShardLimit_CreateStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		creates   []int32
		limit     int
		wantLimit bool
	}{
		{name: "within limit", limit: 4, creates: []int32{2, 2}},
		{name: "exceeds limit", limit: 4, creates: []int32{2, 3}, wantLimit: true},
		{name: "unlimited", limit: -1, creates: []int32{100, 100, 100, 100, 100, 100}},
		{name: "default is 500", limit: 0, creates: []int32{100, 100, 100, 100, 100, 1}, wantLimit: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := limitedClient(t, tt.limit, 0)

			var err error

			for i, n := range tt.creates {
				err = mustCreate(t, c, "s"+string(rune('a'+i)), n)
			}

			if tt.wantLimit {
				var le *types.LimitExceededException
				require.ErrorAs(t, err, &le)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestShardLimit_UpdateShardCountAndDescribeLimits(t *testing.T) {
	t.Parallel()

	c := limitedClient(t, 5, 0)
	require.NoError(t, mustCreate(t, c, "a", 2))
	require.NoError(t, mustCreate(t, c, "b", 2))

	lim, err := c.DescribeLimits(t.Context(), &kinesissdk.DescribeLimitsInput{})
	require.NoError(t, err)
	assert.EqualValues(t, 5, aws.ToInt32(lim.ShardLimit))
	assert.EqualValues(t, 4, aws.ToInt32(lim.OpenShardCount))

	waitActive(t, c, "a")

	_, err = c.UpdateShardCount(t.Context(), &kinesissdk.UpdateShardCountInput{
		StreamName: aws.String("a"), TargetShardCount: aws.Int32(4),
		ScalingType: types.ScalingTypeUniformScaling,
	})
	var le *types.LimitExceededException
	require.ErrorAs(t, err, &le)
}

func waitActive(t *testing.T, c *kinesissdk.Client, name string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := c.DescribeStreamSummary(t.Context(),
			&kinesissdk.DescribeStreamSummaryInput{StreamName: aws.String(name)})

		return err == nil && out.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive
	}, 5*time.Second, 10*time.Millisecond)
}

func TestOnDemandStreamLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		limit     int
		wantLimit bool
		reported  int32
	}{
		{name: "configured limit enforced", limit: 2, wantLimit: true, reported: 2},
		{name: "unlimited", limit: -1, reported: 2147483647},
		{name: "aws default", limit: 0, reported: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := limitedClient(t, 0, tt.limit)

			var err error

			for _, n := range []string{"a", "b", "c"} {
				_, err = c.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
					StreamName:        aws.String(n),
					StreamModeDetails: &types.StreamModeDetails{StreamMode: types.StreamModeOnDemand},
				})
				if err != nil {
					break
				}
			}

			if tt.wantLimit {
				var le *types.LimitExceededException
				require.ErrorAs(t, err, &le)
			} else {
				require.NoError(t, err)
			}

			lim, err := c.DescribeLimits(t.Context(), &kinesissdk.DescribeLimitsInput{})
			require.NoError(t, err)
			assert.Equal(t, tt.reported, aws.ToInt32(lim.OnDemandStreamCountLimit))
		})
	}
}

func TestLatency_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		latency time.Duration
		calls   int
	}{
		{name: "disabled", latency: 0, calls: 0},
		{name: "applied per request", latency: 500 * time.Millisecond, calls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu    sync.Mutex
				slept []time.Duration
			)

			h := newTestHandler(t).WithLatency(tt.latency, func(_ context.Context, d time.Duration) error {
				mu.Lock()
				defer mu.Unlock()

				slept = append(slept, d)

				return nil
			})

			c := newTestKinesisClient(t, h)
			_, err := c.ListStreams(t.Context(), &kinesissdk.ListStreamsInput{})
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()

			assert.Len(t, slept, tt.calls)

			for _, d := range slept {
				assert.Equal(t, tt.latency, d)
			}
		})
	}
}

func TestLatency_CancelledDuringDelay(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newTestHandler(t).WithLatency(500*time.Millisecond, nil)

		ctx, cancel := context.WithCancel(t.Context())
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader("{}")).
			WithContext(ctx)
		req.Header.Set("X-Amz-Target", "Kinesis_20131202.ListStreams")
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")

		rec := httptest.NewRecorder()
		done := make(chan struct{})

		go func() {
			defer close(done)

			_ = h.Handler()(echo.New().NewContext(req, rec))
		}()

		synctest.Wait()

		select {
		case <-done:
			t.Fatal("handler returned before the latency elapsed")
		default:
		}

		cancel()
		synctest.Wait()
		<-done

		assert.Zero(t, rec.Body.Len())
	})
}
