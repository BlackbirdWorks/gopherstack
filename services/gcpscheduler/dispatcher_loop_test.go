package gcpscheduler_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func okResponse(r *http.Request, sc int) *http.Response {
	return &http.Response{StatusCode: sc, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: r}
}

func createLoopJob(t *testing.T, b *gcpscheduler.InMemoryBackend, mut func(*schedulerpb.Job)) *schedulerpb.Job {
	t.Helper()

	j := &schedulerpb.Job{
		Name: location + "/jobs/loop", Schedule: "* * * * *",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://target.test/x"}},
	}
	if mut != nil {
		mut(j)
	}

	out, err := b.CreateJob(&schedulerpb.CreateJobRequest{Parent: location, Job: j})
	require.NoError(t, err)

	return out
}

// TestDispatcherLoopFiresOnCron drives the real ticker loop on synctest's fake clock.
func TestDispatcherLoopFiresOnCron(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls []time.Time

		done := make(chan struct{})
		rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls = append(calls, time.Now())
			if len(calls) == 3 {
				close(done)
			}

			return okResponse(r, http.StatusOK), nil
		})

		b := gcpscheduler.NewInMemoryBackend()
		b.SetHTTPClient(&http.Client{Transport: rt})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b.SetRunContext(ctx)
		createLoopJob(t, b, nil)

		loopDone := make(chan struct{})
		go func() {
			defer close(loopDone)

			b.RunDispatcher(ctx, 500*time.Millisecond)
		}()

		<-done
		cancel()
		<-loopDone
		b.Drain()

		require.Len(t, calls, 3)

		for i := 1; i < len(calls); i++ {
			gap := calls[i].Sub(calls[i-1])
			assert.GreaterOrEqual(t, gap, time.Minute-time.Second)
			assert.LessOrEqual(t, gap, time.Minute+time.Second)
		}

		for _, c := range calls {
			assert.LessOrEqual(t, c.Sub(c.Truncate(time.Minute)), time.Second, "fires on the minute boundary")
		}
	})
}

// TestDispatcherLoopHonoursBackoff checks retry spacing through the real ticker loop.
func TestDispatcherLoopHonoursBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const tick = 100 * time.Millisecond

		var calls []time.Time

		done := make(chan struct{})
		rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls = append(calls, time.Now())

			sc := http.StatusInternalServerError
			if len(calls) == 3 {
				sc = http.StatusOK

				close(done)
			}

			return okResponse(r, sc), nil
		})

		b := gcpscheduler.NewInMemoryBackend()
		b.SetHTTPClient(&http.Client{Transport: rt})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b.SetRunContext(ctx)

		j := createLoopJob(t, b, func(j *schedulerpb.Job) {
			j.Schedule = "0 0 1 1 *"
			j.RetryConfig = &schedulerpb.RetryConfig{
				RetryCount:         5,
				MinBackoffDuration: durationpb.New(time.Second),
				MaxBackoffDuration: durationpb.New(time.Minute),
				MaxDoublings:       4,
			}
		})

		_, err := b.RunJob(j.GetName())
		require.NoError(t, err)

		loopDone := make(chan struct{})
		go func() {
			defer close(loopDone)

			b.RunDispatcher(ctx, tick)
		}()

		<-done
		cancel()
		<-loopDone
		b.Drain()

		require.Len(t, calls, 3)
		assert.GreaterOrEqual(t, calls[1].Sub(calls[0]), time.Second)
		assert.Less(t, calls[1].Sub(calls[0]), time.Second+2*tick)
		assert.GreaterOrEqual(t, calls[2].Sub(calls[1]), 2*time.Second)
		assert.Less(t, calls[2].Sub(calls[1]), 2*time.Second+2*tick)
	})
}

// TestAttemptDeadlineAbortsHungTarget shows attempt_deadline cancelling a request that never answers.
func TestAttemptDeadlineAbortsHungTarget(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()

		var elapsed time.Duration

		rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()

			elapsed = time.Since(start)

			return nil, r.Context().Err()
		})

		b := gcpscheduler.NewInMemoryBackend()
		b.SetHTTPClient(&http.Client{Transport: rt})

		j := createLoopJob(t, b, func(j *schedulerpb.Job) { j.AttemptDeadline = durationpb.New(20 * time.Second) })

		_, err := b.RunJob(j.GetName())
		require.NoError(t, err)
		b.Drain()

		assert.Equal(t, 20*time.Second, elapsed)

		got, err := b.GetJob(j.GetName())
		require.NoError(t, err)
		assert.EqualValues(t, code.Code_DEADLINE_EXCEEDED, got.GetStatus().GetCode())
	})
}
