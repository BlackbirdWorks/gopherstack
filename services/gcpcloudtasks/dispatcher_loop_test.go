package gcpcloudtasks_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudtasks"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestDispatcherLoopHonoursBackoff drives the real ticker loop on synctest's fake clock.
func TestDispatcherLoopHonoursBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const tick = 10 * time.Millisecond

		var calls []time.Time

		done := make(chan struct{})
		rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls = append(calls, time.Now())

			code := http.StatusInternalServerError
			if len(calls) == 3 {
				code = http.StatusOK

				close(done)
			}

			return &http.Response{
				StatusCode: code, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: r,
			}, nil
		})

		b := gcpcloudtasks.NewInMemoryBackend()
		b.SetHTTPClient(&http.Client{Transport: rt})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		b.SetRunContext(ctx)

		q, err := b.CreateQueue(&cloudtaskspb.CreateQueueRequest{
			Parent: location,
			Queue: &cloudtaskspb.Queue{
				Name: location + "/queues/loop",
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts:  5,
					MinBackoff:   durationpb.New(time.Second),
					MaxBackoff:   durationpb.New(time.Minute),
					MaxDoublings: 4,
				},
			},
		})
		require.NoError(t, err)

		_, err = b.CreateTask(&cloudtaskspb.CreateTaskRequest{Parent: q.GetName(), Task: &cloudtaskspb.Task{
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{Url: "https://target.test/x"},
			},
		}})
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
