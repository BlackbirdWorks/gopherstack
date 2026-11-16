package ociqueue_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociqueue"
)

func putErr(t *testing.T, c queue.QueueClient, q queue.Queue, channel, content string) error {
	t.Helper()

	e := queue.PutMessagesDetailsEntry{Content: new(content)}
	if channel != "" {
		e.Metadata = &queue.MessageMetadata{ChannelId: new(channel)}
	}

	_, err := c.PutMessages(
		t.Context(),
		queue.PutMessagesRequest{QueueId: q.Id, Messages: []queue.PutMessagesDetailsEntry{e}},
	)

	return err
}

func TestServiceLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *env)
		name string
		lim  oci.LimitOverrides
		rate bool
	}{
		{name: "queues per tenancy", lim: oci.LimitOverrides{Queues: 2}, run: func(t *testing.T, e *env) {
			t.Helper()

			first := e.mkQueue(t, "one", nil)
			e.mkQueue(t, "two", nil)

			d := queue.CreateQueueDetails{CompartmentId: new(testCompartment), DisplayName: new("three")}
			_, err := e.admin.CreateQueue(t.Context(), queue.CreateQueueRequest{CreateQueueDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, "LimitExceeded", se.GetCode())

			_, err = e.admin.DeleteQueue(t.Context(), queue.DeleteQueueRequest{QueueId: first.Id})
			require.NoError(t, err)
			e.settle()
			e.mkQueue(t, "three", nil)
		}},
		{name: "channels per queue", lim: oci.LimitOverrides{Channels: 2}, run: func(t *testing.T, e *env) {
			t.Helper()

			q := e.mkQueue(t, "ch", nil)
			c := e.data(t, q)
			require.NoError(t, putErr(t, c, q, "a", "x"))
			require.NoError(t, putErr(t, c, q, "b", "x"))
			assert.Equal(t, "LimitExceeded", svcErr(t, putErr(t, c, q, "c", "x")).GetCode())
			require.NoError(t, putErr(t, c, q, "a", "y"))
		}},
		{name: "queue storage", lim: oci.LimitOverrides{QueueBytes: 10}, run: func(t *testing.T, e *env) {
			t.Helper()

			q := e.mkQueue(t, "bytes", nil)
			c := e.data(t, q)
			require.NoError(t, putErr(t, c, q, "", "123456"))
			assert.Equal(t, "LimitExceeded", svcErr(t, putErr(t, c, q, "", "12345")).GetCode())
			require.NoError(t, putErr(t, c, q, "", "1234"))
		}},
		{name: "tenancy storage", lim: oci.LimitOverrides{TenancyBytes: 10}, run: func(t *testing.T, e *env) {
			t.Helper()

			q1, q2 := e.mkQueue(t, "t1", nil), e.mkQueue(t, "t2", nil)
			require.NoError(t, putErr(t, e.data(t, q1), q1, "", "123456"))
			assert.Equal(t, "LimitExceeded", svcErr(t, putErr(t, e.data(t, q2), q2, "", "12345")).GetCode())
		}},
		{name: "channel consumption", lim: oci.LimitOverrides{QueueBytes: 20}, run: func(t *testing.T, e *env) {
			t.Helper()

			q := e.mkQueue(t, "cons", func(d *queue.CreateQueueDetails) { d.ChannelConsumptionLimit = new(50) })
			c := e.data(t, q)
			require.NoError(t, putErr(t, c, q, "a", "123456"))
			assert.Equal(t, "LimitExceeded", svcErr(t, putErr(t, c, q, "a", "12345")).GetCode())
			require.NoError(t, putErr(t, c, q, "b", "12345"))
		}},
		{name: "in flight", lim: oci.LimitOverrides{InFlight: 2}, run: func(t *testing.T, e *env) {
			t.Helper()

			q := e.mkQueue(t, "fly", nil)
			c := e.data(t, q)
			put(t, c, q, "", "a", "b", "c")

			got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.Limit = new(3) })
			require.Len(t, got, 2)

			_, err := c.GetMessages(t.Context(), queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(0)})
			assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

			_, err = c.DeleteMessage(
				t.Context(),
				queue.DeleteMessageRequest{QueueId: q.Id, MessageReceipt: got[0].Receipt},
			)
			require.NoError(t, err)
			require.Len(t, get(t, c, q, nil), 1)
		}},
		{name: "request rate", rate: true, lim: oci.LimitOverrides{RequestsPerSec: 2}, run: func(t *testing.T, e *env) {
			t.Helper()

			q := e.mkQueue(t, "rate", nil)
			c := e.data(t, q)
			require.NoError(t, putErr(t, c, q, "", "a"))
			require.NoError(t, putErr(t, c, q, "", "b"))

			se := svcErr(t, putErr(t, c, q, "", "c"))
			assert.Equal(t, http.StatusTooManyRequests, se.GetHTTPStatusCode())
			assert.Equal(t, "TooManyRequests", se.GetCode())

			_, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
			require.NoError(t, err, "limits are per API")

			e.clock.Advance(time.Second)
			require.NoError(t, putErr(t, c, q, "", "c"))
		}},
		{name: "byte rate", rate: true, lim: oci.LimitOverrides{BytesPerSec: 8}, run: func(t *testing.T, e *env) {
			t.Helper()

			q := e.mkQueue(t, "bytesrate", nil)
			c := e.data(t, q)
			require.NoError(t, putErr(t, c, q, "", "12345"))
			assert.Equal(t, http.StatusTooManyRequests, svcErr(t, putErr(t, c, q, "", "1234")).GetHTTPStatusCode())

			require.Len(t, get(t, c, q, nil), 1)
			_, err := c.GetMessages(t.Context(), queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(0)})
			require.NoError(t, err)

			e.clock.Advance(time.Second)
			require.NoError(t, putErr(t, c, q, "", "1234"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetLimitsForTest(tt.lim)
			e.backend.SetEnforceRateLimits(tt.rate)
			tt.run(t, e)
		})
	}
}

func TestRateLimitsOffByDefault(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetLimitsForTest(oci.LimitOverrides{RequestsPerSec: 1})
	q := e.mkQueue(t, "free", nil)
	c := e.data(t, q)

	for range 3 {
		require.NoError(t, putErr(t, c, q, "", "a"))
	}
}
