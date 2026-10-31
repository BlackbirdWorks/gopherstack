package ociqueue_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutGetDeleteFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	q := e.mkQueue(t, "flow", nil)
	c := e.data(t, q)

	put(t, c, q, "", "one", "two", "three")

	got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.Limit = new(2) })
	require.Len(t, got, 2)
	assert.Equal(t, "one", *got[0].Content)
	assert.Equal(t, "two", *got[1].Content)
	assert.Equal(t, 1, *got[0].DeliveryCount)
	assert.NotEmpty(t, *got[0].Receipt)

	again := get(t, c, q, func(r *queue.GetMessagesRequest) { r.Limit = new(20) })
	require.Len(t, again, 1, "in-flight messages are hidden")
	assert.Equal(t, "three", *again[0].Content)

	stats, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 0, *stats.Queue.VisibleMessages)
	assert.EqualValues(t, 3, *stats.Queue.InFlightMessages)
	assert.EqualValues(t, 11, *stats.Queue.SizeInBytes)

	_, err = c.DeleteMessage(t.Context(), queue.DeleteMessageRequest{QueueId: q.Id, MessageReceipt: got[0].Receipt})
	require.NoError(t, err)

	_, err = c.DeleteMessage(t.Context(), queue.DeleteMessageRequest{QueueId: q.Id, MessageReceipt: got[0].Receipt})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	res, err := c.DeleteMessages(t.Context(), queue.DeleteMessagesRequest{
		QueueId: q.Id,
		Entries: []queue.DeleteMessagesDetailsEntry{
			{Receipt: got[1].Receipt}, {Receipt: new("bogus")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, *res.ClientFailures)
	assert.Equal(t, 0, *res.ServerFailures)
	require.Len(t, res.Entries, 2)
	assert.Nil(t, res.Entries[0].ErrorCode)
	assert.Equal(t, http.StatusNotFound, *res.Entries[1].ErrorCode)

	e.clock.Advance(31 * time.Second)

	left := get(t, c, q, nil)
	require.Len(t, left, 0+1)
	assert.Equal(t, "three", *left[0].Content)
	assert.Equal(t, 2, *left[0].DeliveryCount, "redelivery after the visibility timeout")
}

func TestVisibilityUpdates(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "vis", nil)
	c := e.data(t, q)

	put(t, c, q, "", "a", "b")
	got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.Limit = new(2); r.VisibilityInSeconds = new(100) })
	require.Len(t, got, 2)

	upd, err := c.UpdateMessage(t.Context(), queue.UpdateMessageRequest{
		QueueId: q.Id, MessageReceipt: got[0].Receipt,
		VisibilityInSeconds: new(0),
	})
	require.NoError(t, err)
	assert.Equal(t, *got[0].Id, *upd.Id)

	again := get(t, c, q, nil)
	require.Len(t, again, 1)
	assert.Equal(t, "a", *again[0].Content)

	batch, err := c.UpdateMessages(t.Context(), queue.UpdateMessagesRequest{
		QueueId: q.Id,
		Entries: []queue.UpdateMessagesDetailsEntry{
			{Receipt: got[1].Receipt, VisibilityInSeconds: new(0)},
			{Receipt: new("bogus"), VisibilityInSeconds: new(10)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, *batch.ClientFailures)
	require.NotNil(t, batch.Entries[0].VisibleAfter)
	assert.Equal(t, http.StatusNotFound, *batch.Entries[1].ErrorCode)

	b := get(t, c, q, nil)
	require.Len(t, b, 1)
	assert.Equal(t, "b", *b[0].Content)

	_, err = c.UpdateMessage(t.Context(), queue.UpdateMessageRequest{
		QueueId: q.Id, MessageReceipt: got[1].Receipt,
		VisibilityInSeconds: new(50000),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestPeekIncrementsDeliveryCount(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "peek", nil)
	c := e.data(t, q)

	put(t, c, q, "", "a")

	for want := 1; want <= 3; want++ {
		got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.VisibilityInSeconds = new(0) })
		require.Len(t, got, 1)
		assert.Equal(t, want, *got[0].DeliveryCount)
	}
}

func TestPutValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "putval", nil)
	c := e.data(t, q)

	many := make([]queue.PutMessagesDetailsEntry, 21)
	for i := range many {
		many[i] = queue.PutMessagesDetailsEntry{Content: new("x")}
	}

	tests := []struct {
		name    string
		entries []queue.PutMessagesDetailsEntry
	}{
		{"empty", []queue.PutMessagesDetailsEntry{}},
		{"too many", many},
		{"too large", []queue.PutMessagesDetailsEntry{{Content: new(strings.Repeat("x", 128*1024+1))}}},
		{"long channel", []queue.PutMessagesDetailsEntry{{
			Content: new("x"), Metadata: &queue.MessageMetadata{ChannelId: new(strings.Repeat("c", 65))},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.PutMessages(t.Context(), queue.PutMessagesRequest{
				QueueId: q.Id, Messages: tt.entries,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	_, err := c.GetMessages(t.Context(), queue.GetMessagesRequest{QueueId: q.Id, Limit: new(21)})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	put(t, c, q, "", strings.Repeat("x", 128*1024))
}

func TestChannels(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "chan", nil)
	c := e.data(t, q)

	put(t, c, q, "alpha", "a1", "a2")
	put(t, c, q, "beta", "b1")
	put(t, c, q, "", "plain")

	ch, err := c.ListChannels(t.Context(), queue.ListChannelsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, ch.Items)

	one, err := c.ListChannels(t.Context(), queue.ListChannelsRequest{QueueId: q.Id, Limit: new(1)})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha"}, one.Items)
	require.NotNil(t, one.OpcNextPage)

	filtered, err := c.ListChannels(t.Context(), queue.ListChannelsRequest{QueueId: q.Id, ChannelFilter: new("bet")})
	require.NoError(t, err)
	assert.Equal(t, []string{"beta"}, filtered.Items)

	st, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id, ChannelId: new("alpha")})
	require.NoError(t, err)
	assert.EqualValues(t, 2, *st.Queue.VisibleMessages)

	got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.ChannelFilter = new("beta"); r.Limit = new(5) })
	require.Len(t, got, 1)
	assert.Equal(t, "b1", *got[0].Content)
	assert.Equal(t, "beta", *got[0].Metadata.ChannelId)
}

func TestRetentionExpiresMessages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "ret", func(d *queue.CreateQueueDetails) { d.RetentionInSeconds = new(60) })
	c := e.data(t, q)

	put(t, c, q, "", "old")
	e.clock.Advance(61 * time.Second)
	assert.Empty(t, get(t, c, q, nil))

	st, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 0, *st.Queue.SizeInBytes)
}

func TestDeadLetterQueue(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "dlq", func(d *queue.CreateQueueDetails) { d.DeadLetterQueueDeliveryCount = new(2) })
	c := e.data(t, q)

	put(t, c, q, "", "poison", "fine")

	peek := func(r *queue.GetMessagesRequest) { r.VisibilityInSeconds = new(0); r.Limit = new(1) }
	for range 2 {
		got := get(t, c, q, peek)
		require.Len(t, got, 1)
		assert.Equal(t, "poison", *got[0].Content)
	}

	got := get(t, c, q, peek)
	require.Len(t, got, 1)
	assert.Equal(t, "fine", *got[0].Content, "the poison message went to the dead letter queue")

	st, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 1, *st.Dlq.VisibleMessages)
	assert.EqualValues(t, len("poison"), *st.Dlq.SizeInBytes)

	_, err = e.admin.PurgeQueue(t.Context(), queue.PurgeQueueRequest{
		QueueId:   q.Id,
		PurgeType: queue.PurgeQueueDetailsPurgeTypeDlq,
	})
	require.NoError(t, err)

	st, err = c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 0, *st.Dlq.VisibleMessages)
	assert.EqualValues(t, 1, *st.Queue.VisibleMessages)
}

func TestDLQDisabled(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "nodlq", func(d *queue.CreateQueueDetails) { d.DeadLetterQueueDeliveryCount = new(0) })
	c := e.data(t, q)

	put(t, c, q, "", "m")

	for want := 1; want <= 8; want++ {
		got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.VisibilityInSeconds = new(0) })
		require.Len(t, got, 1)
		assert.Equal(t, want, *got[0].DeliveryCount)
	}
}

func TestPurge(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		det  queue.PurgeQueueDetails
		name string
		want int64
		code int
	}{
		{
			queue.PurgeQueueDetails{PurgeType: queue.PurgeQueueDetailsPurgeTypeNormal, ConsumerGroupId: new("g")},
			"group", 0, http.StatusBadRequest,
		},
		{
			queue.PurgeQueueDetails{PurgeType: queue.PurgeQueueDetailsPurgeTypeNormal, ChannelIds: []string{"x"}},
			"channel", 1, 0,
		},
		{queue.PurgeQueueDetails{PurgeType: queue.PurgeQueueDetailsPurgeTypeBoth}, "all", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := e.mkQueue(t, "purge-"+tt.name, nil)
			c := e.data(t, q)

			put(t, c, q, "x", "a")
			put(t, c, q, "y", "b")

			_, err := e.admin.PurgeQueue(t.Context(), queue.PurgeQueueRequest{QueueId: q.Id, PurgeQueueDetails: tt.det})
			if tt.code != 0 {
				assert.Equal(t, tt.code, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)

			st, serr := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
			require.NoError(t, serr)
			assert.Equal(t, tt.want, *st.Queue.VisibleMessages)
		})
	}
}

func TestLongPollWakesOnPut(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "poll", nil)
	c := e.data(t, q)

	parked := make(chan struct{}, 4)
	e.backend.SetTimer(func(time.Duration) <-chan time.Time {
		parked <- struct{}{}

		return nil
	})

	done := make(chan []queue.GetMessage, 1)

	go func() {
		resp, err := c.GetMessages(
			context.Background(),
			queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(30)},
		)
		if err == nil {
			done <- resp.Messages
		}

		close(done)
	}()

	<-parked
	put(t, c, q, "", "wake up")

	got := <-done
	require.Len(t, got, 1)
	assert.Equal(t, "wake up", *got[0].Content)
}

func TestLongPollTimeoutReturnsEmpty(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "polltimeout", nil)
	c := e.data(t, q)

	fire := make(chan time.Time, 1)
	parked := make(chan time.Duration, 4)
	e.backend.SetTimer(func(d time.Duration) <-chan time.Time {
		parked <- d

		return fire
	})

	done := make(chan []queue.GetMessage, 1)

	go func() {
		resp, err := c.GetMessages(
			context.Background(),
			queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(7)},
		)
		if err == nil {
			done <- resp.Messages
		}

		close(done)
	}()

	assert.Equal(t, 7*time.Second, <-parked)
	fire <- time.Time{}

	got, ok := <-done
	require.True(t, ok)
	assert.Empty(t, got)
}

func TestLongPollWakesWhenVisibilityExpires(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "pollvis", nil)
	c := e.data(t, q)

	put(t, c, q, "", "hidden")
	require.Len(t, get(t, c, q, func(r *queue.GetMessagesRequest) { r.VisibilityInSeconds = new(10) }), 1)

	durations := make(chan time.Duration, 8)
	fires := make(chan chan time.Time, 8)
	e.backend.SetTimer(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		durations <- d
		fires <- ch

		return ch
	})

	done := make(chan []queue.GetMessage, 1)

	go func() {
		resp, err := c.GetMessages(
			context.Background(),
			queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(30)},
		)
		if err == nil {
			done <- resp.Messages
		}

		close(done)
	}()

	assert.Equal(t, 30*time.Second, <-durations)
	<-fires
	assert.Equal(t, 10*time.Second, <-durations)

	e.clock.Advance(11 * time.Second)
	(<-fires) <- time.Time{}

	got := <-done
	require.Len(t, got, 1)
	assert.Equal(t, 2, *got[0].DeliveryCount)
}

func TestLongPollEndsOnShutdown(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "pollstop", nil)
	c := e.data(t, q)

	parked := make(chan struct{}, 4)
	e.backend.SetTimer(func(time.Duration) <-chan time.Time {
		parked <- struct{}{}

		return nil
	})

	done := make(chan struct{})

	go func() {
		defer close(done)

		_, _ = c.GetMessages(context.Background(), queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(30)})
	}()

	<-parked
	e.handler.Shutdown(context.Background())
	<-done
}

func TestDeleteQueueReleasesPollers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "polldel", nil)
	c := e.data(t, q)

	parked := make(chan struct{}, 4)
	e.backend.SetTimer(func(time.Duration) <-chan time.Time {
		parked <- struct{}{}

		return nil
	})

	errc := make(chan error, 1)

	go func() {
		_, err := c.GetMessages(
			context.Background(),
			queue.GetMessagesRequest{QueueId: q.Id, TimeoutInSeconds: new(30)},
		)
		errc <- err
	}()

	<-parked

	_, err := e.admin.DeleteQueue(t.Context(), queue.DeleteQueueRequest{QueueId: q.Id})
	require.NoError(t, err)

	assert.Equal(t, http.StatusConflict, svcErr(t, <-errc).GetHTTPStatusCode())
}

func TestBackendPeekLimitBounds(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "peeklimit", nil)
	c := e.data(t, q)

	for range 3 {
		put(t, c, q, "", "m")
	}

	tests := []struct {
		name    string
		limit   int
		want    int
		wantErr bool
	}{
		{name: "within", limit: 2, want: 2},
		{name: "max", limit: 20, want: 3},
		{name: "zero", limit: 0, wantErr: true},
		{name: "over", limit: 1 << 30, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := e.backend.Peek(*q.Id, false, tt.limit)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, got, tt.want)
		})
	}
}
