package ociqueue_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociqueue"
)

func (e *env) groupQueue(t *testing.T, name string, mod func(*queue.CreateQueueDetails)) queue.Queue {
	t.Helper()

	return e.mkQueue(t, name, func(d *queue.CreateQueueDetails) {
		d.Capabilities = []queue.CapabilityDetails{queue.ConsumerGroupsCapabilityDetails{}}
		if mod != nil {
			mod(d)
		}
	})
}

type groupMod func(*queue.CreateConsumerGroupDetails)

func (e *env) mkGroup(t *testing.T, q queue.Queue, name, filter string, mod groupMod) queue.ConsumerGroup {
	t.Helper()

	d := queue.CreateConsumerGroupDetails{DisplayName: new(name), QueueId: q.Id}
	if filter != "" {
		d.Filter = new(filter)
	}

	if mod != nil {
		mod(&d)
	}

	_, err := e.admin.CreateConsumerGroup(t.Context(), queue.CreateConsumerGroupRequest{CreateConsumerGroupDetails: d})
	require.NoError(t, err)
	e.settle()

	list, err := e.admin.ListConsumerGroups(t.Context(), queue.ListConsumerGroupsRequest{
		QueueId: q.Id, DisplayName: new(name),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	got, err := e.admin.GetConsumerGroup(t.Context(), queue.GetConsumerGroupRequest{ConsumerGroupId: list.Items[0].Id})
	require.NoError(t, err)

	return got.ConsumerGroup
}

func putProps(t *testing.T, c queue.QueueClient, q queue.Queue, content string, props map[string]string) {
	t.Helper()

	entry := queue.PutMessagesDetailsEntry{
		Content:  new(content),
		Metadata: &queue.MessageMetadata{ChannelId: new("c"), CustomProperties: props},
	}

	_, err := c.PutMessages(t.Context(),
		queue.PutMessagesRequest{QueueId: q.Id, Messages: []queue.PutMessagesDetailsEntry{entry}})
	require.NoError(t, err)
}

func contents(msgs []queue.GetMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, *m.Content)
	}

	return out
}

func TestConsumerGroupFanOut(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	q := e.groupQueue(t, "fan", nil)
	c := e.data(t, q)

	require.Len(t, q.Capabilities, 1)

	vips := e.mkGroup(t, q, "vips", `:tier = "gold"`, nil)
	all := e.mkGroup(t, q, "all", "", nil)

	putProps(t, c, q, "m-gold", map[string]string{"tier": "gold"})
	putProps(t, c, q, "m-plain", map[string]string{"tier": "basic"})

	fetch := func(group *string, vis int) []queue.GetMessage {
		return get(t, c, q, func(r *queue.GetMessagesRequest) {
			r.ConsumerGroupId, r.Limit, r.VisibilityInSeconds = group, new(20), new(vis)
		})
	}

	assert.ElementsMatch(t, []string{"m-gold", "m-plain"}, contents(fetch(nil, 30)), "primary")
	assert.Empty(t, fetch(nil, 30), "primary messages are in flight")
	assert.Equal(t, []string{"m-gold"}, contents(fetch(vips.Id, 30)))
	assert.ElementsMatch(t, []string{"m-gold", "m-plain"}, contents(fetch(all.Id, 30)),
		"other groups keep their own visibility")
	assert.Empty(t, fetch(vips.Id, 30))
}

func TestConsumerGroupDeleteIsPerGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.groupQueue(t, "del", nil)
	c := e.data(t, q)
	g := e.mkGroup(t, q, "g", "", nil)

	put(t, c, q, "", "one")

	got := get(t, c, q, func(r *queue.GetMessagesRequest) { r.ConsumerGroupId = g.Id })
	require.Len(t, got, 1)

	_, err := c.DeleteMessage(t.Context(), queue.DeleteMessageRequest{
		QueueId: q.Id, MessageReceipt: got[0].Receipt,
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "receipt belongs to the group")

	_, err = c.DeleteMessage(t.Context(), queue.DeleteMessageRequest{
		QueueId: q.Id, MessageReceipt: got[0].Receipt, ConsumerGroupId: g.Id,
	})
	require.NoError(t, err)

	st, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 1, *st.Queue.VisibleMessages, "the primary still holds the message")

	st, err = c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id, ConsumerGroupId: g.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 0, *st.Queue.VisibleMessages)
}

func TestConsumerGroupDeadLetterIsPerGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.groupQueue(t, "dlq", func(d *queue.CreateQueueDetails) { d.DeadLetterQueueDeliveryCount = new(5) })
	c := e.data(t, q)
	g := e.mkGroup(t, q, "g", "", func(d *queue.CreateConsumerGroupDetails) { d.DeadLetterQueueDeliveryCount = new(1) })

	put(t, c, q, "", "poison")
	require.Len(t, get(t, c, q, func(r *queue.GetMessagesRequest) {
		r.ConsumerGroupId, r.VisibilityInSeconds = g.Id, new(1)
	}), 1)
	e.clock.Advance(2e9)

	st, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id, ConsumerGroupId: g.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 1, *st.Dlq.VisibleMessages)
	assert.EqualValues(t, 0, *st.Queue.VisibleMessages)

	st, err = c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 0, *st.Dlq.VisibleMessages)
	assert.EqualValues(t, 1, *st.Queue.VisibleMessages)
}

func TestConsumerGroupLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	q := e.groupQueue(t, "life", nil)
	c := e.data(t, q)

	created, err := e.admin.CreateConsumerGroup(ctx, queue.CreateConsumerGroupRequest{
		DisplayName: new("g"), QueueId: q.Id, FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	wr, err := e.admin.GetWorkRequest(ctx, queue.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, queue.OperationTypeCreateConsumerGroup, wr.OperationType)

	list, err := e.admin.ListConsumerGroups(ctx, queue.ListConsumerGroupsRequest{QueueId: q.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 2, "primary and g")

	var g queue.ConsumerGroupSummary

	for _, it := range list.Items {
		if *it.DisplayName == "g" {
			g = it
		}
	}

	assert.Equal(t, queue.ConsumerGroupLifecycleStateCreating, g.LifecycleState)
	assert.Equal(t, "v", g.FreeformTags["k"])
	e.settle()

	got, err := e.admin.GetConsumerGroup(ctx, queue.GetConsumerGroupRequest{ConsumerGroupId: g.Id})
	require.NoError(t, err)
	assert.Equal(t, queue.ConsumerGroupLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, *q.Id, *got.QueueId)
	require.NotNil(t, got.Etag)

	_, err = e.admin.UpdateConsumerGroup(ctx, queue.UpdateConsumerGroupRequest{
		ConsumerGroupId: g.Id, IfMatch: new("stale"),
		DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.admin.UpdateConsumerGroup(ctx, queue.UpdateConsumerGroupRequest{
		ConsumerGroupId: g.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), Filter: new(`:a = 1`), IsEnabled: new(false),
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.admin.GetConsumerGroup(ctx, queue.GetConsumerGroupRequest{ConsumerGroupId: g.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *got.DisplayName)
	assert.Equal(t, ":a = 1", *got.Filter)
	assert.Equal(t, queue.ConsumerGroupLifecycleStateInactive, got.LifecycleState)

	put(t, c, q, "", "while-off")
	off := get(t, c, q, func(r *queue.GetMessagesRequest) { r.ConsumerGroupId = g.Id })
	assert.Empty(t, off, "disabled group gets nothing")

	_, err = e.admin.DeleteConsumerGroup(ctx, queue.DeleteConsumerGroupRequest{ConsumerGroupId: g.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.admin.GetConsumerGroup(ctx, queue.GetConsumerGroupRequest{ConsumerGroupId: g.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = c.GetMessages(ctx, queue.GetMessagesRequest{QueueId: q.Id, ConsumerGroupId: g.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestConsumerGroupPrimary(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	q := e.groupQueue(t, "primary", nil)
	c := e.data(t, q)

	got, err := e.admin.GetConsumerGroup(ctx, queue.GetConsumerGroupRequest{ConsumerGroupId: q.Id})
	require.NoError(t, err)
	assert.Equal(t, *q.Id, *got.Id)
	assert.Empty(t, *got.Filter)

	_, err = e.admin.DeleteConsumerGroup(ctx, queue.DeleteConsumerGroupRequest{ConsumerGroupId: q.Id})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.admin.UpdateConsumerGroup(ctx, queue.UpdateConsumerGroupRequest{
		ConsumerGroupId: q.Id, Filter: new(":a"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	g := e.mkGroup(t, q, "catchall", "", nil)

	_, err = e.admin.UpdateConsumerGroup(ctx, queue.UpdateConsumerGroupRequest{
		ConsumerGroupId: q.Id, IsEnabled: new(false),
	})
	require.NoError(t, err)

	put(t, c, q, "", "m")
	assert.Empty(t, get(t, c, q, nil), "disabled primary stores nothing")
	viaGroup := get(t, c, q, func(r *queue.GetMessagesRequest) { r.ConsumerGroupId = g.Id })
	assert.Equal(t, []string{"m"}, contents(viaGroup))
}

func TestConsumerGroupValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.groupQueue(t, "val", nil)
	plain := e.mkQueue(t, "plain", nil)
	e.mkGroup(t, q, "Dup", "", nil)

	mk := func(name string, qid *string) queue.CreateConsumerGroupDetails {
		return queue.CreateConsumerGroupDetails{DisplayName: new(name), QueueId: qid}
	}

	badFilter, badDLQ := mk("f", q.Id), mk("d", q.Id)
	badFilter.Filter = new(":a AND")
	badDLQ.DeadLetterQueueDeliveryCount = new(21)

	tests := []struct {
		det  queue.CreateConsumerGroupDetails
		name string
		code int
	}{
		{mk("dup", q.Id), "dup name", http.StatusConflict},
		{mk(defaultPrimary, q.Id), "primary name", http.StatusConflict},
		{badFilter, "bad filter", http.StatusBadRequest},
		{badDLQ, "dlq", http.StatusBadRequest},
		{mk("n", plain.Id), "no capability", http.StatusConflict},
		{mk("n", new("ocid1.queue.oc1..none")), "no queue", http.StatusNotFound},
		{mk("n", nil), "no queue id", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.admin.CreateConsumerGroup(t.Context(),
				queue.CreateConsumerGroupRequest{CreateConsumerGroupDetails: tt.det})
			assert.Equal(t, tt.code, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	_, err := e.data(t, plain).GetMessages(t.Context(),
		queue.GetMessagesRequest{QueueId: plain.Id, ConsumerGroupId: new("x")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

const defaultPrimary = "Primary Consumer Group"

func TestConsumerGroupLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.groupQueue(t, "limit", nil)

	for i := range 9 {
		e.mkGroup(t, q, "g"+string(rune('a'+i)), "", nil)
	}

	_, err := e.admin.CreateConsumerGroup(t.Context(), queue.CreateConsumerGroupRequest{
		DisplayName: new("over"), QueueId: q.Id,
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestConsumerGroupRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.groupQueue(t, "retry", nil)
	req := queue.CreateConsumerGroupRequest{
		OpcRetryToken: new("tok"), DisplayName: new("g"), QueueId: q.Id,
	}

	first, err := e.admin.CreateConsumerGroup(t.Context(), req)
	require.NoError(t, err)

	second, err := e.admin.CreateConsumerGroup(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, *first.OpcWorkRequestId, *second.OpcWorkRequestId)

	list, err := e.admin.ListConsumerGroups(t.Context(), queue.ListConsumerGroupsRequest{QueueId: q.Id})
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)
}

func TestConsumerGroupPurge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		group     func(g queue.ConsumerGroup, q queue.Queue) *string
		name      string
		primary   int64
		secondary int64
	}{
		{func(g queue.ConsumerGroup, _ queue.Queue) *string { return g.Id }, "group", 1, 0},
		{func(queue.ConsumerGroup, queue.Queue) *string { return nil }, "primary", 0, 1},
		{func(queue.ConsumerGroup, queue.Queue) *string { return new("all") }, "all", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			q := e.groupQueue(t, "purge", nil)
			g := e.mkGroup(t, q, "g", "", nil)
			c := e.data(t, q)
			put(t, c, q, "", "m")

			_, err := e.admin.PurgeQueue(t.Context(), queue.PurgeQueueRequest{
				QueueId:   q.Id,
				PurgeType: queue.PurgeQueueDetailsPurgeTypeNormal, ConsumerGroupId: tt.group(g, q),
			})
			require.NoError(t, err)

			st, err := c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.primary, *st.Queue.VisibleMessages)

			st, err = c.GetStats(t.Context(), queue.GetStatsRequest{QueueId: q.Id, ConsumerGroupId: g.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.secondary, *st.Queue.VisibleMessages)
		})
	}
}

func TestConsumerGroupSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.groupQueue(t, "snap", nil)
	g := e.mkGroup(t, q, "g", ":a", nil)
	putProps(t, e.data(t, q), q, "m", map[string]string{"a": "1"})

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	w, _, err := restored.GetConsumerGroup(*g.Id)
	require.NoError(t, err)
	assert.Equal(t, ":a", w.Filter)

	st, err := restored.GetStats(*q.Id, *g.Id, "")
	require.NoError(t, err)
	assert.EqualValues(t, 1, st.Queue.VisibleMessages)
}

func TestConsumerGroupsCapabilityUpdate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "enable", nil)
	assert.Empty(t, q.Capabilities)

	_, err := e.admin.UpdateQueue(t.Context(), queue.UpdateQueueRequest{
		QueueId: q.Id,
		Capabilities: []queue.CapabilityDetails{
			queue.ConsumerGroupsCapabilityDetails{
				IsPrimaryConsumerGroupEnabled: new(true), PrimaryConsumerGroupDisplayName: new("main"),
			},
		},
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.admin.GetConsumerGroup(t.Context(), queue.GetConsumerGroupRequest{ConsumerGroupId: q.Id})
	require.NoError(t, err)
	assert.Equal(t, "main", *got.DisplayName)

	_, err = e.admin.UpdateQueue(t.Context(), queue.UpdateQueueRequest{
		QueueId: q.Id,
		Capabilities: []queue.CapabilityDetails{
			queue.ConsumerGroupsCapabilityDetails{PrimaryConsumerGroupFilter: new(":a")},
		},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}
