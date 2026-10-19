package gcppubsub_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	vkit "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

const (
	proj      = "projects/test-project"
	waitLimit = 30 * time.Second
)

type env struct {
	client  *pubsub.Client
	handler *gcppubsub.Handler
	opts    []option.ClientOption
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcppubsub.NewHandler(gcppubsub.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	client, err := pubsub.NewClient(t.Context(), "test-project", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, handler: h, opts: opts}
}

func (e *env) topic(t *testing.T, id string, mut ...func(*pubsubpb.Topic)) string {
	t.Helper()

	tp := &pubsubpb.Topic{Name: proj + "/topics/" + id}
	for _, m := range mut {
		m(tp)
	}

	_, err := e.client.TopicAdminClient.CreateTopic(t.Context(), tp)
	require.NoError(t, err)

	return tp.GetName()
}

func (e *env) sub(t *testing.T, id, topicName string, mut ...func(*pubsubpb.Subscription)) string {
	t.Helper()

	s := &pubsubpb.Subscription{Name: proj + "/subscriptions/" + id, Topic: topicName}
	for _, m := range mut {
		m(s)
	}

	_, err := e.client.SubscriptionAdminClient.CreateSubscription(t.Context(), s)
	require.NoError(t, err)

	return s.GetName()
}

func (e *env) publish(t *testing.T, topicName string, msgs ...*pubsub.Message) []string {
	t.Helper()

	pub := e.client.Publisher(topicName)
	pub.EnableMessageOrdering = true

	t.Cleanup(pub.Stop)

	results := make([]*pubsub.PublishResult, len(msgs))
	for i, m := range msgs {
		results[i] = pub.Publish(t.Context(), m)
	}

	ids := make([]string, len(msgs))

	for i, r := range results {
		id, err := r.Get(t.Context())
		require.NoError(t, err)

		ids[i] = id
	}

	return ids
}

// receive pulls n messages through StreamingPull, acking or nacking via handle.
func (e *env) receive(t *testing.T, subName string, n int, handle func(*pubsub.Message)) []*pubsub.Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitLimit)
	defer cancel()

	var (
		mu  sync.Mutex
		got []*pubsub.Message
	)

	sub := e.client.Subscriber(subName)

	require.NoError(t, sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		handle(m)

		mu.Lock()
		defer mu.Unlock()

		got = append(got, m)
		if len(got) >= n {
			cancel()
		}
	}))

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, got, n)

	return got
}

func ack(m *pubsub.Message) { m.Ack() }

func (e *env) pull(t *testing.T, subName string, ackThem bool) []*pubsubpb.ReceivedMessage {
	t.Helper()

	resp, err := e.client.SubscriptionAdminClient.Pull(t.Context(), &pubsubpb.PullRequest{
		Subscription: subName, MaxMessages: 100,
		ReturnImmediately: true, //nolint:staticcheck // deprecated but the emulator still honours it
	})
	require.NoError(t, err)

	if ackThem && len(resp.GetReceivedMessages()) > 0 {
		ids := make([]string, 0, len(resp.GetReceivedMessages()))
		for _, rm := range resp.GetReceivedMessages() {
			ids = append(ids, rm.GetAckId())
		}

		require.NoError(t, e.client.SubscriptionAdminClient.Acknowledge(t.Context(),
			&pubsubpb.AcknowledgeRequest{Subscription: subName, AckIds: ids}))
	}

	return resp.GetReceivedMessages()
}

func TestSDKPublishReceive(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "events")
	sub := e.sub(t, "events-sub", topic)

	ids := e.publish(t, topic,
		&pubsub.Message{Data: []byte("one"), Attributes: map[string]string{"k": "v"}},
		&pubsub.Message{Data: []byte("two")},
		&pubsub.Message{Attributes: map[string]string{"only": "attrs"}},
	)
	require.Len(t, ids, 3)

	got := e.receive(t, sub, 3, ack)

	byID := map[string]*pubsub.Message{}
	for _, m := range got {
		byID[m.ID] = m
	}

	require.Len(t, byID, 3)
	assert.Equal(t, "one", string(byID[ids[0]].Data))
	assert.Equal(t, map[string]string{"k": "v"}, byID[ids[0]].Attributes)
	assert.Equal(t, "two", string(byID[ids[1]].Data))
	assert.Equal(t, "attrs", byID[ids[2]].Attributes["only"])
	assert.WithinDuration(t, time.Now(), byID[ids[0]].PublishTime, time.Minute)

	assert.Empty(t, e.pull(t, sub, false), "acked messages must not redeliver")
}

func TestSDKFanOutAndPull(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "fan")
	s1 := e.sub(t, "fan-1", topic)
	s2 := e.sub(t, "fan-2", topic)

	e.publish(t, topic, &pubsub.Message{Data: []byte("x")})

	for _, s := range []string{s1, s2} {
		msgs := e.pull(t, s, true)
		require.Len(t, msgs, 1)
		assert.Equal(t, "x", string(msgs[0].GetMessage().GetData()))
		assert.Empty(t, e.pull(t, s, false))
	}
}

func TestSDKOrdering(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "ordered")
	sub := e.sub(t, "ordered-sub", topic, func(s *pubsubpb.Subscription) { s.EnableMessageOrdering = true })

	const n = 15

	msgs := make([]*pubsub.Message, n)
	for i := range msgs {
		msgs[i] = &pubsub.Message{Data: []byte{byte('a' + i)}, OrderingKey: "key"}
	}

	e.publish(t, topic, msgs...)

	got := e.receive(t, sub, n, ack)

	for i, m := range got {
		assert.Equal(t, string([]byte{byte('a' + i)}), string(m.Data), "message %d out of order", i)
		assert.Equal(t, "key", m.OrderingKey)
	}
}

func TestSDKFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "filtered")
	sub := e.sub(t, "filtered-sub", topic, func(s *pubsubpb.Subscription) {
		s.Filter = `attributes.color = "red" AND NOT attributes:skip`
	})

	e.publish(t, topic,
		&pubsub.Message{Data: []byte("blue"), Attributes: map[string]string{"color": "blue"}},
		&pubsub.Message{Data: []byte("red-skip"), Attributes: map[string]string{"color": "red", "skip": "1"}},
		&pubsub.Message{Data: []byte("red"), Attributes: map[string]string{"color": "red"}},
	)

	got := e.receive(t, sub, 1, ack)
	assert.Equal(t, "red", string(got[0].Data))
	assert.Empty(t, e.pull(t, sub, false))

	_, err := e.client.SubscriptionAdminClient.CreateSubscription(t.Context(), &pubsubpb.Subscription{
		Name: proj + "/subscriptions/bad-filter", Topic: topic, Filter: "attributes.x ==",
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSDKDeadLetter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "main")
	dead := e.topic(t, "dead")
	deadSub := e.sub(t, "dead-sub", dead)
	sub := e.sub(t, "main-sub", topic, func(s *pubsubpb.Subscription) {
		s.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dead, MaxDeliveryAttempts: 5}
	})

	e.publish(t, topic, &pubsub.Message{Data: []byte("poison"), Attributes: map[string]string{"a": "b"}})

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	wg.Add(1)

	attempts := make(chan int, 16)

	go func() {
		defer wg.Done()

		_ = e.client.Subscriber(sub).Receive(ctx, func(_ context.Context, m *pubsub.Message) {
			if m.DeliveryAttempt != nil {
				select {
				case attempts <- *m.DeliveryAttempt:
				default:
				}
			}

			m.Nack()
		})
	}()

	var dl []*pubsubpb.ReceivedMessage

	require.Eventually(t, func() bool {
		dl = e.pull(t, deadSub, true)

		return len(dl) > 0
	}, waitLimit, 20*time.Millisecond)

	cancel()
	wg.Wait()
	close(attempts)

	maxSeen := 0
	for a := range attempts {
		maxSeen = max(maxSeen, a)
	}

	assert.Equal(t, 5, maxSeen)
	assert.Equal(t, "poison", string(dl[0].GetMessage().GetData()))
	assert.Equal(t, "5", dl[0].GetMessage().GetAttributes()["CloudPubSubDeadLetterSourceDeliveryCount"])
	assert.Equal(t, sub, dl[0].GetMessage().GetAttributes()["CloudPubSubDeadLetterSourceSubscription"])
	assert.Equal(t, "b", dl[0].GetMessage().GetAttributes()["a"])
	assert.Empty(t, e.pull(t, sub, false))
}

func TestSDKExactlyOnce(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "eod")
	sub := e.sub(t, "eod-sub", topic, func(s *pubsubpb.Subscription) { s.EnableExactlyOnceDelivery = true })

	got, err := e.client.SubscriptionAdminClient.GetSubscription(t.Context(),
		&pubsubpb.GetSubscriptionRequest{Subscription: sub})
	require.NoError(t, err)
	assert.True(t, got.GetEnableExactlyOnceDelivery())

	e.publish(t, topic, &pubsub.Message{Data: []byte("once")})

	var ackStatus pubsub.AcknowledgeStatus

	e.receive(t, sub, 1, func(m *pubsub.Message) {
		ackStatus, err = m.AckWithResult().Get(t.Context())
	})

	require.NoError(t, err)
	assert.Equal(t, pubsub.AcknowledgeStatusSuccess, ackStatus)
	assert.Empty(t, e.pull(t, sub, false))
}

func TestSDKSeekToTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "seektime")
	sub := e.sub(t, "seektime-sub", topic, func(s *pubsubpb.Subscription) { s.RetainAckedMessages = true })

	e.publish(t, topic, &pubsub.Message{Data: []byte("old")})

	cut := time.Now()

	e.publish(t, topic, &pubsub.Message{Data: []byte("new")})
	require.Len(t, e.pull(t, sub, true), 2)

	_, err := e.client.SubscriptionAdminClient.Seek(t.Context(), &pubsubpb.SeekRequest{
		Subscription: sub, Target: &pubsubpb.SeekRequest_Time{Time: timestamppb.New(cut)},
	})
	require.NoError(t, err)

	msgs := e.pull(t, sub, true)
	require.Len(t, msgs, 1)
	assert.Equal(t, "new", string(msgs[0].GetMessage().GetData()))
}

func TestSDKSnapshotSeek(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "snap")
	sub := e.sub(t, "snap-sub", topic)
	admin := e.client.SubscriptionAdminClient

	e.publish(t, topic, &pubsub.Message{Data: []byte("a")}, &pubsub.Message{Data: []byte("b")})

	snapName := proj + "/snapshots/snap-1"
	created, err := admin.CreateSnapshot(t.Context(), &pubsubpb.CreateSnapshotRequest{
		Name: snapName, Subscription: sub, Labels: map[string]string{"l": "1"},
	})
	require.NoError(t, err)
	assert.Equal(t, topic, created.GetTopic())
	assert.True(t, created.GetExpireTime().AsTime().After(time.Now()))

	_, err = admin.CreateSnapshot(t.Context(), &pubsubpb.CreateSnapshotRequest{Name: snapName, Subscription: sub})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	e.publish(t, topic, &pubsub.Message{Data: []byte("c")})
	require.Len(t, e.pull(t, sub, true), 3)
	assert.Empty(t, e.pull(t, sub, false))

	_, err = admin.Seek(t.Context(), &pubsubpb.SeekRequest{
		Subscription: sub, Target: &pubsubpb.SeekRequest_Snapshot{Snapshot: snapName},
	})
	require.NoError(t, err)

	pulled := e.pull(t, sub, true)
	data := make([]string, 0, len(pulled))

	for _, m := range pulled {
		data = append(data, string(m.GetMessage().GetData()))
	}

	assert.ElementsMatch(t, []string{"a", "b", "c"}, data)

	snapIt := e.client.TopicAdminClient.ListTopicSnapshots(t.Context(),
		&pubsubpb.ListTopicSnapshotsRequest{Topic: topic})
	list, err := snapIt.Next()
	require.NoError(t, err)
	assert.Equal(t, snapName, list)

	require.NoError(t, admin.DeleteSnapshot(t.Context(), &pubsubpb.DeleteSnapshotRequest{Snapshot: snapName}))

	_, err = admin.GetSnapshot(t.Context(), &pubsubpb.GetSnapshotRequest{Snapshot: snapName})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSDKAdminErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "exists")
	e.sub(t, "exists-sub", topic)

	ta, sa := e.client.TopicAdminClient, e.client.SubscriptionAdminClient

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{func() error {
			_, err := ta.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: proj + "/topics/missing"})

			return err
		}, "get topic missing", codes.NotFound},
		{func() error {
			_, err := ta.CreateTopic(t.Context(), &pubsubpb.Topic{Name: topic})

			return err
		}, "create topic duplicate", codes.AlreadyExists},
		{func() error {
			_, err := ta.CreateTopic(t.Context(), &pubsubpb.Topic{Name: proj + "/topics/9bad"})

			return err
		}, "create topic bad name", codes.InvalidArgument},
		{func() error {
			_, err := ta.UpdateTopic(t.Context(), &pubsubpb.UpdateTopicRequest{Topic: &pubsubpb.Topic{Name: topic}})

			return err
		}, "update topic no mask", codes.InvalidArgument},
		{func() error {
			_, err := ta.UpdateTopic(t.Context(), &pubsubpb.UpdateTopicRequest{
				Topic: &pubsubpb.Topic{Name: topic}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			})

			return err
		}, "update topic immutable", codes.InvalidArgument},
		{func() error {
			return ta.DeleteTopic(t.Context(), &pubsubpb.DeleteTopicRequest{Topic: proj + "/topics/missing"})
		}, "delete topic missing", codes.NotFound},
		{func() error {
			_, err := ta.Publish(t.Context(), &pubsubpb.PublishRequest{Topic: proj + "/topics/missing",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("x")}}})

			return err
		}, "publish missing topic", codes.NotFound},
		{func() error {
			_, err := ta.Publish(t.Context(), &pubsubpb.PublishRequest{Topic: topic,
				Messages: []*pubsubpb.PubsubMessage{{}}})

			return err
		}, "publish empty message", codes.InvalidArgument},
		{func() error {
			_, err := ta.Publish(t.Context(), &pubsubpb.PublishRequest{Topic: topic})

			return err
		}, "publish no messages", codes.InvalidArgument},
		{func() error {
			_, err := sa.CreateSubscription(t.Context(), &pubsubpb.Subscription{
				Name: proj + "/subscriptions/orphan", Topic: proj + "/topics/missing"})

			return err
		}, "sub on missing topic", codes.NotFound},
		{func() error {
			_, err := sa.CreateSubscription(t.Context(), &pubsubpb.Subscription{
				Name: proj + "/subscriptions/exists-sub", Topic: topic})

			return err
		}, "sub duplicate", codes.AlreadyExists},
		{func() error {
			_, err := sa.CreateSubscription(t.Context(), &pubsubpb.Subscription{
				Name: proj + "/subscriptions/bad-ack", Topic: topic, AckDeadlineSeconds: 5})

			return err
		}, "sub ack deadline too small", codes.InvalidArgument},
		{func() error {
			_, err := sa.CreateSubscription(t.Context(), &pubsubpb.Subscription{
				Name:  proj + "/subscriptions/bad-dlq",
				Topic: topic,
				DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{
					DeadLetterTopic:     proj + "/topics/nope",
					MaxDeliveryAttempts: 5,
				},
			})

			return err
		}, "sub dead letter topic missing", codes.NotFound},
		{func() error {
			_, err := sa.CreateSubscription(t.Context(), &pubsubpb.Subscription{
				Name: proj + "/subscriptions/bad-dlq2", Topic: topic,
				DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: topic, MaxDeliveryAttempts: 2}})

			return err
		}, "sub dead letter attempts out of range", codes.InvalidArgument},
		{func() error {
			_, err := sa.GetSubscription(
				t.Context(),
				&pubsubpb.GetSubscriptionRequest{Subscription: proj + "/subscriptions/none"},
			)

			return err
		}, "get sub missing", codes.NotFound},
		{func() error {
			_, err := sa.UpdateSubscription(t.Context(), &pubsubpb.UpdateSubscriptionRequest{
				Subscription: &pubsubpb.Subscription{Name: proj + "/subscriptions/exists-sub", Topic: topic},
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"topic"}},
			})

			return err
		}, "update sub immutable topic", codes.InvalidArgument},
		{func() error {
			_, err := sa.Pull(
				t.Context(),
				&pubsubpb.PullRequest{Subscription: proj + "/subscriptions/none", MaxMessages: 1},
			)

			return err
		}, "pull missing sub", codes.NotFound},
		{func() error {
			return sa.Acknowledge(
				t.Context(),
				&pubsubpb.AcknowledgeRequest{Subscription: proj + "/subscriptions/exists-sub"},
			)
		}, "ack no ids", codes.InvalidArgument},
		{func() error {
			_, err := sa.Seek(t.Context(), &pubsubpb.SeekRequest{
				Subscription: proj + "/subscriptions/exists-sub",
				Target:       &pubsubpb.SeekRequest_Snapshot{Snapshot: proj + "/snapshots/none"}})

			return err
		}, "seek missing snapshot", codes.NotFound},
		{func() error {
			_, err := sa.CreateSnapshot(t.Context(), &pubsubpb.CreateSnapshotRequest{
				Name: proj + "/snapshots/snap-x1", Subscription: proj + "/subscriptions/none"})

			return err
		}, "snapshot missing sub", codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, status.Code(tt.call()))
		})
	}
}

func TestSDKTopicAndSubscriptionLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ta, sa := e.client.TopicAdminClient, e.client.SubscriptionAdminClient

	topic := e.topic(t, "life", func(tp *pubsubpb.Topic) {
		tp.Labels = map[string]string{"env": "test"}
		tp.MessageRetentionDuration = durationpb.New(time.Hour)
	})

	got, err := ta.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	require.NoError(t, err)
	assert.Equal(t, "test", got.GetLabels()["env"])
	assert.Equal(t, time.Hour, got.GetMessageRetentionDuration().AsDuration())

	upd, err := ta.UpdateTopic(t.Context(), &pubsubpb.UpdateTopicRequest{
		Topic:      &pubsubpb.Topic{Name: topic, Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", upd.GetLabels()["env"])
	assert.Equal(t, time.Hour, upd.GetMessageRetentionDuration().AsDuration(), "unmasked fields are preserved")

	for _, id := range []string{"life-a", "life-b", "life-c"} {
		e.sub(t, id, topic)
	}

	subs := collectStrings(
		t,
		ta.ListTopicSubscriptions(t.Context(), &pubsubpb.ListTopicSubscriptionsRequest{Topic: topic, PageSize: 2}).Next,
	)
	assert.Len(t, subs, 3)

	s, err := sa.GetSubscription(
		t.Context(),
		&pubsubpb.GetSubscriptionRequest{Subscription: proj + "/subscriptions/life-a"},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 10, s.GetAckDeadlineSeconds())
	assert.Equal(t, 7*24*time.Hour, s.GetMessageRetentionDuration().AsDuration())
	assert.Equal(t, time.Hour, s.GetTopicMessageRetentionDuration().AsDuration())

	us, err := sa.UpdateSubscription(t.Context(), &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: s.GetName(), AckDeadlineSeconds: 60, RetainAckedMessages: true},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"ack_deadline_seconds", "retain_acked_messages"}},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 60, us.GetAckDeadlineSeconds())
	assert.True(t, us.GetRetainAckedMessages())

	all := 0

	it := sa.ListSubscriptions(t.Context(), &pubsubpb.ListSubscriptionsRequest{Project: proj, PageSize: 2})
	for {
		_, nerr := it.Next()
		if nerr != nil {
			break
		}

		all++
	}

	assert.Equal(t, 3, all)

	require.NoError(t, e.client.SubscriptionAdminClient.DeleteSubscription(t.Context(),
		&pubsubpb.DeleteSubscriptionRequest{Subscription: s.GetName()}))
	require.NoError(t, ta.DeleteTopic(t.Context(), &pubsubpb.DeleteTopicRequest{Topic: topic}))

	orphan, err := sa.GetSubscription(
		t.Context(),
		&pubsubpb.GetSubscriptionRequest{Subscription: proj + "/subscriptions/life-b"},
	)
	require.NoError(t, err)
	assert.Equal(t, "_deleted-topic_", orphan.GetTopic())

	_, err = ta.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func collectStrings(t *testing.T, next func() (string, error)) []string {
	t.Helper()

	var out []string

	for {
		s, err := next()
		if err != nil {
			require.Contains(t, err.Error(), "no more items")

			return out
		}

		out = append(out, s)
	}
}

func TestSDKDetachSubscription(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "detach")
	sub := e.sub(t, "detach-sub", topic)

	e.publish(t, topic, &pubsub.Message{Data: []byte("before")})

	_, err := e.client.TopicAdminClient.DetachSubscription(
		t.Context(),
		&pubsubpb.DetachSubscriptionRequest{Subscription: sub},
	)
	require.NoError(t, err)

	e.publish(t, topic, &pubsub.Message{Data: []byte("after")})

	got, err := e.client.SubscriptionAdminClient.GetSubscription(
		t.Context(),
		&pubsubpb.GetSubscriptionRequest{Subscription: sub},
	)
	require.NoError(t, err)
	assert.True(t, got.GetDetached())

	msgs := e.pull(t, sub, true)
	require.Len(t, msgs, 1)
	assert.Equal(t, "before", string(msgs[0].GetMessage().GetData()))

	it := e.client.TopicAdminClient.ListTopicSubscriptions(
		t.Context(),
		&pubsubpb.ListTopicSubscriptionsRequest{Topic: topic},
	)
	_, err = it.Next()
	require.Error(t, err, "detached subscriptions are not listed")
}

func TestSDKModifyAckDeadline(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "modack")
	sub := e.sub(t, "modack-sub", topic)
	sa := e.client.SubscriptionAdminClient

	e.publish(t, topic, &pubsub.Message{Data: []byte("m")})

	first := e.pull(t, sub, false)
	require.Len(t, first, 1)
	assert.Empty(t, e.pull(t, sub, false), "leased message is hidden")

	require.NoError(t, sa.ModifyAckDeadline(t.Context(), &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{first[0].GetAckId()}, AckDeadlineSeconds: 0,
	}))

	second := e.pull(t, sub, true)
	require.Len(t, second, 1)
	assert.Equal(t, first[0].GetMessage().GetMessageId(), second[0].GetMessage().GetMessageId())
	assert.NotEqual(t, first[0].GetAckId(), second[0].GetAckId())

	assert.Equal(t, codes.InvalidArgument, status.Code(sa.ModifyAckDeadline(t.Context(),
		&pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: []string{"x"}, AckDeadlineSeconds: 601})))
}

func TestSDKSchemaTopic(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	sc, err := vkit.NewSchemaClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sc.Close() })

	def := `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},` +
		`{"name":"tag","type":["null","string"]}]}`

	created, err := sc.CreateSchema(t.Context(), &pubsubpb.CreateSchemaRequest{
		Parent: proj, SchemaId: "user", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
	})
	require.NoError(t, err)
	assert.Equal(t, proj+"/schemas/user", created.GetName())
	assert.Len(t, created.GetRevisionId(), 8)

	_, err = sc.CreateSchema(t.Context(), &pubsubpb.CreateSchemaRequest{
		Parent: proj, SchemaId: "user", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = sc.CreateSchema(t.Context(), &pubsubpb.CreateSchemaRequest{
		Parent:   proj,
		SchemaId: "broken",
		Schema:   &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: `{"type":"nope"}`},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = e.client.TopicAdminClient.CreateTopic(t.Context(), &pubsubpb.Topic{
		Name:           proj + "/topics/bad-schema",
		SchemaSettings: &pubsubpb.SchemaSettings{Schema: proj + "/schemas/ghost", Encoding: pubsubpb.Encoding_JSON},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	topic := e.topic(t, "users", func(tp *pubsubpb.Topic) {
		tp.SchemaSettings = &pubsubpb.SchemaSettings{Schema: created.GetName(), Encoding: pubsubpb.Encoding_JSON}
	})

	e.publish(t, topic, &pubsub.Message{Data: []byte(`{"id":1,"name":"ann","tag":{"string":"x"}}`)})

	_, err = e.client.TopicAdminClient.Publish(t.Context(), &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte(`{"id":"nope","name":"ann"}`)}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = sc.ValidateMessage(t.Context(), &pubsubpb.ValidateMessageRequest{
		Parent:     proj,
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Name{Name: created.GetName()},
		Message:    []byte(`{"id":2,"name":"bob"}`), Encoding: pubsubpb.Encoding_JSON,
	})
	require.NoError(t, err)

	rev2, err := sc.CommitSchema(t.Context(), &pubsubpb.CommitSchemaRequest{
		Name: created.GetName(), Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
	})
	require.NoError(t, err)
	assert.NotEqual(t, created.GetRevisionId(), rev2.GetRevisionId())

	revs := sc.ListSchemaRevisions(t.Context(), &pubsubpb.ListSchemaRevisionsRequest{Name: created.GetName()})
	first, err := revs.Next()
	require.NoError(t, err)
	assert.Equal(t, rev2.GetRevisionId(), first.GetRevisionId(), "newest first")

	basic, err := sc.GetSchema(
		t.Context(),
		&pubsubpb.GetSchemaRequest{Name: created.GetName(), View: pubsubpb.SchemaView_BASIC},
	)
	require.NoError(t, err)
	assert.Empty(t, basic.GetDefinition())

	require.NoError(t, sc.DeleteSchema(t.Context(), &pubsubpb.DeleteSchemaRequest{Name: created.GetName()}))

	_, err = sc.GetSchema(t.Context(), &pubsubpb.GetSchemaRequest{Name: created.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSDKPushSubscription(t *testing.T) {
	t.Parallel()

	type envelope struct {
		Message struct {
			Attributes map[string]string `json:"attributes"`
			Data       string            `json:"data"`
			MessageID  string            `json:"messageId"`
		} `json:"message"`
		Subscription string `json:"subscription"`
	}

	hits := make(chan envelope, 4)
	first := true

	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var env envelope
		_ = json.NewDecoder(r.Body).Decode(&env)

		mu.Lock()
		fail := first
		first = false
		mu.Unlock()

		if fail {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		hits <- env

		w.WriteHeader(http.StatusNoContent)
	}))

	t.Cleanup(srv.Close)

	e := newEnv(t)
	topic := e.topic(t, "push")
	sub := e.sub(t, "push-sub", topic, func(s *pubsubpb.Subscription) {
		s.PushConfig = &pubsubpb.PushConfig{PushEndpoint: srv.URL}
	})

	_, err := e.client.SubscriptionAdminClient.Pull(
		t.Context(),
		&pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1},
	)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	ids := e.publish(t, topic, &pubsub.Message{Data: []byte("pushed"), Attributes: map[string]string{"x": "y"}})

	select {
	case env := <-hits:
		raw, derr := base64.StdEncoding.DecodeString(env.Message.Data)
		require.NoError(t, derr)
		assert.Equal(t, "pushed", string(raw))
		assert.Equal(t, ids[0], env.Message.MessageID)
		assert.Equal(t, "y", env.Message.Attributes["x"])
		assert.Equal(t, sub, env.Subscription)
	case <-time.After(waitLimit):
		t.Fatal("push delivery did not arrive")
	}

	require.NoError(t, e.client.SubscriptionAdminClient.ModifyPushConfig(t.Context(),
		&pubsubpb.ModifyPushConfigRequest{Subscription: sub, PushConfig: &pubsubpb.PushConfig{}}))
	e.publish(t, topic, &pubsub.Message{Data: []byte("pull-now")})
	assert.Len(t, e.pull(t, sub, true), 1)
}

func TestSDKPersistence(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "persist", func(tp *pubsubpb.Topic) { tp.Labels = map[string]string{"a": "b"} })
	sub := e.sub(t, "persist-sub", topic, func(s *pubsubpb.Subscription) { s.Filter = `attributes:keep` })

	e.publish(t, topic,
		&pubsub.Message{Data: []byte("kept"), Attributes: map[string]string{"keep": "1"}},
		&pubsub.Message{Data: []byte("filtered")},
	)

	_, err := e.client.SubscriptionAdminClient.CreateSnapshot(t.Context(),
		&pubsubpb.CreateSnapshotRequest{Name: proj + "/snapshots/snap-ps", Subscription: sub})
	require.NoError(t, err)

	data := e.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	e2 := newEnv(t)
	require.NoError(t, e2.handler.Restore(t.Context(), data))

	got, err := e2.client.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	require.NoError(t, err)
	assert.Equal(t, "b", got.GetLabels()["a"])

	_, err = e2.client.SubscriptionAdminClient.GetSnapshot(
		t.Context(),
		&pubsubpb.GetSnapshotRequest{Snapshot: proj + "/snapshots/snap-ps"},
	)
	require.NoError(t, err)

	msgs := e2.pull(t, sub, true)
	require.Len(t, msgs, 1)
	assert.Equal(t, "kept", string(msgs[0].GetMessage().GetData()))

	e2.publish(t, topic, &pubsub.Message{Data: []byte("later"), Attributes: map[string]string{"keep": "1"}})
	assert.Len(t, e2.pull(t, sub, true), 1, "restored filter still applies to new messages")

	e2.handler.Reset()

	_, err = e2.client.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSDKStreamingFlowControl(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := e.topic(t, "flow")
	sub := e.sub(t, "flow-sub", topic)

	const n = 30

	msgs := make([]*pubsub.Message, n)
	for i := range msgs {
		msgs[i] = &pubsub.Message{Data: []byte("x")}
	}

	e.publish(t, topic, msgs...)

	ctx, cancel := context.WithTimeout(t.Context(), waitLimit)
	defer cancel()

	var (
		mu          sync.Mutex
		inflight    int
		maxInflight int
		done        int
	)

	s := e.client.Subscriber(sub)
	s.ReceiveSettings.MaxOutstandingMessages = 5

	require.NoError(t, s.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		mu.Lock()
		inflight++
		maxInflight = max(maxInflight, inflight)
		mu.Unlock()

		mu.Lock()
		inflight--
		done++
		finished := done == n
		mu.Unlock()

		m.Ack()

		if finished {
			cancel()
		}
	}))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, n, done)
	assert.LessOrEqual(t, maxInflight, 5)
}
