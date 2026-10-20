package integration_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newPubSubClient(t *testing.T) *pubsub.Client {
	t.Helper()

	client, err := pubsub.NewClient(t.Context(), gcpProject, gcpGRPCOpts(t, "8085")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func receiveOne(t *testing.T, client *pubsub.Client, subName string) *pubsub.Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var got *pubsub.Message

	require.NoError(t, client.Subscriber(subName).Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()

		got = m

		cancel()
	}))
	require.NotNil(t, got)

	return got
}

func TestIntegration_GCP_PubSub_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newPubSubClient(t)
	ctx := t.Context()
	proj := "projects/" + gcpProject
	topicName := proj + "/topics/" + gcpName("it-topic")
	subName := proj + "/subscriptions/" + gcpName("it-sub")

	_, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topicName, Labels: map[string]string{"env": "it"},
	})
	require.NoError(t, err)

	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: subName, Topic: topicName, AckDeadlineSeconds: 20,
	})
	require.NoError(t, err)

	topic, err := client.TopicAdminClient.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic:      &pubsubpb.Topic{Name: topicName, Labels: map[string]string{"env": "it2"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "it2", topic.GetLabels()["env"])

	pub := client.Publisher(topicName)
	defer pub.Stop()

	id, err := pub.Publish(ctx, &pubsub.Message{
		Data: []byte("payload"), Attributes: map[string]string{"k": "v"},
	}).Get(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	msg := receiveOne(t, client, subName)
	assert.Equal(t, "payload", string(msg.Data))
	assert.Equal(t, "v", msg.Attributes["k"])

	it := client.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + gcpProject})
	found := false

	for {
		tp, nerr := it.Next()
		if nerr != nil {
			break
		}

		found = found || tp.GetName() == topicName
	}

	assert.True(t, found)

	require.NoError(t, client.SubscriptionAdminClient.DeleteSubscription(ctx,
		&pubsubpb.DeleteSubscriptionRequest{Subscription: subName}))
	require.NoError(t, client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topicName}))
}

func TestIntegration_GCP_PubSub_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newPubSubClient(t)

	_, err := client.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{
		Topic: "projects/" + gcpProject + "/topics/" + gcpName("absent"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
