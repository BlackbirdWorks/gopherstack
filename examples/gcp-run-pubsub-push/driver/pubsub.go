package main

import (
	"context"
	"fmt"
	"net/url"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func topicName() string { return "projects/" + project + "/topics/" + topicID }

func subName() string { return "projects/" + project + "/subscriptions/" + subID }

func newPubSub(ctx context.Context) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, project, grpcOpts(env("PUBSUB_ADDR", "localhost:8085"))...)
}

func notFound(err error) bool { return status.Code(err) == codes.NotFound }

func resetTopic(ctx context.Context, ps *pubsub.Client) error {
	err := ps.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: subName()})
	if err != nil && !notFound(err) {
		return err
	}

	err = ps.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topicName()})
	if err != nil && !notFound(err) {
		return err
	}

	return nil
}

// wirePush creates the topic and a push subscription targeting the service's ingress URL.
func wirePush(ctx context.Context, serviceURI string) (string, error) {
	u, err := url.Parse(serviceURI)
	if err != nil {
		return "", err
	}

	endpoint := "http://" + u.Host + "/push"

	ps, err := newPubSub(ctx)
	if err != nil {
		return "", err
	}
	defer ps.Close()

	if err = resetTopic(ctx, ps); err != nil {
		return "", err
	}

	if _, err = ps.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName()}); err != nil {
		return "", err
	}

	_, err = ps.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: subName(), Topic: topicName(),
		PushConfig: &pubsubpb.PushConfig{PushEndpoint: endpoint},
	})

	return endpoint, err
}

func publish(ctx context.Context, n int) error {
	ps, err := newPubSub(ctx)
	if err != nil {
		return err
	}
	defer ps.Close()

	pub := ps.Publisher(topicName())
	defer pub.Stop()

	for i := 1; i <= n; i++ {
		res := pub.Publish(ctx, &pubsub.Message{
			Data:       []byte(fmt.Sprintf("order-%d", i)),
			Attributes: map[string]string{"source": "demo"},
		})

		id, gerr := res.Get(ctx)
		if gerr != nil {
			return gerr
		}

		fmt.Printf("published order-%d id=%s\n", i, id)
	}

	return nil
}
