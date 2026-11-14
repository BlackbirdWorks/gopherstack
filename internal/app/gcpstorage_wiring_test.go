package app

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestGCPStorageNotificationsDeliverToPubSub(t *testing.T) {
	t.Parallel()

	gcs := gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend())
	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	byName := map[string]service.Registerable{"GCPStorage": gcs, "GCPPubSub": ps}

	wireGCPStorageSecurity(byName)

	const (
		topic = "projects/p/topics/events"
		sub   = "projects/p/subscriptions/events-sub"
	)

	_, err := ps.Backend.CreateTopic(&pubsubpb.Topic{Name: topic})
	require.NoError(t, err)

	_, err = ps.Backend.CreateSubscription(&pubsubpb.Subscription{Name: sub, Topic: topic})
	require.NoError(t, err)

	_, err = gcs.Backend.CreateBucket("p", gcpstoragebackend.Patch{"name": []byte(`"bkt"`)})
	require.NoError(t, err)

	_, err = gcs.Backend.AddNotification("bkt", gcpstoragebackend.Notification{
		Topic: "//pubsub.googleapis.com/" + topic, PayloadFormat: "JSON_API_V1",
	})
	require.NoError(t, err)

	_, err = gcs.Backend.PutObject("bkt", "k", []byte("v"), gcpstoragebackend.Patch{}, gcpstoragebackend.Preconds{})
	require.NoError(t, err)

	res, err := ps.SubscriberAPI().Pull(context.Background(), &pubsubpb.PullRequest{
		Subscription: sub, MaxMessages: 10, ReturnImmediately: true, //nolint:staticcheck // immediate poll in tests
	})
	require.NoError(t, err)
	require.Len(t, res.GetReceivedMessages(), 1)
	assert.Equal(t, "OBJECT_FINALIZE", res.GetReceivedMessages()[0].GetMessage().GetAttributes()["eventType"])
}

func TestGCPStorageSecurityWiringWithoutPeers(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() { wireGCPStorageSecurity(map[string]service.Registerable{}) })
	assert.NotPanics(t, func() {
		wireGCPStorageSecurity(map[string]service.Registerable{
			"GCPStorage": gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend()),
		})
	})
}
