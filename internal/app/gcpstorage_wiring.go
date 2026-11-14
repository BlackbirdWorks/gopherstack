package app

import (
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpiambackend "github.com/blackbirdworks/gopherstack/services/gcpiam"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type gcsPubSubNotifier struct {
	ps *gcppubsubbackend.InMemoryBackend
}

func (n gcsPubSubNotifier) TopicExists(topic string) bool {
	_, err := n.ps.GetTopic(topic)

	return err == nil
}

func (n gcsPubSubNotifier) Publish(topic string, data []byte, attrs map[string]string) error {
	_, err := n.ps.Publish(&pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: attrs}},
	})

	return err
}

// wireGCPStorageSecurity gives Cloud Storage Pub/Sub delivery for bucket notifications and Cloud IAM service
// account keys for verifying signed URLs.
func wireGCPStorageSecurity(byName map[string]service.Registerable) {
	gcs, ok := byName["GCPStorage"].(*gcpstoragebackend.Handler)
	if !ok {
		return
	}

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		gcs.Backend.SetNotifier(gcsPubSubNotifier{ps: ps.Backend})
	}

	if iam, isIAM := byName["GCPIAM"].(*gcpiambackend.Handler); isIAM {
		gcs.Backend.SetKeyResolver(iam.Backend.VerificationKeys)
	}
}
