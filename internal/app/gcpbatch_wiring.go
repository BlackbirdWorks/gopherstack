package app

import (
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbatchbackend "github.com/blackbirdworks/gopherstack/services/gcpbatch"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

// wireGCPBatch connects Batch to Secret Manager (secret_variables) and Cloud Logging (CLOUD_LOGGING logs policy).
func wireGCPBatch(byName map[string]service.Registerable) {
	bt, ok := byName["GCPBatch"].(*gcpbatchbackend.Handler)
	if !ok {
		return
	}

	var svc gcpbatchbackend.Services

	if sm, isSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isSM {
		svc.Secrets = cloudBuildSecrets{b: sm.Backend}
	}

	if lg, isLG := byName["GCPLogging"].(*gcploggingbackend.Handler); isLG {
		svc.Logs = lg.Backend
	}

	bt.Backend.SetServices(svc)

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		bt.Backend.SetNotifier(batchNotifier{b: ps.Backend})
	}
}

// batchNotifier publishes Batch job notifications to the Pub/Sub emulator.
type batchNotifier struct {
	b *gcppubsubbackend.InMemoryBackend
}

func (n batchNotifier) PublishNotification(topic string, data []byte, attrs map[string]string) error {
	_, err := n.b.Publish(&pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: attrs}},
	})

	return err
}
