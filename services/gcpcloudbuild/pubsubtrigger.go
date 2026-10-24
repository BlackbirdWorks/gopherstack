package gcpcloudbuild

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// HandlePubsubMessage starts a build for every enabled pubsub_config trigger subscribed to topic
// ("projects/p/topics/t"). The trigger's $(body...) bindings see the message as Pub/Sub delivers it to a push
// endpoint: {"message": {"data": <base64>, "attributes": {...}, "messageId", "publishTime"}, "subscription"}.
func (b *InMemoryBackend) HandlePubsubMessage(
	topic string, data []byte, attrs map[string]string, id string, published time.Time,
) {
	b.mu.RLock("HandlePubsubMessage")

	var hits []triggerRef

	for _, t := range b.triggers {
		if t.GetPubsubConfig().GetTopic() != topic {
			continue
		}

		if r, err := parseName(t.GetResourceName(), "triggers"); err == nil {
			hits = append(hits, triggerRef{r: r, t: cloneTrigger(t)})
		}
	}

	b.mu.RUnlock()

	for _, h := range hits {
		payload, err := pubsubPayload(h.t.GetPubsubConfig().GetSubscription(), data, attrs, id, published)
		if err == nil {
			err = b.fireEvent(h.r, h.t, payload)
		}

		if err != nil {
			logger.Load(b.ctx).WarnContext(b.ctx, "gcpcloudbuild: pubsub trigger did not start a build",
				"trigger", h.t.GetId(), "topic", topic, "error", err)
		}
	}
}

type triggerRef struct {
	t *cloudbuildpb.BuildTrigger
	r ref
}

func pubsubPayload(
	subscription string, data []byte, attrs map[string]string, id string, published time.Time,
) ([]byte, error) {
	if attrs == nil {
		attrs = map[string]string{}
	}

	return json.Marshal(map[string]any{
		"message": map[string]any{
			"data":        base64.StdEncoding.EncodeToString(data),
			"attributes":  attrs,
			"messageId":   id,
			"publishTime": published.UTC().Format(time.RFC3339Nano),
		},
		"subscription": subscription,
	})
}
