package gcpeventarc

import (
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// PubSub is the slice of the Pub/Sub emulator Eventarc uses as its event transport.
type PubSub interface {
	CreateTopic(in *pubsubpb.Topic) (*pubsubpb.Topic, error)
	GetTopic(name string) (*pubsubpb.Topic, error)
	DeleteTopic(name string) error
	CreateSubscription(in *pubsubpb.Subscription) (*pubsubpb.Subscription, error)
	DeleteSubscription(name string) error
	Publish(req *pubsubpb.PublishRequest) ([]string, error)
}

// Targets resolves the serving URL of a Cloud Run service, which also covers the service behind a Cloud
// Functions (2nd gen) function of the same id.
type Targets interface {
	ServiceURL(project, region, service string) (string, error)
}

// Services are the emulators Eventarc reads from and delivers to; absent ones stay nil.
type Services struct {
	PubSub  PubSub
	Targets Targets
}

// SetServices connects Eventarc to the other emulators.
func (b *InMemoryBackend) SetServices(s Services) {
	b.mu.Lock("SetServices")
	defer b.mu.Unlock()

	b.svc = s
}
