package gcpeventarc

import (
	"context"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// PubSub is the slice of the Pub/Sub emulator Eventarc uses as its event transport.
type PubSub interface {
	CreateTopic(in *pubsubpb.Topic) (*pubsubpb.Topic, error)
	GetTopic(name string) (*pubsubpb.Topic, error)
	DeleteTopic(name string) error
	CreateSubscription(in *pubsubpb.Subscription) (*pubsubpb.Subscription, error)
	DeleteSubscription(name string) error
	UpdateSubscription(req *pubsubpb.UpdateSubscriptionRequest) (*pubsubpb.Subscription, error)
	Publish(req *pubsubpb.PublishRequest) ([]string, error)
}

// Targets resolves the serving URL of a Cloud Run service, which also covers the service behind a Cloud
// Functions (2nd gen) function of the same id.
type Targets interface {
	ServiceURL(project, region, service string) (string, error)
}

// Workflows starts workflow executions for workflow destinations.
type Workflows interface {
	WorkflowExists(name string) bool
	StartExecution(ctx context.Context, workflow string, argument []byte) error
}

// Buckets resolves the location of a Cloud Storage bucket.
type Buckets interface {
	BucketLocation(bucket string) (string, bool)
	BucketProject(bucket string) (string, bool)
}

// Organization places projects under the organization an organization subscription covers.
type Organization interface {
	ProjectInOrganization(project string) (in, known bool)
}

// Clusters checks the GKE clusters of GKE destinations.
type Clusters interface {
	ClusterExists(name string) bool
}

// Services are the emulators Eventarc reads from and delivers to; absent ones stay nil.
type Services struct {
	PubSub    PubSub
	Targets   Targets
	Workflows Workflows
	Buckets   Buckets
	Org       Organization
	Clusters  Clusters
}

// SetServices connects Eventarc to the other emulators.
func (b *InMemoryBackend) SetServices(s Services) {
	b.mu.Lock("SetServices")
	b.svc = s
	b.mu.Unlock()

	b.rebindTransports()
}
