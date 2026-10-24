package app

import (
	"log/slog"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcparbackend "github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestCloudBuildStore(t *testing.T) {
	t.Parallel()

	gcs := gcpstoragebackend.NewInMemoryBackend()
	store := cloudBuildStore{gcsObjectStore{b: gcs}}

	require.NoError(t, store.EnsureBucket("p", "logs-bucket"))
	require.NoError(t, store.EnsureBucket("p", "logs-bucket"), "creating an existing bucket is fine")
	require.Error(t, store.EnsureBucket("p", "Bad Name"))

	require.NoError(t, store.WriteObject("logs-bucket", "a/b.txt", []byte("data"), "text/plain"))

	got, err := store.ReadObject("logs-bucket", "a/b.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got)

	require.Error(t, store.WriteObject("nope", "x", nil, ""))
}

func TestCloudBuildPublisher(t *testing.T) {
	t.Parallel()

	ps := gcppubsubbackend.NewInMemoryBackend()
	pub := cloudBuildPublisher{b: ps}

	require.NoError(
		t,
		pub.Publish("p", "cloud-builds", []byte(`{"id":"b1"}`), map[string]string{"status": "QUEUED"}),
	)
	require.NoError(
		t,
		pub.Publish("p", "cloud-builds", []byte(`{"id":"b1"}`), map[string]string{"status": "WORKING"}),
		"topic is reused",
	)

	topic, err := ps.GetTopic("projects/p/topics/cloud-builds")
	require.NoError(t, err)
	assert.Equal(t, "projects/p/topics/cloud-builds", topic.GetName())

	_, err = ps.CreateSubscription(
		&pubsubpb.Subscription{Name: "projects/p/subscriptions/sub1", Topic: topic.GetName()},
	)
	require.NoError(t, err)
	require.NoError(
		t,
		pub.Publish("p", "cloud-builds", []byte(`{"id":"b1"}`), map[string]string{"status": "SUCCESS"}),
	)
}

func TestCloudBuildSecrets(t *testing.T) {
	t.Parallel()

	sm := gcpsecretmanagerbackend.NewInMemoryBackend()
	_, err := sm.CreateSecret(&secretmanagerpb.CreateSecretRequest{
		Parent: "projects/p", SecretId: "s", Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{
				Automatic: &secretmanagerpb.Replication_Automatic{},
			}},
		},
	})
	require.NoError(t, err)

	_, err = sm.AddSecretVersion(&secretmanagerpb.AddSecretVersionRequest{
		Parent: "projects/p/secrets/s", Payload: &secretmanagerpb.SecretPayload{Data: []byte("v")},
	})
	require.NoError(t, err)

	res := cloudBuildSecrets{b: sm}

	got, err := res.AccessSecret("projects/p/secrets/s/versions/1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)

	_, err = res.AccessSecret("projects/p/secrets/s/versions/9")
	require.Error(t, err)
}

func TestWireGCPCloudBuild(t *testing.T) {
	t.Parallel()

	cb := gcpcloudbuildbackend.NewHandler(gcpcloudbuildbackend.NewInMemoryBackend())
	t.Cleanup(cb.Backend.Close)

	assert.NotPanics(t, func() { wireGCPCloudBuild(map[string]service.Registerable{}) })
	assert.NotPanics(t, func() { wireGCPCloudBuild(map[string]service.Registerable{"GCPCloudBuild": cb}) })
	assert.NotPanics(t, func() {
		wireGCPCloudBuild(map[string]service.Registerable{
			"GCPCloudBuild": cb,
			"GCPStorage":    gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend()),
			"GCPPubSub":     gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend()),
			"GCPSecretManager": gcpsecretmanagerbackend.NewHandler(
				gcpsecretmanagerbackend.NewInMemoryBackend(),
			),
			"GCPArtifactRegistry": gcparbackend.NewHandler(gcparbackend.NewInMemoryBackend()),
		})
	})
}

func TestReserveFixedServicePorts_GCPCloudBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", 8100, 8200, true},
		{"port outside pool range", 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{GCPCloudBuild: gcpcloudbuildbackend.Settings{Port: gcpcloudbuildbackend.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(gcpcloudbuildbackend.DefaultPort))
		})
	}
}

func TestWireGCPCloudBuild_PubsubTrigger(t *testing.T) {
	t.Parallel()

	cb := gcpcloudbuildbackend.NewHandler(gcpcloudbuildbackend.NewInMemoryBackend())
	t.Cleanup(cb.Backend.Close)

	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	wireGCPCloudBuild(map[string]service.Registerable{"GCPCloudBuild": cb, "GCPPubSub": ps})

	const topic = "projects/p/topics/deploys"

	_, err := ps.Backend.CreateTopic(&pubsubpb.Topic{Name: topic})
	require.NoError(t, err)

	trigger, err := cb.Backend.CreateBuildTrigger(&cloudbuildpb.CreateBuildTriggerRequest{
		Parent: "projects/p/locations/global",
		Trigger: &cloudbuildpb.BuildTrigger{
			Name:         "on-deploy",
			PubsubConfig: &cloudbuildpb.PubsubConfig{Topic: topic},
			BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: &cloudbuildpb.Build{
				Steps: []*cloudbuildpb.BuildStep{{Name: "img"}},
			}},
		},
	})
	require.NoError(t, err)

	_, err = ps.Backend.Publish(&pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("go")}},
	})
	require.NoError(t, err)
	cb.Backend.Wait()

	builds, _, err := cb.Backend.ListBuilds(&cloudbuildpb.ListBuildsRequest{Parent: "projects/p/locations/global"})
	require.NoError(t, err)
	require.Len(t, builds, 1)
	assert.Equal(t, trigger.GetId(), builds[0].GetBuildTriggerId())
}
