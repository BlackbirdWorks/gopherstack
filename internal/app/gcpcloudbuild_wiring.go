package app

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcparbackend "github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const bucketPatchKey = "name"

// cloudBuildStore gives Cloud Build source, artifacts and logs access to the GCS emulator.
type cloudBuildStore struct {
	gcsObjectStore
}

func (s cloudBuildStore) EnsureBucket(project, bucket string) error {
	patch := gcpstoragebackend.Patch{}
	patch[bucketPatchKey] = []byte(strconv.Quote(bucket))

	_, err := s.b.CreateBucket(project, patch)

	var ae *gcpstoragebackend.APIError
	if err != nil && (!errors.As(err, &ae) || ae.Status != http.StatusConflict) {
		return fmt.Errorf("create bucket %s: %w", bucket, err)
	}

	return nil
}

// cloudBuildPublisher publishes build status messages to the project's cloud-builds topic, creating it on demand.
type cloudBuildPublisher struct {
	b *gcppubsubbackend.InMemoryBackend
}

func (p cloudBuildPublisher) Publish(project, topic string, data []byte, attrs map[string]string) error {
	name := "projects/" + project + "/topics/" + topic

	if _, err := p.b.GetTopic(name); err != nil {
		if _, err = p.b.CreateTopic(&pubsubpb.Topic{Name: name}); err != nil {
			return fmt.Errorf("create topic %s: %w", name, err)
		}
	}

	return logSinkPublisher(p).Publish(name, data, attrs)
}

type cloudBuildSecrets struct {
	b *gcpsecretmanagerbackend.InMemoryBackend
}

func (s cloudBuildSecrets) AccessSecret(versionName string) ([]byte, error) {
	res, err := s.b.AccessSecretVersion(versionName)
	if err != nil {
		return nil, fmt.Errorf("access secret version: %w", err)
	}

	return res.GetPayload().GetData(), nil
}

// cloudBuildServices resolves the emulators Cloud Build reads from and writes to; absent ones stay nil.
func cloudBuildServices(byName map[string]service.Registerable) gcpcloudbuildbackend.Services {
	var svc gcpcloudbuildbackend.Services

	if gcs, ok := byName["GCPStorage"].(*gcpstoragebackend.Handler); ok {
		svc.Store = cloudBuildStore{gcsObjectStore{b: gcs.Backend}}
	}

	if ps, ok := byName["GCPPubSub"].(*gcppubsubbackend.Handler); ok {
		svc.Pub = cloudBuildPublisher{b: ps.Backend}
	}

	if lg, ok := byName["GCPLogging"].(*gcploggingbackend.Handler); ok {
		svc.Logs = lg.Backend
	}

	if sm, ok := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); ok {
		svc.Secrets = cloudBuildSecrets{b: sm.Backend}
	}

	if ar, ok := byName["GCPArtifactRegistry"].(*gcparbackend.Handler); ok {
		svc.RegistryAddr = "localhost:" + strconv.Itoa(ar.Port)
	}

	return svc
}

// wireGCPStorageConsumers wires the GCP emulators that exchange objects with the GCS emulator.
func wireGCPStorageConsumers(byName map[string]service.Registerable) {
	wireGCPRedis(byName)
	wireGCPCloudBuild(byName)
	wireGCPEventarc(byName)
	wireGCPSecretRotation(byName)
	wireGCPWorkflows(byName)
	wireGCPRun(byName)
	wireGCPContainer(byName)
	wireGCPIdentityToolkit(byName)
	wireGCPBatch(byName)
}

// wireGCPCloudBuild connects Cloud Build to Cloud Storage, Pub/Sub, Cloud Logging, Secret Manager and the
// Artifact Registry emulator, and starts pubsub_config triggers from messages published to Pub/Sub.
func wireGCPCloudBuild(byName map[string]service.Registerable) {
	if cb, ok := byName["GCPCloudBuild"].(*gcpcloudbuildbackend.Handler); ok {
		cb.Backend.SetServices(cloudBuildServices(byName))

		if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
			ps.Backend.ObservePublishes(func(topic string, m *pubsubpb.PubsubMessage) {
				cb.Backend.HandlePubsubMessage(
					topic, m.GetData(), m.GetAttributes(), m.GetMessageId(), m.GetPublishTime().AsTime(),
				)
			})
		}
	}
}

// wireGCPSecretRotation delivers Secret Manager SECRET_ROTATE notifications to the Pub/Sub emulator.
func wireGCPSecretRotation(byName map[string]service.Registerable) {
	sm, okSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler)
	ps, okPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler)

	if okSM && okPS {
		sm.Backend.SetPublisher(logSinkPublisher{b: ps.Backend})
	}
}
