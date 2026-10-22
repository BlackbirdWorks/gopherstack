package gcpcloudbuild

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const (
	reasonNoRuntime = "no container runtime is available to run build steps; start Docker or Podman and restart"
	reasonDisabled  = "build execution is disabled (engine=none); builds are recorded and then failed"
	buildsTopic     = "cloud-builds"
	pubsubTimeout   = 5 * time.Second
)

// Runtime is the slice of the container runtime a build needs. The runtime must also implement
// container.ExitWaiter; it should implement container.LogStreamer (step output) and container.ImagePusher
// (images:).
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// ObjectStore reads and writes Cloud Storage objects in the emulator.
type ObjectStore interface {
	ReadObject(bucket, object string) ([]byte, error)
	WriteObject(bucket, object string, data []byte, contentType string) error
	// EnsureBucket creates bucket under project when it does not exist.
	EnsureBucket(project, bucket string) error
}

// Publisher publishes a message to a Pub/Sub topic of a project, creating the topic when missing.
type Publisher interface {
	Publish(project, topic string, data []byte, attrs map[string]string) error
}

// SecretResolver reads a Secret Manager secret version ("projects/p/secrets/s/versions/v").
type SecretResolver interface {
	AccessSecret(versionName string) ([]byte, error)
}

// Services are the emulator backends a build reads from and writes to.
type Services struct {
	Store   ObjectStore
	Pub     Publisher
	Secrets SecretResolver
	Logs    gcpserve.LogSink
	// RegistryAddr is the Artifact Registry emulator's host:port as the docker daemon reaches it.
	RegistryAddr string
	// GitImage runs git for gitSource and repoSource; empty uses DefaultGitImage.
	GitImage string
	// RepoBase is the base URL (http, https or file) repoSource repo_names resolve under as
	// <RepoBase>/<project>/<repo>; empty uses Cloud Source Repositories' address.
	RepoBase string
}

// timerFunc starts a timer; the channel fires after d and stop cancels it.
type timerFunc func(d time.Duration) (<-chan time.Time, func() bool)

func realTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)

	return t.C, t.Stop
}
