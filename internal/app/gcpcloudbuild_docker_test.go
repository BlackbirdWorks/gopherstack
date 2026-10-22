package app

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"log/slog"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcparbackend "github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const buildImage = "alpine:3.20"

type buildEnv struct {
	cb      *gcpcloudbuildbackend.Handler
	gcs     *gcpstoragebackend.InMemoryBackend
	logs    *gcploggingbackend.InMemoryBackend
	ps      *gcppubsubbackend.InMemoryBackend
	ar      *gcparbackend.Handler
	statusM *statusRecorder
	arPort  int
}

// statusRecorder notes the status of every build message the real Pub/Sub publisher is given.
type statusRecorder struct {
	pub      cloudBuildPublisher
	statuses []string
	mu       sync.Mutex
}

func (r *statusRecorder) Publish(project, topic string, data []byte, attrs map[string]string) error {
	r.mu.Lock()
	r.statuses = append(r.statuses, attrs["status"])
	r.mu.Unlock()

	return r.pub.Publish(project, topic, data, attrs)
}

func (r *statusRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.statuses...)
}

// newBuildEnv wires Cloud Build to the other GCP emulators the way production does and runs steps in a real
// container runtime. It skips the test when no runtime or image is available.
func newBuildEnv(t *testing.T) *buildEnv {
	t.Helper()

	if testing.Short() {
		t.Skip("docker-backed Cloud Build tests are skipped in short mode")
	}

	rt, err := container.NewRuntime(container.Config{Logger: slog.Default()})
	if err != nil {
		t.Skipf("no container runtime: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container runtime is not reachable: %v", err)
	}

	if err = rt.PullImage(t.Context(), buildImage); err != nil {
		t.Skipf("cannot pull %s: %v", buildImage, err)
	}

	e := &buildEnv{
		gcs:  gcpstoragebackend.NewInMemoryBackend(),
		logs: gcploggingbackend.NewInMemoryBackend(),
		ps:   gcppubsubbackend.NewInMemoryBackend(),
		ar:   gcparbackend.NewHandler(gcparbackend.NewInMemoryBackend()),
	}

	e.ar.Port = 0
	require.NoError(t, e.ar.StartWorker(t.Context()))
	t.Cleanup(func() { e.ar.Shutdown(context.Background()) })

	_, port, err := net.SplitHostPort(e.ar.Addr())
	require.NoError(t, err)

	e.arPort, err = strconv.Atoi(port)
	require.NoError(t, err)
	e.ar.Port = e.arPort

	backend := gcpcloudbuildbackend.NewInMemoryBackend()
	backend.SetContext(t.Context())
	backend.SetWorkDir(t.TempDir())
	backend.SetRuntime(rt)

	e.cb = gcpcloudbuildbackend.NewHandler(backend)
	t.Cleanup(backend.Close)

	byName := map[string]service.Registerable{
		"GCPCloudBuild": e.cb,
		"GCPStorage":    gcpstoragebackend.NewHandler(e.gcs),
		"GCPPubSub":     gcppubsubbackend.NewHandler(e.ps),
		"GCPLogging":    gcploggingbackend.NewHandler(e.logs),
		"GCPSecretManager": gcpsecretmanagerbackend.NewHandler(
			gcpsecretmanagerbackend.NewInMemoryBackend(),
		),
		"GCPArtifactRegistry": e.ar,
	}

	svc := cloudBuildServices(byName)
	e.statusM = &statusRecorder{
		pub: svc.Pub.(cloudBuildPublisher),
	} //nolint:forcetypeassert // built by cloudBuildServices above
	svc.Pub = e.statusM
	backend.SetServices(svc)

	return e
}

func sourceArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, body := range files {
		require.NoError(
			t,
			tw.WriteHeader(
				&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg},
			),
		)
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

func shStep(id, script string, wait ...string) *cloudbuildpb.BuildStep {
	return &cloudbuildpb.BuildStep{
		Name:       buildImage,
		Id:         id,
		Entrypoint: "sh",
		Args:       []string{"-c", script},
		WaitFor:    wait,
	}
}

func (e *buildEnv) create(t *testing.T, project string, b *cloudbuildpb.Build) *cloudbuildpb.Build {
	t.Helper()

	op, err := e.cb.Backend.CreateBuild(&cloudbuildpb.CreateBuildRequest{ProjectId: project, Build: b})
	require.NoError(t, err)

	meta := &cloudbuildpb.BuildOperationMetadata{}
	require.NoError(t, op.GetMetadata().UnmarshalTo(meta))

	return meta.GetBuild()
}

func (e *buildEnv) finished(t *testing.T, b *cloudbuildpb.Build) *cloudbuildpb.Build {
	t.Helper()

	e.cb.Backend.Wait()

	got, err := e.cb.Backend.GetBuild(&cloudbuildpb.GetBuildRequest{Name: b.GetName()})
	require.NoError(t, err)

	return got
}

func (e *buildEnv) logLines(t *testing.T, project string) []string {
	t.Helper()

	entries, _, err := e.logs.ListLogEntries(&loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{
			"projects/" + project,
		}, Filter: `logName="projects/` + project + `/logs/cloudbuild"`,
	})
	require.NoError(t, err)

	out := make([]string, len(entries))
	for i, en := range entries {
		out[i] = en.GetTextPayload()
	}

	return out
}

func TestGCPCloudBuild_Docker_StepsSourceArtifacts(t *testing.T) {
	t.Parallel()

	e := newBuildEnv(t)

	const project = "cb-docker"

	_, err := e.gcs.CreateBucket(project, gcpstoragebackend.Patch{"name": []byte(`"cb-src"`)})
	require.NoError(t, err)
	_, err = e.gcs.CreateBucket(project, gcpstoragebackend.Patch{"name": []byte(`"cb-out"`)})
	require.NoError(t, err)
	_, err = e.gcs.PutObject("cb-src", "src.tgz", sourceArchive(t, map[string]string{"input.txt": "seed"}),
		gcpstoragebackend.Patch{}, gcpstoragebackend.Preconds{})
	require.NoError(t, err)

	started := e.create(t, project, &cloudbuildpb.Build{
		Source: &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSource{
			StorageSource: &cloudbuildpb.StorageSource{Bucket: "cb-src", Object: "src.tgz"},
		}},
		Substitutions: map[string]string{"_WHO": "world"},
		Steps: []*cloudbuildpb.BuildStep{
			shStep("a", "echo a-$$PWD-$_WHO > a.txt; echo from-a", "-"),
			shStep("b", "echo b > b.txt; echo from-b", "-"),
			shStep("c", "cat input.txt a.txt b.txt > out.txt; echo project=$PROJECT_ID; ls", "a", "b"),
		},
		Artifacts: &cloudbuildpb.Artifacts{Objects: &cloudbuildpb.Artifacts_ArtifactObjects{
			Location: "gs://cb-out/results/", Paths: []string{"out.txt"},
		}},
		LogsBucket: "gs://cb-out",
	})
	assert.Equal(t, cloudbuildpb.Build_QUEUED, started.GetStatus())

	got := e.finished(t, started)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	out, err := e.gcs.GetObject("cb-out", "results/out.txt", 0, gcpstoragebackend.Preconds{})
	require.NoError(t, err)
	assert.Equal(t, "seeda-/workspace-world\nb\n", string(out.Data))

	_, err = e.gcs.GetObject(
		"cb-out",
		"results/artifacts-"+got.GetId()+".json",
		0,
		gcpstoragebackend.Preconds{},
	)
	require.NoError(t, err)

	logObj, err := e.gcs.GetObject("cb-out", "log-"+got.GetId()+".txt", 0, gcpstoragebackend.Preconds{})
	require.NoError(t, err)
	assert.Contains(t, string(logObj.Data), `Step #2 - "c": project=`+project)

	lines := e.logLines(t, project)
	assert.Contains(t, lines, `Step #0 - "a": from-a`)
	assert.Contains(t, lines, `Step #2 - "c": project=`+project)
	assert.Contains(t, lines, `Step #2 - "c": input.txt`)

	assert.Equal(t, []string{"QUEUED", "WORKING", "SUCCESS"}, e.statusM.seen())
}

func TestGCPCloudBuild_Docker_FailureCancelTimeout(t *testing.T) {
	t.Parallel()

	e := newBuildEnv(t)

	const project = "cb-docker-ends"

	failing := e.create(t, project, &cloudbuildpb.Build{Steps: []*cloudbuildpb.BuildStep{
		shStep("ok", "echo fine"), shStep("bad", "exit 7"), shStep("never", "echo unreachable"),
	}})
	got := e.finished(t, failing)
	assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
	assert.Equal(
		t,
		`build step 1 "`+buildImage+`" failed: step exited with non-zero status: 7`,
		got.GetStatusDetail(),
	)
	assert.Equal(t, cloudbuildpb.Build_CANCELLED, got.GetSteps()[2].GetStatus())
	assert.EqualValues(t, 7, got.GetSteps()[1].GetExitCode())

	slow := e.create(
		t,
		project,
		&cloudbuildpb.Build{Steps: []*cloudbuildpb.BuildStep{shStep("slow", "sleep 300")}},
	)
	waitForStatus(t, e, slow, cloudbuildpb.Build_WORKING)

	cancelled, err := e.cb.Backend.CancelBuild(&cloudbuildpb.CancelBuildRequest{Name: slow.GetName()})
	require.NoError(t, err)
	assert.Equal(t, cloudbuildpb.Build_CANCELLED, cancelled.GetStatus())
	assert.Equal(t, cloudbuildpb.Build_CANCELLED, e.finished(t, slow).GetStatus())

	timed := e.create(t, project, &cloudbuildpb.Build{
		Timeout: durationpb.New(2 * time.Second),
		Steps:   []*cloudbuildpb.BuildStep{shStep("slow", "sleep 300")},
	})
	got = e.finished(t, timed)
	assert.Equal(t, cloudbuildpb.Build_TIMEOUT, got.GetStatus(), got.GetStatusDetail())
}

// waitForStatus blocks until the build reaches want or ends.
func waitForStatus(t *testing.T, e *buildEnv, b *cloudbuildpb.Build, want cloudbuildpb.Build_Status) {
	t.Helper()

	require.Eventually(t, func() bool {
		got, err := e.cb.Backend.GetBuild(&cloudbuildpb.GetBuildRequest{Name: b.GetName()})

		return err == nil && got.GetStatus() == want &&
			got.GetSteps()[0].GetStatus() == cloudbuildpb.Build_WORKING
	}, 2*time.Minute, 100*time.Millisecond)
}

func TestGCPCloudBuild_Docker_PushImageToArtifactRegistry(t *testing.T) {
	t.Parallel()

	e := newBuildEnv(t)

	const (
		project = "cb-docker-push"
		repo    = "projects/" + project + "/locations/us-central1/repositories/images"
	)

	op, err := e.ar.Backend.CreateRepository(&artifactregistrypb.CreateRepositoryRequest{
		Parent: "projects/" + project + "/locations/us-central1", RepositoryId: "images",
		Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER},
	})
	require.NoError(t, err)
	require.True(t, op.GetDone())

	image := "us-central1-docker.pkg.dev/" + project + "/images/app:v1"
	require.NoError(t, exec.CommandContext(t.Context(), "docker", "tag", buildImage, image).Run())
	t.Cleanup(func() { _ = exec.CommandContext(context.Background(), "docker", "rmi", image).Run() })

	started := e.create(t, project, &cloudbuildpb.Build{
		Steps:  []*cloudbuildpb.BuildStep{shStep("noop", "echo built")},
		Images: []string{image},
	})
	got := e.finished(t, started)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
	require.Len(t, got.GetResults().GetImages(), 1)
	assert.Equal(t, image, got.GetResults().GetImages()[0].GetName())
	assert.Contains(t, got.GetResults().GetImages()[0].GetDigest(), "sha256:")

	imgs, _, err := e.ar.Backend.ListDockerImages(
		t.Context(),
		&artifactregistrypb.ListDockerImagesRequest{Parent: repo},
	)
	require.NoError(t, err)
	require.Len(t, imgs, 1)
	assert.Equal(t, []string{"v1"}, imgs[0].GetTags())
	assert.Contains(t, imgs[0].GetName(), got.GetResults().GetImages()[0].GetDigest())
}
