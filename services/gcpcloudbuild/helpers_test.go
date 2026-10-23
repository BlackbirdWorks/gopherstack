package gcpcloudbuild_test

import (
	"context"
	"testing"
	"time"

	cloudbuild "cloud.google.com/go/cloudbuild/apiv1/v2"
	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const (
	project = "test-project"
	parent  = "projects/" + project + "/locations/global"
)

type env struct {
	client  *cloudbuild.Client
	backend *gcpcloudbuild.InMemoryBackend
	handler *gcpcloudbuild.Handler
	rt      *fakeRuntime
	store   *fakeStore
	pub     *fakePublisher
	logs    *fakeLogs
	timers  *manualTimers
	svc     gcpcloudbuild.Services
	opts    []option.ClientOption
}

// newEnv starts a Cloud Build listener whose steps run on the fake runtime.
func newEnv(t *testing.T, behaviors map[string]behavior) *env {
	t.Helper()

	e := &env{
		rt: newFakeRuntime(
			behaviors,
		), store: newFakeStore(), pub: &fakePublisher{}, logs: &fakeLogs{},
		timers: &manualTimers{},
	}

	b := gcpcloudbuild.NewInMemoryBackend()
	b.SetContext(t.Context())
	b.SetWorkDir(t.TempDir())
	b.SetRuntime(e.rt)
	b.SetClock(time.Now, e.timers.start)
	e.svc = gcpcloudbuild.Services{
		Store:        e.store,
		Pub:          e.pub,
		Logs:         e.logs,
		RegistryAddr: "localhost:8134",
		Secrets: fakeSecrets{
			"projects/p/secrets/s/versions/1":    "hunter2",
			"projects/p/secrets/hook/versions/1": "whsec",
		},
	}
	b.SetServices(e.svc)

	h := gcpcloudbuild.NewHandler(b)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e.backend, e.handler = b, h
	e.opts = []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	client, err := cloudbuild.NewClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	e.client = client

	return e
}

func step(name string, mut ...func(*cloudbuildpb.BuildStep)) *cloudbuildpb.BuildStep {
	s := &cloudbuildpb.BuildStep{Name: name}
	for _, m := range mut {
		m(s)
	}

	return s
}

func withID(id string) func(*cloudbuildpb.BuildStep) {
	return func(s *cloudbuildpb.BuildStep) { s.Id = id }
}

func waitFor(ids ...string) func(*cloudbuildpb.BuildStep) {
	return func(s *cloudbuildpb.BuildStep) { s.WaitFor = ids }
}

func withArgs(args ...string) func(*cloudbuildpb.BuildStep) {
	return func(s *cloudbuildpb.BuildStep) { s.Args = args }
}

func buildOf(steps ...*cloudbuildpb.BuildStep) *cloudbuildpb.Build {
	return &cloudbuildpb.Build{Steps: steps}
}

// run queues a build and waits for it to reach a terminal status.
func (e *env) run(t *testing.T, b *cloudbuildpb.Build) *cloudbuildpb.Build {
	t.Helper()

	op, err := e.client.CreateBuild(
		t.Context(),
		&cloudbuildpb.CreateBuildRequest{Parent: parent, Build: b},
	)
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	e.backend.Wait()

	return got
}

func (e *env) get(t *testing.T, name string) *cloudbuildpb.Build {
	t.Helper()

	got, err := e.client.GetBuild(t.Context(), &cloudbuildpb.GetBuildRequest{Name: name})
	require.NoError(t, err)

	return got
}

func timeout(d time.Duration) *durationpb.Duration { return durationpb.New(d) }

// configure changes the backend services after newEnv.
func (e *env) configure(mut func(*gcpcloudbuild.Services)) {
	mut(&e.svc)
	e.backend.SetServices(e.svc)
}
