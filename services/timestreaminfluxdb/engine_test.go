package timestreaminfluxdb_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

var (
	errPull = errors.New("image pull denied")
	errBoom = errors.New("boom")
)

type fakeEngine struct {
	startErr  error
	backupErr error
	gate      chan struct{}
	specs     []timestreaminfluxdb.EngineSpec
	recreated []timestreaminfluxdb.EngineSpec
	stopped   []string
	restarted []string
	backups   []string
	logs      []byte
	mu        sync.Mutex
	next      atomic.Int32
}

func (e *fakeEngine) Restart(
	_ context.Context, h timestreaminfluxdb.EngineHandle,
) (timestreaminfluxdb.EngineHandle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.restarted = append(e.restarted, h.ContainerID)

	return h, nil
}

func (e *fakeEngine) Recreate(
	_ context.Context, h timestreaminfluxdb.EngineHandle, spec timestreaminfluxdb.EngineSpec,
) (timestreaminfluxdb.EngineHandle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.recreated = append(e.recreated, spec)
	h.ContainerID += "+"

	return h, nil
}

func (e *fakeEngine) Backup(_ context.Context, h timestreaminfluxdb.EngineHandle, token, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.backups = append(e.backups, h.ContainerID+"|"+token+"|"+id)

	return e.backupErr
}

func (e *fakeEngine) DrainLogs(timestreaminfluxdb.EngineHandle) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := e.logs
	e.logs = nil

	return out
}

func (e *fakeEngine) snapshot() ([]timestreaminfluxdb.EngineSpec, []timestreaminfluxdb.EngineSpec, []string, []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Clone(e.specs), slices.Clone(e.recreated), slices.Clone(e.restarted), slices.Clone(e.backups)
}

func (e *fakeEngine) Start(
	ctx context.Context,
	spec timestreaminfluxdb.EngineSpec,
) (timestreaminfluxdb.EngineHandle, error) {
	e.mu.Lock()
	e.specs = append(e.specs, spec)
	e.mu.Unlock()

	if e.gate != nil {
		select {
		case <-e.gate:
		case <-ctx.Done():
			return timestreaminfluxdb.EngineHandle{}, ctx.Err()
		}
	}

	if e.startErr != nil {
		return timestreaminfluxdb.EngineHandle{}, e.startErr
	}

	n := e.next.Add(1)

	return timestreaminfluxdb.EngineHandle{ContainerID: "ctr-" + spec.Name, Host: "127.0.0.1", Port: 18000 + n}, nil
}

func (e *fakeEngine) Stop(_ context.Context, h timestreaminfluxdb.EngineHandle) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.stopped = append(e.stopped, h.ContainerID)

	return nil
}

func (e *fakeEngine) stoppedIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]string(nil), e.stopped...)
}

func newEngineFixture(t *testing.T, eng *fakeEngine) *fixture {
	t.Helper()

	b := timestreaminfluxdb.NewInMemoryBackend(testAccount, "us-east-1")
	b.SetEngine(eng)
	t.Cleanup(b.Close)

	return newFixtureFromBackend(t, b, &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})
}

func status(t *testing.T, f *fixture, id *string) types.Status {
	t.Helper()

	got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: id})
	require.NoError(t, err)

	return got.Status
}

func TestEngineBackedInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, eng *fakeEngine)
		eng  *fakeEngine
		name string
	}{
		{name: "stays creating until the server is up", eng: &fakeEngine{gate: make(chan struct{})},
			run: func(t *testing.T, f *fixture, eng *fakeEngine) {
				t.Helper()

				in := instanceInput("engine-db")
				in.Username, in.Organization, in.Bucket = aws.String("alice"), aws.String("acme"), aws.String("metrics")

				out, err := f.client.CreateDbInstance(t.Context(), in)
				require.NoError(t, err)

				f.settle()
				assert.Equal(
					t,
					types.StatusCreating,
					status(t, f, out.Id),
					"the lifecycle clock must not complete an engine start",
				)

				close(eng.gate)

				require.Eventually(
					t,
					func() bool { return status(t, f, out.Id) == types.StatusAvailable },
					5*time.Second,
					5*time.Millisecond,
				)

				got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: out.Id})
				require.NoError(t, err)
				assert.Equal(t, "127.0.0.1", aws.ToString(got.Endpoint))
				assert.Equal(t, int32(18001), aws.ToInt32(got.Port))

				require.Len(t, eng.specs, 1)
				assert.Equal(t, aws.ToString(out.Id), eng.specs[0].Name)
				assert.Equal(t, "alice", eng.specs[0].Username)
				assert.Equal(t, "Passw0rdPassw0rd", eng.specs[0].Password)
				assert.Equal(t, "acme", eng.specs[0].Organization)
				assert.Equal(t, "metrics", eng.specs[0].Bucket)
				assert.Len(t, eng.specs[0].Token, 64)

				_, err = f.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: out.Id})
				require.NoError(t, err)
				assert.Equal(t, []string{"ctr-" + aws.ToString(out.Id)}, eng.stoppedIDs())
			}},
		{name: "defaults are applied to the container", eng: &fakeEngine{},
			run: func(t *testing.T, f *fixture, eng *fakeEngine) {
				t.Helper()

				out, err := f.client.CreateDbInstance(t.Context(), instanceInput("default-db"))
				require.NoError(t, err)
				require.Eventually(
					t,
					func() bool { return status(t, f, out.Id) == types.StatusAvailable },
					5*time.Second,
					5*time.Millisecond,
				)

				assert.Equal(t, "admin", eng.specs[0].Username)
				assert.Equal(t, "organization", eng.specs[0].Organization)
				assert.Equal(t, "bucket", eng.specs[0].Bucket)
			}},
		{name: "start failure marks the instance FAILED", eng: &fakeEngine{startErr: errPull},
			run: func(t *testing.T, f *fixture, eng *fakeEngine) {
				t.Helper()

				out, err := f.client.CreateDbInstance(t.Context(), instanceInput("broken-db"))
				require.NoError(t, err)
				require.Eventually(
					t,
					func() bool { return status(t, f, out.Id) == types.StatusFailed },
					5*time.Second,
					5*time.Millisecond,
				)

				got, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: out.Id})
				require.NoError(t, err)
				assert.Empty(t, aws.ToString(got.Endpoint))

				_, err = f.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: out.Id})
				require.NoError(t, err)
				assert.Empty(t, eng.stoppedIDs())
			}},
		{name: "a starting instance cannot be deleted", eng: &fakeEngine{gate: make(chan struct{})},
			run: func(t *testing.T, f *fixture, eng *fakeEngine) {
				t.Helper()

				out, err := f.client.CreateDbInstance(t.Context(), instanceInput("late-db"))
				require.NoError(t, err)

				_, err = f.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: out.Id})
				assert.Equal(t, "ConflictException", apiCode(err), "a CREATING instance cannot be deleted")

				close(eng.gate)
				require.Eventually(
					t,
					func() bool { return status(t, f, out.Id) == types.StatusAvailable },
					5*time.Second,
					5*time.Millisecond,
				)
			}},
		{name: "close stops running servers and aborts pending starts", eng: &fakeEngine{},
			run: func(t *testing.T, f *fixture, eng *fakeEngine) {
				t.Helper()

				out, err := f.client.CreateDbInstance(t.Context(), instanceInput("closing-db"))
				require.NoError(t, err)
				require.Eventually(
					t,
					func() bool { return status(t, f, out.Id) == types.StatusAvailable },
					5*time.Second,
					5*time.Millisecond,
				)

				f.backend.Close()
				assert.Equal(t, []string{"ctr-" + aws.ToString(out.Id)}, eng.stoppedIDs())
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newEngineFixture(t, tt.eng), tt.eng)
		})
	}
}

func TestCloseAbortsPendingStart(t *testing.T) {
	t.Parallel()

	eng := &fakeEngine{gate: make(chan struct{})}
	f := newEngineFixture(t, eng)

	_, err := f.client.CreateDbInstance(t.Context(), instanceInput("pending-db"))
	require.NoError(t, err)

	f.backend.Close()
	assert.Empty(t, eng.stoppedIDs())
}

type fakeRuntime struct {
	createErr error
	pulled    []string
	specs     []container.Spec
	removed   []string
	calls     []string
	execs     []string
	execExit  int
	mu        sync.Mutex
	hasImage  bool
}

func (r *fakeRuntime) HasImage(context.Context, string) (bool, error) { return r.hasImage, nil }

func (r *fakeRuntime) PullImage(_ context.Context, ref string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pulled = append(r.pulled, ref)

	return nil
}

func (r *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.createErr != nil {
		return "", r.createErr
	}

	r.specs = append(r.specs, spec)

	return "ctr-" + strconv.Itoa(len(r.specs)), nil
}

func (r *fakeRuntime) StopContainer(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, "stop:"+id)

	return nil
}

func (r *fakeRuntime) StartContainer(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, "start:"+id)

	return nil
}

func (r *fakeRuntime) Exec(_ context.Context, id string, cmd []string) (container.ExecResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.execs = append(r.execs, id+" "+strings.Join(cmd, " "))

	return container.ExecResult{ExitCode: r.execExit, Stderr: "boom"}, nil
}

func (r *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removed = append(r.removed, id)

	return nil
}

type fakeHealth struct {
	urls   []string
	status []int
	mu     sync.Mutex
}

func (h *fakeHealth) Do(r *http.Request) (*http.Response, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.urls = append(h.urls, r.URL.String())

	code := http.StatusOK
	if len(h.status) > 0 {
		code, h.status = h.status[0], h.status[1:]
	}

	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func TestDockerEngine(t *testing.T) {
	t.Parallel()

	spec := timestreaminfluxdb.EngineSpec{
		Name:         "abc",
		Username:     "u",
		Password:     "Passw0rd",
		Organization: "o",
		Bucket:       "bk",
	}

	t.Run("start waits for health and publishes the port", func(t *testing.T) {
		t.Parallel()

		ports, err := portalloc.New(30000, 30010)
		require.NoError(t, err)

		rt, hc := &fakeRuntime{}, &fakeHealth{status: []int{503, 503}}
		eng := &timestreaminfluxdb.DockerEngine{Runtime: rt, Ports: ports, HTTP: hc, PollInterval: time.Millisecond}

		h, err := eng.Start(t.Context(), spec)
		require.NoError(t, err)
		assert.Equal(t, "ctr-1", h.ContainerID)
		assert.Equal(t, "127.0.0.1", h.Host)
		assert.GreaterOrEqual(t, h.Port, int32(30000))
		assert.Equal(t, []string{"influxdb:2"}, rt.pulled)
		assert.Len(t, hc.urls, 3)
		assert.Equal(t, "http://127.0.0.1:"+itoa(h.Port)+"/health", hc.urls[0])

		require.Len(t, rt.specs, 1)
		assert.Equal(t, "influxdb:2", rt.specs[0].Image)
		assert.ElementsMatch(t, []string{
			"DOCKER_INFLUXDB_INIT_MODE=setup",
			"DOCKER_INFLUXDB_INIT_USERNAME=u",
			"DOCKER_INFLUXDB_INIT_PASSWORD=Passw0rd",
			"DOCKER_INFLUXDB_INIT_ORG=o",
			"DOCKER_INFLUXDB_INIT_BUCKET=bk",
		}, rt.specs[0].Env)
		assert.Equal(t, []string{"127.0.0.1:" + itoa(h.Port) + ":8086"}, rt.specs[0].Ports)
		assert.True(t, ports.IsAllocated(int(h.Port)))

		require.NoError(t, eng.Stop(t.Context(), h))
		assert.Equal(t, []string{"ctr-1"}, rt.removed)
		assert.False(t, ports.IsAllocated(int(h.Port)))
	})

	t.Run("an image already present is not pulled", func(t *testing.T) {
		t.Parallel()

		ports, err := portalloc.New(30020, 30030)
		require.NoError(t, err)

		rt := &fakeRuntime{hasImage: true}
		eng := &timestreaminfluxdb.DockerEngine{Runtime: rt, Ports: ports, HTTP: &fakeHealth{}, Image: "influxdb:2.7"}

		_, err = eng.Start(t.Context(), spec)
		require.NoError(t, err)
		assert.Empty(t, rt.pulled)
		assert.Equal(t, "influxdb:2.7", rt.specs[0].Image)
	})

	t.Run("create failure frees the port", func(t *testing.T) {
		t.Parallel()

		ports, err := portalloc.New(30040, 30050)
		require.NoError(t, err)

		eng := &timestreaminfluxdb.DockerEngine{
			Runtime: &fakeRuntime{createErr: errBoom},
			Ports:   ports,
			HTTP:    &fakeHealth{},
		}

		_, err = eng.Start(t.Context(), spec)
		require.Error(t, err)
		assert.Empty(t, ports.Allocated())
	})

	t.Run("health timeout removes the container", func(t *testing.T) {
		t.Parallel()

		ports, err := portalloc.New(30060, 30070)
		require.NoError(t, err)

		rt := &fakeRuntime{}
		hc := &fakeHealth{status: repeat(503, 1000)}
		eng := &timestreaminfluxdb.DockerEngine{
			Runtime: rt, Ports: ports, HTTP: hc, PollInterval: time.Millisecond, StartTimeout: 20 * time.Millisecond,
		}

		_, err = eng.Start(t.Context(), spec)
		require.Error(t, err)
		assert.Equal(t, []string{"ctr-1"}, rt.removed)
		assert.Empty(t, ports.Allocated())
	})

	t.Run("no allocator falls back to an ephemeral port", func(t *testing.T) {
		t.Parallel()

		eng := &timestreaminfluxdb.DockerEngine{Runtime: &fakeRuntime{hasImage: true}, HTTP: &fakeHealth{}}
		h, err := eng.Start(t.Context(), spec)
		require.NoError(t, err)
		assert.Positive(t, h.Port)
		require.NoError(t, eng.Stop(t.Context(), h))
	})
}

func repeat(v, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = v
	}

	return out
}

func itoa(n int32) string {
	if n == 0 {
		return "0"
	}

	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}

	return string(b)
}
