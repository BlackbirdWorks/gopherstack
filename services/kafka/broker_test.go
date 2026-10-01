package kafka_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

var errBrokerDown = errors.New("broker down")

type fakeBrokerRuntime struct {
	startErr error
	gate     chan struct{}
	started  []container.Spec
	removed  []string
	mu       sync.Mutex
}

func (f *fakeBrokerRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	if f.gate != nil {
		<-f.gate
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.startErr != nil {
		return "", f.startErr
	}

	f.started = append(f.started, spec)

	return "ctr-" + strconv.Itoa(len(f.started)), nil
}

func (f *fakeBrokerRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeBrokerRuntime) startedSpecs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.started)
}

func (f *fakeBrokerRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.removed)
}

type brokerFixture struct {
	rt      *fakeBrokerRuntime
	backend *kafka.InMemoryBackend
	handler *kafka.Handler
	ready   atomic.Bool
}

func newBrokerFixture(t *testing.T, rt *fakeBrokerRuntime, startTimeout time.Duration) *brokerFixture {
	t.Helper()

	f := &brokerFixture{rt: rt, backend: newTestBackend(t)}
	f.handler = kafka.NewHandler(f.backend)
	f.backend.EnableBrokers(kafka.BrokerConfig{
		Runtime: rt,
		Probe: func(context.Context, string) error {
			if f.ready.Load() {
				return nil
			}

			return errBrokerDown
		},
		StartTimeout: startTimeout,
	})
	t.Cleanup(f.backend.Close)

	return f
}

func (f *brokerFixture) create(t *testing.T, name string) string {
	t.Helper()

	c, err := f.backend.CreateCluster(t.Context(), name, "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterStateCreating, c.State)

	return c.ClusterArn
}

func (f *brokerFixture) state(t *testing.T, arn string) string {
	t.Helper()

	c, err := f.backend.DescribeCluster(t.Context(), arn)
	require.NoError(t, err)

	return c.State
}

func (f *brokerFixture) awaitState(t *testing.T, arn, want string) {
	t.Helper()

	require.Eventually(t, func() bool { return f.state(t, arn) == want }, 10*time.Second, 10*time.Millisecond)
}

func (f *brokerFixture) bootstrapJSON(t *testing.T, arn string) map[string]any {
	t.Helper()

	rec := doKafkaRequest(t, f.handler, http.MethodGet, "/v1/clusters/"+url.PathEscape(arn)+"/bootstrap-brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	return decodeJSONResponse(t, rec)
}

func TestBroker_StateMachineAndBootstrap(t *testing.T) {
	t.Parallel()

	f := newBrokerFixture(t, &fakeBrokerRuntime{}, 0)
	arn := f.create(t, "real")

	require.Eventually(t, func() bool { return len(f.rt.startedSpecs()) == 1 }, 10*time.Second, 5*time.Millisecond)

	servers, managed := f.backend.ManagedBootstrap(arn)
	assert.True(t, managed)
	assert.Empty(t, servers)
	assert.Equal(t, kafka.ClusterStateCreating, f.state(t, arn))
	assert.Empty(t, f.bootstrapJSON(t, arn)["bootstrapBrokerString"])

	f.ready.Store(true)
	f.awaitState(t, arn, kafka.ClusterStateActive)

	spec := f.rt.startedSpecs()[0]
	require.Len(t, spec.Ports, 1)

	hostPort, containerPort, _ := strings.Cut(spec.Ports[0], ":")
	assert.Equal(t, "9092", containerPort)
	assert.Equal(t, kafka.BrokerImage, spec.Image)
	assert.Contains(t, spec.Env, "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:"+hostPort)

	assert.Equal(t, []string{"127.0.0.1:" + hostPort}, f.backend.BootstrapServers(arn))

	out := f.bootstrapJSON(t, arn)
	assert.Equal(t, "127.0.0.1:"+hostPort, out["bootstrapBrokerString"])
	assert.NotContains(t, out, "bootstrapBrokerStringTls")

	require.NoError(t, f.backend.DeleteCluster(t.Context(), arn))
	require.Eventually(t, func() bool { return slices.Contains(f.rt.removedIDs(), "ctr-1") },
		10*time.Second, 5*time.Millisecond)

	_, managed = f.backend.ManagedBootstrap(arn)
	assert.False(t, managed)
}

func TestBroker_StartFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rt          *fakeBrokerRuntime
		name        string
		timeout     time.Duration
		wantRemoved bool
	}{
		{name: "create_error", rt: &fakeBrokerRuntime{startErr: errBrokerDown}},
		{name: "never_reachable", rt: &fakeBrokerRuntime{}, timeout: 50 * time.Millisecond, wantRemoved: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newBrokerFixture(t, tt.rt, tt.timeout)
			arn := f.create(t, "doomed")
			f.awaitState(t, arn, kafka.ClusterStateFailed)

			c, err := f.backend.DescribeCluster(t.Context(), arn)
			require.NoError(t, err)
			require.NotNil(t, c.StateInfo)
			assert.NotEmpty(t, c.StateInfo.Message)
			assert.Empty(t, f.backend.BootstrapServers(arn))

			if tt.wantRemoved {
				require.Eventually(t, func() bool {
					return len(f.rt.removedIDs()) == 1
				}, 10*time.Second, 5*time.Millisecond)
			}
		})
	}
}

func TestBroker_DeleteDuringStartup(t *testing.T) {
	t.Parallel()

	rt := &fakeBrokerRuntime{gate: make(chan struct{})}
	f := newBrokerFixture(t, rt, 0)
	arn := f.create(t, "racy")

	require.NoError(t, f.backend.DeleteCluster(t.Context(), arn))
	close(rt.gate)

	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"ctr-1"}, rt.removedIDs())
}

func TestBroker_CloseAndResetRemoveContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(*brokerFixture)
		name string
	}{
		{name: "close", stop: func(f *brokerFixture) { f.backend.Close() }},
		{name: "reset", stop: func(f *brokerFixture) { f.backend.Reset() }},
		{name: "shutdown", stop: func(f *brokerFixture) { f.handler.Shutdown(context.Background()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newBrokerFixture(t, &fakeBrokerRuntime{}, 0)
			f.ready.Store(true)

			a := f.create(t, "a")
			b := f.create(t, "b")
			f.awaitState(t, a, kafka.ClusterStateActive)
			f.awaitState(t, b, kafka.ClusterStateActive)

			tt.stop(f)

			require.Eventually(t, func() bool {
				return len(f.rt.removedIDs()) == 2
			}, 10*time.Second, 5*time.Millisecond)
		})
	}
}

func TestBroker_RestoreRelaunchesBrokers(t *testing.T) {
	t.Parallel()

	src := newBrokerFixture(t, &fakeBrokerRuntime{}, 0)
	src.ready.Store(true)

	arn := src.create(t, "persisted")
	src.awaitState(t, arn, kafka.ClusterStateActive)

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newBrokerFixture(t, &fakeBrokerRuntime{}, 0)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))
	assert.Equal(t, kafka.ClusterStateCreating, dst.state(t, arn))

	dst.ready.Store(true)
	dst.awaitState(t, arn, kafka.ClusterStateActive)
	assert.Len(t, dst.backend.BootstrapServers(arn), 1)
	assert.Len(t, dst.rt.startedSpecs(), 1)
}

func TestBroker_MetadataOnlyBackendUnchanged(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	c, err := b.CreateCluster(t.Context(), "plain", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, nil, nil)
	require.NoError(t, err)

	servers, managed := b.ManagedBootstrap(c.ClusterArn)
	assert.False(t, managed)
	assert.Empty(t, servers)

	got, err := b.DescribeCluster(t.Context(), c.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterStateActive, got.State)

	b.Close()
}
