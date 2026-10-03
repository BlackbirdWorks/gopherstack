package mq_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

var (
	errNoDocker = errors.New("no docker")
	errRefused  = errors.New("refused")
)

type fakeRuntime struct {
	failErr error
	specs   []container.Spec
	removed []string
	mu      sync.Mutex
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failErr != nil {
		return "", f.failErr
	}

	f.specs = append(f.specs, spec)

	return "ctr-" + spec.Name, nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeRuntime) createdSpecs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

func (f *fakeRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

type probeCall struct {
	engine, user, pass string
}

type gatedProbe struct {
	err   error
	gate  chan struct{}
	calls []probeCall
	mu    sync.Mutex
}

func newGatedProbe() *gatedProbe { return &gatedProbe{gate: make(chan struct{})} }

func (g *gatedProbe) probe(ctx context.Context, engine, _, user, pass string) error {
	g.mu.Lock()
	g.calls = append(g.calls, probeCall{engine, user, pass})
	err := g.err
	g.mu.Unlock()

	if err != nil {
		return err
	}

	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func dockerBackend(t *testing.T, rt *fakeRuntime, probe *gatedProbe) *mq.InMemoryBackend {
	t.Helper()

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableBrokers(mq.BrokerConfig{Runtime: rt, Probe: probe.probe, StartTimeout: time.Minute})
	t.Cleanup(b.Close)

	return b
}

func createBroker(t *testing.T, b *mq.InMemoryBackend, engine string) *mq.Broker {
	t.Helper()

	br, err := b.CreateBroker("b1", "", engine, "", "", false, false, nil, nil,
		[]*mq.User{{Username: "admin", Password: "s3cretpassword"}}, nil)
	require.NoError(t, err)

	return br
}

func brokerState(b *mq.InMemoryBackend, id string) string {
	br, err := b.DescribeBroker(id)
	if err != nil {
		return err.Error()
	}

	return br.BrokerState
}

func TestDockerBrokerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engine     string
		image      string
		wantEngine string
		wantEnv    []string
		wantScheme []string
		wantPorts  int
	}{
		{
			name: "rabbitmq", engine: mq.EngineTypeRabbitMQ, image: mq.RabbitMQImage, wantPorts: 2,
			wantEnv:    []string{"RABBITMQ_DEFAULT_USER=admin", "RABBITMQ_DEFAULT_PASS=s3cretpassword"},
			wantScheme: []string{"amqp://"}, wantEngine: mq.EngineTypeRabbitMQ,
		},
		{
			name: "activemq", engine: mq.EngineTypeActiveMQ, image: mq.ActiveMQImage, wantPorts: 3,
			wantEnv:    []string{"ACTIVEMQ_CONNECTION_USER=admin", "ACTIVEMQ_CONNECTION_PASSWORD=s3cretpassword"},
			wantScheme: []string{"tcp://", "stomp://", "mqtt://"}, wantEngine: mq.EngineTypeActiveMQ,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			b := dockerBackend(t, rt, probe)

			br := createBroker(t, b, tt.engine)
			assert.Equal(t, mq.BrokerStateCreating, br.BrokerState)

			_, _, ok := b.MQConsumerEndpoint(br.BrokerArn)
			assert.False(t, ok, "not reachable yet")

			require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 1 }, 10*time.Second, time.Millisecond)

			spec := rt.createdSpecs()[0]
			assert.Equal(t, tt.image, spec.Image)
			assert.Len(t, spec.Ports, tt.wantPorts)
			assert.Subset(t, spec.Env, tt.wantEnv)
			assert.Equal(t, mq.BrokerStateCreating, brokerState(b, br.BrokerID))

			close(probe.gate)

			require.Eventually(t, func() bool {
				return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
			}, 10*time.Second, time.Millisecond)

			desc, err := b.DescribeBroker(br.BrokerID)
			require.NoError(t, err)
			require.Len(t, desc.BrokerInstances, 1)
			require.Len(t, desc.BrokerInstances[0].Endpoints, len(tt.wantScheme))

			for i, scheme := range tt.wantScheme {
				ep := desc.BrokerInstances[0].Endpoints[i]
				assert.True(t, strings.HasPrefix(ep, scheme+"127.0.0.1:"), ep)
			}

			engine, addr, ok := b.MQConsumerEndpoint(br.BrokerArn)
			require.True(t, ok)
			assert.Equal(t, tt.wantEngine, engine)
			assert.True(t, strings.HasPrefix(addr, "127.0.0.1:"))

			_, err = b.DeleteBroker(br.BrokerID)
			require.NoError(t, err)

			require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, time.Millisecond)
			assert.Equal(t, "ctr-"+spec.Name, rt.removedIDs()[0])

			_, _, ok = b.MQConsumerEndpoint(br.BrokerArn)
			assert.False(t, ok)
		})
	}
}

func TestDockerBrokerFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runtimeErr   error
		probeErr     error
		name         string
		wantRemovals int
	}{
		{name: "start error", runtimeErr: errNoDocker},
		{name: "never reachable", probeErr: errRefused, wantRemovals: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{failErr: tt.runtimeErr}, newGatedProbe()
			probe.err = tt.probeErr

			b := mq.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableBrokers(mq.BrokerConfig{Runtime: rt, Probe: probe.probe, StartTimeout: 50 * time.Millisecond})
			t.Cleanup(b.Close)

			br := createBroker(t, b, mq.EngineTypeRabbitMQ)

			require.Eventually(t, func() bool {
				return brokerState(b, br.BrokerID) == mq.BrokerStateCreationFailed
			}, 10*time.Second, time.Millisecond)

			require.Eventually(
				t,
				func() bool { return len(rt.removedIDs()) == tt.wantRemovals },
				10*time.Second,
				time.Millisecond,
			)

			_, _, ok := b.MQConsumerEndpoint(br.BrokerArn)
			assert.False(t, ok)
		})
	}
}

func TestDockerBrokerCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		password string
		users    bool
		wantErr  bool
		wantCtr  bool
	}{
		{
			name:     "activemq unsafe password",
			engine:   mq.EngineTypeActiveMQ,
			password: "a/b&c",
			users:    true,
			wantErr:  true,
		},
		{
			name:     "rabbitmq accepts any password",
			engine:   mq.EngineTypeRabbitMQ,
			password: "a/b&c",
			users:    true,
			wantCtr:  true,
		},
		{name: "no user stays metadata only", engine: mq.EngineTypeRabbitMQ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			close(probe.gate)

			b := dockerBackend(t, rt, probe)

			var users []*mq.User
			if tt.users {
				users = []*mq.User{{Username: "admin", Password: tt.password}}
			}

			br, err := b.CreateBroker("b1", "", tt.engine, "", "", false, false, nil, nil, users, nil)
			if tt.wantErr {
				require.ErrorIs(t, err, mq.ErrValidation)

				return
			}

			require.NoError(t, err)

			if tt.wantCtr {
				require.Eventually(
					t,
					func() bool { return len(rt.createdSpecs()) == 1 },
					10*time.Second,
					time.Millisecond,
				)

				return
			}

			assert.Equal(t, mq.BrokerStateRunning, br.BrokerState)
			assert.Empty(t, rt.createdSpecs())
		})
	}
}

func TestDockerBrokerTeardown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		teardown func(t *testing.T, b *mq.InMemoryBackend)
		name     string
	}{
		{name: "reset", teardown: func(_ *testing.T, b *mq.InMemoryBackend) { b.Reset() }},
		{name: "close", teardown: func(_ *testing.T, b *mq.InMemoryBackend) { b.Close() }},
		{name: "restore", teardown: func(t *testing.T, b *mq.InMemoryBackend) {
			t.Helper()
			require.NoError(t, b.Restore(t.Context(), b.Snapshot(t.Context())))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			close(probe.gate)

			b := dockerBackend(t, rt, probe)
			br := createBroker(t, b, mq.EngineTypeRabbitMQ)

			require.Eventually(t, func() bool {
				return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
			}, 10*time.Second, time.Millisecond)

			tt.teardown(t, b)

			require.Eventually(t, func() bool { return len(rt.removedIDs()) >= 1 }, 10*time.Second, time.Millisecond)
		})
	}
}

func TestDockerBrokerRestoreRelaunches(t *testing.T) {
	t.Parallel()

	rt, probe := &fakeRuntime{}, newGatedProbe()
	close(probe.gate)

	b := dockerBackend(t, rt, probe)
	br := createBroker(t, b, mq.EngineTypeRabbitMQ)

	require.Eventually(t, func() bool {
		return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
	}, 10*time.Second, time.Millisecond)

	require.NoError(t, b.Restore(t.Context(), b.Snapshot(t.Context())))

	require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 2 }, 10*time.Second, time.Millisecond)

	second := rt.createdSpecs()[1]
	assert.Contains(t, second.Env, "RABBITMQ_DEFAULT_USER=admin")
	assert.NotContains(t, second.Env, "RABBITMQ_DEFAULT_PASS=s3cretpassword", "passwords are not persisted")

	require.Eventually(t, func() bool {
		return brokerState(b, br.BrokerID) == mq.BrokerStateRunning
	}, 10*time.Second, time.Millisecond)
}

func TestMetadataOnlyBrokerStaysRunning(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	br := createBroker(t, b, mq.EngineTypeRabbitMQ)

	assert.Equal(t, mq.BrokerStateRunning, br.BrokerState)
	assert.True(t, strings.HasPrefix(br.BrokerInstances[0].Endpoints[0], "amqps://"))

	_, _, ok := b.MQConsumerEndpoint(br.BrokerArn)
	assert.False(t, ok)
}
