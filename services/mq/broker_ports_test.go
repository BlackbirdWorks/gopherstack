package mq_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

const forbiddenHostPort = "1883"

func createBrokerMode(t *testing.T, b *mq.InMemoryBackend, engine, mode string) *mq.Broker {
	t.Helper()

	br, err := b.CreateBroker("b1", mode, engine, "", "", false, false, nil, nil,
		[]*mq.User{{Username: "admin", Password: "s3cretpassword"}}, nil)
	require.NoError(t, err)

	return br
}

func waitRunning(t *testing.T, b *mq.InMemoryBackend, id string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return brokerState(b, id) == mq.BrokerStateRunning
	}, 10*time.Second, time.Millisecond)
}

func TestDockerBrokerPortMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engine        string
		wantContainer []string
	}{
		{
			name: "rabbitmq", engine: mq.EngineTypeRabbitMQ,
			wantContainer: []string{"5671", "5672", "15671", "15672"},
		},
		{
			name: "activemq", engine: mq.EngineTypeActiveMQ,
			wantContainer: []string{
				"61617", "61616", "5671", "5672", "61614", "61613", "8883", "1883", "61619", "8162", "8161",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			b := dockerBackend(t, rt, probe)
			createBroker(t, b, tt.engine)
			require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 1 }, 10*time.Second, time.Millisecond)

			spec := rt.createdSpecs()[0]

			gotContainer := make([]string, 0, len(spec.Ports))
			hostPorts := make([]string, 0, len(spec.Ports))

			for _, p := range spec.Ports {
				_, hostPort, ctrPort, err := container.ParsePortSpec(p)
				require.NoError(t, err)

				gotContainer = append(gotContainer, ctrPort)
				hostPorts = append(hostPorts, hostPort)
			}

			assert.ElementsMatch(t, tt.wantContainer, gotContainer)
			assert.NotContains(t, hostPorts, forbiddenHostPort)

			seen := map[string]bool{}
			for _, hp := range hostPorts {
				assert.False(t, seen[hp], "duplicate host port %s", hp)
				seen[hp] = true
			}

			assert.NotEmpty(t, spec.Entrypoint, "TLS bootstrap entrypoint")
			assert.NotEmpty(t, spec.Cmd)
		})
	}
}

func TestDockerBrokerEndpointShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		engine      string
		wantConsole string
		wantSchemes []string
	}{
		{
			name: "rabbitmq", engine: mq.EngineTypeRabbitMQ,
			wantSchemes: []string{"amqps://"}, wantConsole: "https://",
		},
		{
			name: "activemq", engine: mq.EngineTypeActiveMQ,
			wantSchemes: []string{"ssl://", "amqp+ssl://", "stomp+ssl://", "mqtt+ssl://", "wss://"},
			wantConsole: "https://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			close(probe.gate)

			b := dockerBackend(t, rt, probe)
			br := createBroker(t, b, tt.engine)
			waitRunning(t, b, br.BrokerID)

			desc, err := b.DescribeBroker(br.BrokerID)
			require.NoError(t, err)
			require.Len(t, desc.BrokerInstances, 1)

			inst := desc.BrokerInstances[0]
			require.Len(t, inst.Endpoints, len(tt.wantSchemes))

			for i, scheme := range tt.wantSchemes {
				assert.True(t, strings.HasPrefix(inst.Endpoints[i], scheme+"127.0.0.1:"), inst.Endpoints[i])
				assert.NotContains(t, inst.Endpoints[i], ":"+forbiddenHostPort)
			}

			assert.True(t, strings.HasPrefix(inst.ConsoleURL, tt.wantConsole+"127.0.0.1:"), inst.ConsoleURL)

			port := inst.ConsoleURL[strings.LastIndex(inst.ConsoleURL, ":")+1:]
			_, err = strconv.Atoi(port)
			require.NoError(t, err)

			published := slices.Collect(func(yield func(string) bool) {
				for _, p := range rt.createdSpecs()[0].Ports {
					_, hp, _, perr := container.ParsePortSpec(p)
					if perr != nil || !yield(hp) {
						return
					}
				}
			})
			assert.Contains(t, published, port, "console URL points at a published port")
		})
	}
}

func TestDockerBrokerMultiAZ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		engine    string
		mode      string
		wantNodes int
	}{
		{name: "single", engine: mq.EngineTypeActiveMQ, mode: mq.DeploymentModeSingleInstance, wantNodes: 1},
		{name: "active standby", engine: mq.EngineTypeActiveMQ, mode: mq.DeploymentModeActiveStandby, wantNodes: 2},
		{name: "rabbit cluster", engine: mq.EngineTypeRabbitMQ, mode: mq.DeploymentModeCluster, wantNodes: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			close(probe.gate)

			b := dockerBackend(t, rt, probe)
			br := createBrokerMode(t, b, tt.engine, tt.mode)
			waitRunning(t, b, br.BrokerID)

			specs := rt.createdSpecs()
			require.Len(t, specs, tt.wantNodes)

			names := map[string]bool{}
			hostPorts := map[string]bool{}

			for _, s := range specs {
				names[s.Name] = true

				for _, p := range s.Ports {
					_, hp, _, err := container.ParsePortSpec(p)
					require.NoError(t, err)
					assert.False(t, hostPorts[hp], "host port %s reused across nodes", hp)
					hostPorts[hp] = true
				}
			}

			assert.Len(t, names, tt.wantNodes)

			desc, err := b.DescribeBroker(br.BrokerID)
			require.NoError(t, err)
			require.Len(t, desc.BrokerInstances, tt.wantNodes)

			endpoints := map[string]bool{}
			for _, inst := range desc.BrokerInstances {
				require.NotEmpty(t, inst.Endpoints)
				endpoints[inst.Endpoints[0]] = true
			}

			assert.Len(t, endpoints, tt.wantNodes, "each instance reports its own endpoints")

			_, err = b.DeleteBroker(br.BrokerID)
			require.NoError(t, err)

			require.Eventually(
				t,
				func() bool { return len(rt.removedIDs()) == tt.wantNodes },
				10*time.Second,
				time.Millisecond,
			)
		})
	}
}
