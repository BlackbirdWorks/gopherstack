package mq_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestDockerBroker_PublishedPortsBindLoopbackByDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
		host   string
		wantIP string
	}{
		{name: "rabbitmq_default", engine: mq.EngineTypeRabbitMQ, wantIP: "127.0.0.1"},
		{name: "activemq_default", engine: mq.EngineTypeActiveMQ, wantIP: "127.0.0.1"},
		{name: "rabbitmq_external", engine: mq.EngineTypeRabbitMQ, host: "10.1.2.3", wantIP: "0.0.0.0"},
		{name: "activemq_external", engine: mq.EngineTypeActiveMQ, host: "mq.example.com", wantIP: "0.0.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, probe := &fakeRuntime{}, newGatedProbe()
			b := mq.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableBrokers(mq.BrokerConfig{Runtime: rt, Probe: probe.probe, Host: tt.host, StartTimeout: time.Minute})
			t.Cleanup(b.Close)

			createBroker(t, b, tt.engine)
			require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 1 }, 10*time.Second, time.Millisecond)

			ports := rt.createdSpecs()[0].Ports
			require.NotEmpty(t, ports)

			for _, p := range ports {
				ip, _, _, err := container.ParsePortSpec(p)
				require.NoError(t, err)
				assert.Equal(t, tt.wantIP, ip.String())
			}
		})
	}
}
