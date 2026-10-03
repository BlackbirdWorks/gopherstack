package kafka_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestBroker_PublishedPortBindsLoopbackByDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		host   string
		wantIP string
	}{
		{name: "default", host: "", wantIP: "127.0.0.1"},
		{name: "loopback_host", host: "127.0.0.1", wantIP: "127.0.0.1"},
		{name: "external_host", host: "10.1.2.3", wantIP: "0.0.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeBrokerRuntime{}
			b := newTestBackend(t)
			b.EnableBrokers(kafka.BrokerConfig{Runtime: rt, Host: tt.host, StartTimeout: time.Minute})
			t.Cleanup(b.Close)

			_, err := b.CreateCluster(t.Context(), "bind", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, nil, nil)
			require.NoError(t, err)
			started := func() bool { return len(rt.startedSpecs()) == 1 }
			require.Eventually(t, started, 10*time.Second, 5*time.Millisecond)

			ip, _, _, err := container.ParsePortSpec(rt.startedSpecs()[0].Ports[0])
			require.NoError(t, err)
			assert.Equal(t, tt.wantIP, ip.String())
		})
	}
}
