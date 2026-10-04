package rds_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestEngine_PublishedPortBindsLoopbackByDefault(t *testing.T) {
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

			rt := newFakeEngineRuntime()
			b := rds.NewInMemoryBackend("123456789012", "us-east-1")
			t.Cleanup(b.Close)
			b.EnableEngine(rds.EngineConfig{
				Runtime:      rt,
				Host:         tt.host,
				StartTimeout: time.Minute,
				Probe:        func(context.Context, rds.EngineLogin) error { return nil },
			})

			_, err := b.CreateDBInstance("bind", "postgres", "db.t3.micro", "appdb", "master", "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "s3cretpassword"})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return rt.containers() == 1 }, 10*time.Second, 5*time.Millisecond)

			ip, _, _, err := container.ParsePortSpec(rt.firstSpec().Ports[0])
			require.NoError(t, err)
			assert.Equal(t, tt.wantIP, ip.String())
		})
	}
}
