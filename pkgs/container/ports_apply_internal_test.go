package container

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

func TestApplyPortsProtocols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		spec  string
		want  string
		wantE bool
	}{
		{name: "default tcp", spec: "127.0.0.1:8080:80", want: "80/tcp"},
		{name: "udp", spec: "127.0.0.1:5353:53/udp", want: "53/udp"},
		{name: "explicit tcp", spec: "5353:53/tcp", want: "53/tcp"},
		{name: "bad port", spec: "5353:x/udp", wantE: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &dockercontainer.Config{}
			host := &dockercontainer.HostConfig{}
			err := applyPorts(cfg, host, []string{tt.spec})

			if tt.wantE {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, host.PortBindings, 1)

			for p := range host.PortBindings {
				assert.Equal(t, tt.want, p.String())
			}
		})
	}
}
