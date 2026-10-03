package container_test

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestParsePortSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     string
		wantIP   string
		wantHost string
		wantCtr  string
		wantErr  bool
	}{
		{name: "host_container", spec: "19092:9092", wantIP: "0.0.0.0", wantHost: "19092", wantCtr: "9092"},
		{name: "ip_form", spec: "127.0.0.1:19092:9092", wantIP: "127.0.0.1", wantHost: "19092", wantCtr: "9092"},
		{name: "all_interfaces_ip", spec: "0.0.0.0:1:2", wantIP: "0.0.0.0", wantHost: "1", wantCtr: "2"},
		{name: "single_field", spec: "9092", wantErr: true},
		{name: "empty", spec: "", wantErr: true},
		{name: "bad_ip", spec: "999.1.1.1:1:2", wantErr: true},
		{name: "hostname_not_ip", spec: "localhost:1:2", wantErr: true},
		{name: "too_many_fields", spec: "127.0.0.1:1:2:3", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ip, host, ctr, err := container.ParsePortSpec(tt.spec)
			if tt.wantErr {
				require.ErrorIs(t, err, container.ErrInvalidPort)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, netip.MustParseAddr(tt.wantIP), ip)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantCtr, ctr)
		})
	}
}

func TestBindHostFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want string
	}{
		{name: "empty", host: "", want: "127.0.0.1"},
		{name: "ipv4_loopback", host: "127.0.0.1", want: "127.0.0.1"},
		{name: "other_loopback", host: "127.0.0.2", want: "127.0.0.1"},
		{name: "ipv6_loopback", host: "::1", want: "127.0.0.1"},
		{name: "localhost", host: "localhost", want: "127.0.0.1"},
		{name: "lan_ip", host: "10.1.2.3", want: "0.0.0.0"},
		{name: "hostname", host: "eks.example.com", want: "0.0.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, container.BindHostFor(tt.host))
		})
	}
}

func TestDockerRuntime_CreateAndStart_PortHostIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		port   string
		wantIP string
	}{
		{name: "loopback", port: "127.0.0.1:19092:9092", wantIP: "127.0.0.1"},
		{name: "default_all", port: "19092:9092", wantIP: "0.0.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &mockAPI{}
			spec := container.Spec{Image: "alpine", Ports: []string{tt.port}}
			_, err := newRuntime(api).CreateAndStart(t.Context(), spec)
			require.NoError(t, err)

			for _, b := range api.lastHost.PortBindings {
				assert.Equal(t, tt.wantIP, b[0].HostIP.String())
				assert.Equal(t, "19092", b[0].HostPort)
			}
		})
	}
}
