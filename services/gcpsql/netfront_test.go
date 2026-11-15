package gcpsql_test

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func startEcho(t *testing.T) *net.TCPAddr {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()

	a, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return a
}

// echoes reports whether a connection to addr is forwarded to the engine.
func echoes(t *testing.T, addr string) bool {
	t.Helper()

	d := net.Dialer{Timeout: 2 * time.Second}

	c, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return false
	}
	defer c.Close()

	_ = c.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err = c.Write([]byte("ping")); err != nil {
		return false
	}

	buf := make([]byte, 4)
	_, err = io.ReadFull(c, buf)

	return err == nil && string(buf) == "ping"
}

func netSettings(ipv4 bool, cidrs ...string) *sqladmin.Settings {
	ip := &sqladmin.IpConfiguration{Ipv4Enabled: ipv4, ForceSendFields: []string{"Ipv4Enabled"}}
	for _, c := range cidrs {
		ip.AuthorizedNetworks = append(ip.AuthorizedNetworks, &sqladmin.AclEntry{Value: c})
	}

	return &sqladmin.Settings{IpConfiguration: ip}
}

func TestAuthorizedNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings *sqladmin.Settings
		name     string
		want     bool
	}{
		{name: "none configured", settings: netSettings(true), want: true},
		{name: "loopback range", settings: netSettings(true, "127.0.0.0/8"), want: true},
		{name: "single address", settings: netSettings(true, "127.0.0.1"), want: true},
		{name: "other range", settings: netSettings(true, "10.0.0.0/8", "203.0.113.7"), want: false},
		{name: "public ip off", settings: netSettings(false, "127.0.0.0/8"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnvWith(t, &fakeLauncher{base: startEcho(t).Port})
			e.createInstance(t, "net", "POSTGRES_16", tt.settings)

			in, err := e.backend.GetInstance(project, "net")
			require.NoError(t, err)
			require.NotEmpty(t, in.GopherstackEndpoint)
			assert.NotEqual(t, "127.0.0.1:"+strconv.Itoa(e.launcher.base), in.GopherstackEndpoint,
				"clients connect through the instance front door, not the engine port")
			assert.Equal(t, tt.want, echoes(t, in.GopherstackEndpoint))
		})
	}
}

func TestAuthorizedNetworksFollowPatch(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, &fakeLauncher{base: startEcho(t).Port})
	e.createInstance(t, "net", "POSTGRES_16", netSettings(true, "10.0.0.0/8"))

	in, err := e.backend.GetInstance(project, "net")
	require.NoError(t, err)
	require.False(t, echoes(t, in.GopherstackEndpoint))

	op, err := e.svc.Instances.Patch(project, "net", &sqladmin.DatabaseInstance{
		Settings: netSettings(true, "10.0.0.0/8", "127.0.0.1/32"),
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)
	assert.True(t, echoes(t, in.GopherstackEndpoint), "an added network admits the peer without a restart")
}

func TestAuthorizedNetworksRejectGarbage(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	_, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: "bad", DatabaseVersion: "POSTGRES_16", Settings: netSettings(true, "not-a-cidr"),
	}).Context(t.Context()).Do()
	require.Error(t, err)
	assert.Equal(t, 400, statusOf(t, err))
}
