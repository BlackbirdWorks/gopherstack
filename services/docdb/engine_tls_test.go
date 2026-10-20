package docdb_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func caPool(t *testing.T, b *docdb.InMemoryBackend) *x509.CertPool {
	t.Helper()

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.TLSCAPEM()))

	return pool
}

func tlsHandshakeOK(c docdb.DBCluster, pool *x509.CertPool, serverName string) bool {
	d := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 5 * time.Second},
		Config:    &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12},
	}

	conn, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)))
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}

func TestEngineTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tls     bool
		wantTLS bool
	}{
		{name: "tls enabled", tls: true, wantTLS: true},
		{name: "tls disabled", tls: false, wantTLS: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(docdb.EngineConfig{
				Runtime: rt, Probe: readyProbe, StartTimeout: time.Minute, TLS: tt.tls,
			})
			t.Cleanup(b.Close)

			createEngineCluster(t, b, "5.0.0")
			c := waitClusterStatus(t, b, "available")

			specs := rt.createdSpecs()
			require.Len(t, specs, 1)
			assert.Empty(t, specs[0].Mounts)
			assert.Empty(t, specs[0].Cmd)

			if !tt.wantTLS {
				assert.Nil(t, b.TLSCAPEM())

				return
			}

			assert.Contains(t, specs[0].Ports[0], "127.0.0.1:")
			assert.NotContains(t, specs[0].Ports[0], ":"+strconv.Itoa(c.Port)+":")

			pool := caPool(t, b)
			for _, name := range []string{"localhost", "127.0.0.1"} {
				assert.True(t, tlsHandshakeOK(c, pool, name), name)
			}

			assert.False(t, tlsHandshakeOK(c, x509.NewCertPool(), "localhost"), "untrusted CA")

			_, err := b.DeleteDBCluster(t.Context(), "c1", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return !tlsHandshakeOK(c, pool, "localhost") },
				10*time.Second, 5*time.Millisecond)
		})
	}
}

func TestEngineTLSPasswordChangeUsesPlainShell(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: rt, Probe: readyProbe, StartTimeout: time.Minute, TLS: true})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "5.0.0")
	waitClusterStatus(t, b, "available")

	_, err := b.ModifyDBCluster(t.Context(), "c1", "", nil, 0, "", "",
		&docdb.ModifyDBClusterOptions{MasterUserPassword: "n3wpassword!"})
	require.NoError(t, err)

	execs := rt.execCalls()
	require.Len(t, execs, 1)
	assert.NotContains(t, execs[0], "--tls")
}

func TestEngineTLSParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		launchTLS  string
		rebootTLS  string
		wantBefore bool
		wantAfter  bool
	}{
		{name: "default enabled", wantBefore: true, wantAfter: true},
		{name: "disabled at launch", launchTLS: "disabled"},
		{name: "disable on reboot", launchTLS: "enabled", rebootTLS: "disabled", wantBefore: true},
		{name: "enable on reboot", launchTLS: "disabled", rebootTLS: "enabled", wantAfter: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(docdb.EngineConfig{Runtime: rt, Probe: readyProbe, StartTimeout: time.Minute, TLS: true})
			t.Cleanup(b.Close)

			ctx := t.Context()
			_, err := b.CreateDBClusterParameterGroup(ctx, "pg", "docdb5.0", "d", nil)
			require.NoError(t, err)

			if tt.launchTLS != "" {
				_, err = b.ModifyDBClusterParameterGroup(ctx, "pg", map[string]string{"tls": tt.launchTLS})
				require.NoError(t, err)
			}

			_, err = b.CreateDBCluster(ctx, "c1", "docdb", "5.0.0", "admin1", "s3cretpassword",
				"", "pg", "", 0, false, false, 0, "", "", nil, nil, nil)
			require.NoError(t, err)
			c := waitClusterStatus(t, b, "available")

			_, err = b.CreateDBInstance(ctx, "i1", "c1", "", "docdb", 0, nil, nil)
			require.NoError(t, err)

			pool := caPool(t, b)
			assert.Equal(t, tt.wantBefore, tlsHandshakeOK(c, pool, "localhost"))

			if tt.rebootTLS != "" {
				_, err = b.ModifyDBClusterParameterGroup(ctx, "pg", map[string]string{"tls": tt.rebootTLS})
				require.NoError(t, err)
				assert.Equal(t, tt.wantBefore, tlsHandshakeOK(c, pool, "localhost"), "static until reboot")
			}

			_, err = b.RebootDBInstance(ctx, "i1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantAfter, tlsHandshakeOK(c, pool, "localhost"))
		})
	}
}
