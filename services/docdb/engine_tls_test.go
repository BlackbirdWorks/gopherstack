package docdb_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

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

			var (
				mu     sync.Mutex
				server []byte
				ca     []byte
				dir    string
			)

			rt := &fakeEngineRuntime{onCreate: func(spec container.Spec) {
				if len(spec.Mounts) == 0 {
					return
				}

				mu.Lock()
				defer mu.Unlock()

				dir = strings.SplitN(spec.Mounts[0], ":", 2)[0]
				server, _ = os.ReadFile(filepath.Join(dir, "server.pem"))
				ca, _ = os.ReadFile(filepath.Join(dir, "ca.pem"))
			}}

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(docdb.EngineConfig{
				Runtime: rt, Probe: readyProbe, StartTimeout: time.Minute, TLS: tt.tls,
			})
			t.Cleanup(b.Close)

			createEngineCluster(t, b, "5.0.0")
			waitClusterStatus(t, b, "available")

			specs := rt.createdSpecs()
			require.Len(t, specs, 1)

			if !tt.wantTLS {
				assert.Empty(t, specs[0].Mounts)
				assert.Empty(t, specs[0].Cmd)
				assert.Nil(t, b.TLSCAPEM())

				return
			}

			assert.Equal(t, "requireTLS", specs[0].Cmd[2])
			assert.Contains(t, specs[0].Cmd, "/gopherstack-tls/server.pem")
			assert.Equal(t, b.TLSCAPEM(), ca)

			mu.Lock()
			srvPEM, mountDir := append([]byte(nil), server...), dir
			mu.Unlock()

			pair, err := tls.X509KeyPair(srvPEM, srvPEM)
			require.NoError(t, err)

			leaf, err := x509.ParseCertificate(pair.Certificate[0])
			require.NoError(t, err)

			pool := x509.NewCertPool()
			block, _ := pem.Decode(b.TLSCAPEM())
			require.NotNil(t, block)

			caCert, err := x509.ParseCertificate(block.Bytes)
			require.NoError(t, err)
			pool.AddCert(caCert)

			for _, name := range []string{"localhost", "127.0.0.1"} {
				_, err = leaf.Verify(x509.VerifyOptions{
					Roots: pool, DNSName: name, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				})
				require.NoError(t, err, name)
			}

			_, err = b.DeleteDBCluster(t.Context(), "c1", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				_, serr := os.Stat(mountDir)

				return os.IsNotExist(serr)
			}, 10*time.Second, 5*time.Millisecond)
		})
	}
}

func TestEngineTLSPasswordChangeUsesTLSShell(t *testing.T) {
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
	assert.Contains(t, execs[0], "--tls")
	assert.Contains(t, execs[0], "/gopherstack-tls/ca.pem")
}

func TestEngineDockerRealMongoTLSAndSCRAM(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(docdb.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rec := &recordingEngineRuntime{EngineRuntime: er}
	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(docdb.EngineConfig{Runtime: rec, TLS: true})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "5.0.0")

	c := waitClusterStatusFor(t, b, "available", dockerEngineWait)
	require.Len(t, rec.containerIDs(), 1)
	id := rec.containerIDs()[0]
	addr := "127.0.0.1:" + strconv.Itoa(c.Port)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(b.TLSCAPEM()))

	conn, err := (&tls.Dialer{Config: &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}}).
		DialContext(
			t.Context(),
			"tcp",
			addr,
		)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	shell := func(ctx context.Context, user, pass, mech string) container.ExecResult {
		res, eerr := er.Exec(ctx, id, []string{
			"mongo", "--quiet", "--tls", "--tlsCAFile", "/gopherstack-tls/ca.pem", "--host", "localhost",
			"-u", user, "-p", pass, "--authenticationDatabase", "admin", "--authenticationMechanism", mech,
			"--eval", "db.runCommand({connectionStatus:1}).authInfo.authenticatedUsers[0].user",
		})
		require.NoError(t, eerr)

		return res
	}

	for _, mech := range []string{"SCRAM-SHA-1", "SCRAM-SHA-256"} {
		good := shell(t.Context(), "admin1", "s3cretpassword", mech)
		assert.Equal(t, 0, good.ExitCode, mech+": "+good.Stderr+good.Stdout)
		assert.Contains(t, good.Stdout, "admin1")

		bad := shell(t.Context(), "admin1", "wrong", mech)
		assert.NotEqual(t, 0, bad.ExitCode, mech)
	}
}
