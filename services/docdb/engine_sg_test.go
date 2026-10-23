package docdb_test

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// echoAt serves an echo listener where the fake runtime says the container publishes its port.
func echoAt(t *testing.T, spec container.Spec) {
	t.Helper()

	ip, hostPort, _, err := container.ParsePortSpec(spec.Ports[0])
	require.NoError(t, err)

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", net.JoinHostPort(ip.String(), hostPort))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go func() {
				defer func() { _ = c.Close() }()

				_, _ = io.Copy(c, c)
			}()
		}
	}()
}

func echoes(t *testing.T, c docdb.DBCluster) bool {
	t.Helper()

	conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(t.Context(), "tcp",
		net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)))
	if err != nil {
		return false
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = conn.Write([]byte("x"))

	buf := make([]byte, 1)
	n, _ := conn.Read(buf)

	return n == 1
}

func TestEngineSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tls     bool
		enforce bool
	}{
		{"plaintext enforced", false, true},
		{"tls enforced", true, true},
		{"plaintext flag off publishes directly", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sgtest.New(sgtest.OpenTo("sg-1", 27017, "203.0.113.0/24"))
			rt := &fakeEngineRuntime{onCreate: func(spec container.Spec) { echoAt(t, spec) }}
			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(docdb.EngineConfig{Runtime: rt, Probe: readyProbe, StartTimeout: time.Minute, TLS: tt.tls})
			t.Cleanup(b.Close)

			if tt.enforce {
				b.SetSecurityGroupEnforcer(src.Enforcer())
			}

			_, err := b.CreateDBCluster(t.Context(), "c1", "docdb", "5.0.0", "admin1", "s3cretpassword",
				"", "", "", 0, false, false, 0, "", "", nil, nil,
				&docdb.CreateDBClusterOptions{VpcSecurityGroupIDs: []string{"sg-1"}})
			require.NoError(t, err)

			c := waitClusterStatus(t, b, "available")
			reach := func() bool { return echoes(t, c) }

			if tt.tls {
				pool := caPool(t, b)
				reach = func() bool { return tlsHandshakeOK(c, pool, "localhost") }
			}

			if !tt.enforce {
				assert.Contains(t, rt.createdSpecs()[0].Ports[0], ":"+strconv.Itoa(c.Port)+":",
					"with the flag off the container publishes the endpoint port itself")
				assert.True(t, reach())

				return
			}

			assert.Contains(t, rt.createdSpecs()[0].Ports[0], "127.0.0.1:")
			assert.False(t, reach(), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 27017, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 27017, "127.0.0.1/32").Ingress...)
			src.Set(g)
			assert.True(t, reach(), "authorized loopback connects on the next connection")
		})
	}
}
