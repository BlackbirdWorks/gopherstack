package elasticache_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func pingOK(t *testing.T, addr string) bool {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	line, _ := bufio.NewReader(conn).ReadString('\n')

	return line == "+PONG\r\n"
}

func TestClusterSecurityGroups(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		name    string
		enforce bool
	}{
		{"enforced refuses until authorized", true},
		{"flag off is open", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sgtest.New(sgtest.OpenTo("sg-1", 6379, "203.0.113.0/24"))
			b := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)
			t.Cleanup(b.Reset)

			if tt.enforce {
				b.SetSecurityGroupEnforcer(src.Enforcer())
			}

			c, err := b.CreateCluster(ctx, "c1", "redis", "", 0)
			require.NoError(t, err)

			if !tt.enforce {
				assert.True(t, pingOK(t, c.ConnectAddress))

				return
			}

			assert.False(t, pingOK(t, c.ConnectAddress), "no groups and no default group denies")

			_, err = b.ApplyClusterSettings(ctx, "c1", elasticache.ClusterSettings{SecurityGroupIDs: []string{"sg-1"}})
			require.NoError(t, err)
			assert.False(t, pingOK(t, c.ConnectAddress), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 6379, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 6379, "127.0.0.1/32").Ingress...)
			src.Set(g)

			assert.True(t, pingOK(t, c.ConnectAddress), "authorized loopback connects on the next connection")

			require.NoError(t, b.DeleteCluster(ctx, "c1"))
			assert.False(t, pingOK(t, c.ConnectAddress), "relay closes with the cluster")
		})
	}
}
