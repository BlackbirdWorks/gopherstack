package memorydb_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func pongs(addr string) bool {
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
			h, b := newDataPlaneHandler(t, memorydb.EngineEmbedded)

			if tt.enforce {
				b.SetSecurityGroupEnforcer(src.Enforcer())
			}

			body := minimalClusterBody("sg1")
			body["SecurityGroupIds"] = []string{"sg-1"}
			addr := connectAddr(t, createClusterObj(t, h, body))

			if !tt.enforce {
				assert.True(t, pongs(addr))

				return
			}

			assert.False(t, pongs(addr), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 6379, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 6379, "127.0.0.1/32").Ingress...)
			src.Set(g)
			assert.True(t, pongs(addr), "authorized loopback connects on the next connection")

			require.Equal(t, 200, doRequest(t, h, "UpdateCluster",
				map[string]any{"ClusterName": "sg1", "SecurityGroupIds": []string{"sg-other"}}).Code)
			assert.False(t, pongs(addr), "swapping the cluster's groups applies to the next connection")
		})
	}
}
