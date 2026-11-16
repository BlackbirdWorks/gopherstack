package ocimysql_test

import (
	"bufio"
	"encoding/json/v2"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// greetOn starts a listener that answers every connection with name.
func greetOn(t *testing.T, name string) net.Listener {
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

			_, _ = c.Write([]byte(name + "\n"))
			_ = c.Close()
		}
	}()

	return ln
}

func greeting(t *testing.T, host string, port int) string {
	t.Helper()

	var d net.Dialer

	c, err := d.DialContext(t.Context(), "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	require.NoError(t, err)

	defer c.Close()

	line, _ := bufio.NewReader(c).ReadString('\n')

	return line
}

func TestReadEndpointRoundRobin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		exclude bool
		want    int
	}{
		{name: "both replicas", want: 2},
		{name: "one excluded", exclude: true, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newFakeLauncher()

			var mu sync.Mutex

			ports := map[string]int{}

			l.portOf = func(id string) int {
				mu.Lock()
				defer mu.Unlock()

				if _, ok := ports[id]; !ok {
					if a, isTCP := greetOn(t, id).Addr().(*net.TCPAddr); isTCP {
						ports[id] = a.Port
					}
				}

				return ports[id]
			}

			e := newEnv(t, envOpts{launcher: l})
			db := e.mkEngineDB(t, "primary", func(d *my.CreateDbSystemDetails) { d.ShapeName = new(shapeReplica) })

			var r1, r2 my.CreateReplicaResponse

			r1, r2 = e.mkReplica(t, db.Id, "r1"), e.mkReplica(t, db.Id, "r2")

			for _, id := range []*string{r1.Id, r2.Id} {
				require.Eventually(t, func() bool {
					r, err := e.rp.GetReplica(t.Context(), my.GetReplicaRequest{ReplicaId: id})

					return err == nil && r.LifecycleState == my.ReplicaLifecycleStateActive
				}, e.waitFor(), 20*time.Millisecond)
			}

			if tt.exclude {
				_, err := e.db.UpdateDbSystem(t.Context(), my.UpdateDbSystemRequest{
					DbSystemId:   db.Id,
					ReadEndpoint: &my.UpdateReadEndpointDetails{ExcludeIps: []string{replicaSubnetIP(t, e, *r2.Id)}},
				})
				require.NoError(t, err)
			}

			var lb *my.DbSystemEndpoint

			for _, ep := range e.getDB(t, db.Id).Endpoints {
				if ep.ResourceType == my.DbSystemEndpointResourceTypeLoadBalancer {
					lb = &ep
				}
			}

			require.NotNil(t, lb, "the first replica creates the read endpoint")

			seen := map[string]bool{}
			for range 6 {
				seen[greeting(t, *lb.IpAddress, *lb.Port)] = true
			}

			assert.Len(t, seen, tt.want, "connections spread over the read replicas that are not excluded")
		})
	}
}

// replicaSubnetIP reads the address a replica holds in its subnet from the persisted state; under the engine its
// endpoint shows the server's loopback address instead.
func replicaSubnetIP(t *testing.T, e *env, id string) string {
	t.Helper()

	var snap struct {
		Replicas map[string]struct {
			IP string `json:"ipAddress"`
		} `json:"replicas"`
	}

	require.NoError(t, json.Unmarshal(e.backend.Snapshot(t.Context()), &snap, jsonx.V1()))

	return snap.Replicas[id].IP
}
