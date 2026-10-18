package memorydb_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

const dpPassword = "passwordpassword1"

type respConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialResp(t *testing.T, addr string) *respConn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return &respConn{conn: conn, r: bufio.NewReader(conn)}
}

// do sends one command and returns the first reply line, plus the bulk body when present.
func (c *respConn) do(t *testing.T, args ...string) string {
	t.Helper()

	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}

	require.NoError(t, c.conn.SetDeadline(time.Now().Add(2*time.Second)))
	_, err := c.conn.Write([]byte(sb.String()))
	require.NoError(t, err)

	line, err := c.r.ReadString('\n')
	require.NoError(t, err)
	line = strings.TrimRight(line, "\r\n")

	if strings.HasPrefix(line, "$") && line != "$-1" {
		n, convErr := strconv.Atoi(line[1:])
		require.NoError(t, convErr)
		buf := make([]byte, n+2)
		_, err = c.r.Read(buf)
		require.NoError(t, err)

		return string(buf[:n])
	}

	return line
}

func newDataPlaneHandler(t *testing.T, mode string) (*memorydb.Handler, *memorydb.InMemoryBackend) {
	t.Helper()

	b := newTestBackend()
	b.SetEngine(mode, nil)
	t.Cleanup(b.Reset)

	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h, b
}

func clusterEndpointAddr(t *testing.T, cl map[string]any) (string, float64) {
	t.Helper()

	return endpointOf(t, cl, "ClusterEndpoint")
}

func endpointOf(t *testing.T, obj map[string]any, key string) (string, float64) {
	t.Helper()

	ep, ok := obj[key].(map[string]any)
	require.True(t, ok, "no %s", key)

	host, _ := ep["Address"].(string)
	port, _ := ep["Port"].(float64)

	return host, port
}

func connectAddr(t *testing.T, cl map[string]any) string {
	t.Helper()

	host, port := clusterEndpointAddr(t, cl)

	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

func describeClusterObj(t *testing.T, h *memorydb.Handler, name string) map[string]any {
	t.Helper()

	rec := doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": name, "ShowShardDetails": true})
	require.Equal(t, 200, rec.Code, rec.Body.String())

	var resp struct {
		Clusters []map[string]any `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Clusters, 1)

	return resp.Clusters[0]
}

func portClosed(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return true
	}
	_ = conn.Close()

	return false
}

func TestDataPlane_SetGetAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "single_shard", body: minimalClusterBody("dp-single")},
		{
			name: "multi_shard_with_replicas",
			body: map[string]any{
				"ClusterName": "dp-multi", "NodeType": "db.r6g.large", "ACLName": "open-access",
				"NumShards": 2, "NumReplicasPerShard": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newDataPlaneHandler(t, memorydb.EngineEmbedded)
			name, _ := tt.body["ClusterName"].(string)
			cl := createClusterObj(t, h, tt.body)

			addr := connectAddr(t, cl)
			host, port := clusterEndpointAddr(t, cl)
			assert.Equal(t, "127.0.0.1", host)
			assert.NotEqual(t, float64(6379), port)

			c := dialResp(t, addr)
			assert.Equal(t, "+OK", c.do(t, "SET", "k", "v"))
			assert.Equal(t, "v", c.do(t, "GET", "k"))

			desc := describeClusterObj(t, h, name)
			assert.Equal(t, addr, connectAddr(t, desc))

			shards, _ := desc["Shards"].([]any)
			require.NotEmpty(t, shards)
			for _, s := range shards {
				nodes, _ := s.(map[string]any)["Nodes"].([]any)
				for _, n := range nodes {
					nh, np := endpointOf(t, n.(map[string]any), "Endpoint")
					assert.Equal(t, addr, net.JoinHostPort(nh, strconv.Itoa(int(np))))
				}
			}

			rec := doRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": name})
			require.Equal(t, 200, rec.Code, rec.Body.String())
			assert.Eventually(t, func() bool { return portClosed(addr) }, 2*time.Second, 10*time.Millisecond)
		})
	}
}

func TestDataPlane_StubKeepsSyntheticEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "stub", mode: memorydb.EngineStub},
		{name: "docker", mode: memorydb.EngineDocker},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newDataPlaneHandler(t, tt.mode)
			cl := createClusterObj(t, h, minimalClusterBody("dp-stub"))
			host, port := clusterEndpointAddr(t, cl)
			assert.Equal(t, "dp-stub.memorydb.us-east-1.amazonaws.com", host)
			assert.InDelta(t, 6379, port, 0)
		})
	}
}

func TestDataPlane_ResetAndRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reset func(t *testing.T, b *memorydb.InMemoryBackend, oldAddr string, h *memorydb.Handler)
		name  string
	}{
		{
			name: "reset_closes_port",
			reset: func(t *testing.T, b *memorydb.InMemoryBackend, oldAddr string, _ *memorydb.Handler) {
				t.Helper()
				b.Reset()
				assert.True(t, portClosed(oldAddr))
			},
		},
		{
			name: "restore_restarts_server",
			reset: func(t *testing.T, b *memorydb.InMemoryBackend, oldAddr string, h *memorydb.Handler) {
				t.Helper()
				data := b.Snapshot(context.Background())
				require.NotNil(t, data)
				require.NoError(t, b.Restore(context.Background(), data))
				assert.True(t, portClosed(oldAddr))

				cl := describeClusterObj(t, h, "dp-restore")
				c := dialResp(t, connectAddr(t, cl))
				assert.Equal(t, "+OK", c.do(t, "SET", "a", "b"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newDataPlaneHandler(t, memorydb.EngineEmbedded)
			cl := createClusterObj(t, h, minimalClusterBody("dp-restore"))
			addr := connectAddr(t, cl)
			require.False(t, portClosed(addr))

			tt.reset(t, b, addr, h)
		})
	}
}

func TestDataPlane_ACLAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantAuth   string
		authArgs   []string
		wantAnon   bool
		wantAccess bool
	}{
		{name: "valid_password", authArgs: []string{"AUTH", "alice", dpPassword}, wantAuth: "+OK", wantAccess: true},
		{name: "wrong_password", authArgs: []string{"AUTH", "alice", "nottherightpass123"}, wantAuth: "-WRONGPASS"},
		{name: "no_auth", wantAnon: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newDataPlaneHandler(t, memorydb.EngineEmbedded)

			rec := doRequest(t, h, "CreateUser", map[string]any{
				"UserName": "alice", "AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{"Type": "password", "Passwords": []string{dpPassword}},
			})
			require.Equal(t, 200, rec.Code, rec.Body.String())
			rec = doRequest(t, h, "CreateACL", map[string]any{"ACLName": "secure", "UserNames": []string{"alice"}})
			require.Equal(t, 200, rec.Code, rec.Body.String())

			cl := createClusterObj(t, h, map[string]any{
				"ClusterName": "dp-auth", "NodeType": "db.r6g.large", "ACLName": "secure",
			})
			c := dialResp(t, connectAddr(t, cl))

			if tt.wantAnon {
				assert.True(t, strings.HasPrefix(c.do(t, "GET", "k"), "-NOAUTH"))

				return
			}

			assert.True(t, strings.HasPrefix(c.do(t, tt.authArgs...), tt.wantAuth))
			if tt.wantAccess {
				assert.Equal(t, "+OK", c.do(t, "SET", "k", "v"))
			} else {
				assert.True(t, strings.HasPrefix(c.do(t, "GET", "k"), "-NOAUTH"))
			}
		})
	}
}

func TestDataPlane_ACLChangeResyncs(t *testing.T) {
	t.Parallel()

	h, _ := newDataPlaneHandler(t, memorydb.EngineEmbedded)

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName": "bob", "AccessString": "on ~* +@all",
		"AuthenticationMode": map[string]any{"Type": "password", "Passwords": []string{dpPassword}},
	})
	require.Equal(t, 200, rec.Code, rec.Body.String())
	rec = doRequest(t, h, "CreateACL", map[string]any{"ACLName": "later", "UserNames": []string{"bob"}})
	require.Equal(t, 200, rec.Code, rec.Body.String())

	cl := createClusterObj(t, h, minimalClusterBody("dp-resync"))
	addr := connectAddr(t, cl)
	assert.Equal(t, "+OK", dialResp(t, addr).do(t, "SET", "k", "v"))

	rec = doRequest(t, h, "UpdateCluster", map[string]any{"ClusterName": "dp-resync", "ACLName": "later"})
	require.Equal(t, 200, rec.Code, rec.Body.String())

	assert.True(t, strings.HasPrefix(dialResp(t, addr).do(t, "GET", "k"), "-NOAUTH"))
}
