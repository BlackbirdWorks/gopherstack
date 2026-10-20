package rds_test

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func startEcho(t *testing.T, port int) {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
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

// echoes reports whether a connection to addr is relayed to the database (true) or dropped (false).
func echoes(t *testing.T, addr string) bool {
	t.Helper()

	c, err := (&net.Dialer{Timeout: engineWait}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return false
	}

	defer func() { _ = c.Close() }()

	require.NoError(t, c.SetDeadline(time.Now().Add(engineWait)))

	if _, err = c.Write([]byte("ping")); err != nil {
		return false
	}

	buf := make([]byte, len("ping"))
	_, err = io.ReadFull(c, buf)

	return err == nil && string(buf) == "ping"
}

func (f *engineFixture) clusterWithMembers(t *testing.T) {
	t.Helper()

	f.readyCluster(t, "c1")
	startEcho(t, hostPort(t, f.rt.spec(0)))

	for _, id := range []string{"w1", "r1"} {
		_, err := f.b.CreateDBInstance(id, "aurora-mysql", "db.r5.large", "", "master", "", 0,
			rds.DBInstanceOptions{DBClusterIdentifier: "c1"})
		require.NoError(t, err)
	}

	rds.FlushInstanceLifecycle(f.b)
}

func (f *engineFixture) endpointAddr(t *testing.T, id string) string {
	t.Helper()

	eps, err := f.b.DescribeDBClusterEndpoints("c1", id)
	require.NoError(t, err)
	require.Len(t, eps, 1)

	return eps[0].Endpoint
}

func TestEngineClusterEndpointListenerRoutesByMembers(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.clusterWithMembers(t)

	_, err := f.b.CreateDBClusterEndpoint("ep-r", "c1", "READER", rds.ClusterEndpointOptions{})
	require.NoError(t, err)
	_, err = f.b.CreateDBClusterEndpoint("ep-w", "c1", "WRITER", rds.ClusterEndpointOptions{})
	require.NoError(t, err)

	readerAddr, writerAddr := f.endpointAddr(t, "ep-r"), f.endpointAddr(t, "ep-w")
	assert.NotEqual(t, readerAddr, writerAddr)
	assert.True(t, echoes(t, readerAddr))
	assert.True(t, echoes(t, writerAddr))

	tests := []struct {
		wantErr error
		name    string
		opts    rds.ClusterEndpointOptions
		wantUp  bool
	}{
		{name: "static member", opts: rds.ClusterEndpointOptions{StaticMembers: []string{"w1"}}, wantUp: true},
		{name: "excluded one", opts: rds.ClusterEndpointOptions{ExcludedMembers: []string{"w1"}}, wantUp: true},
		{name: "excluded all", opts: rds.ClusterEndpointOptions{ExcludedMembers: []string{"w1", "r1"}}},
		{name: "unknown member", opts: rds.ClusterEndpointOptions{StaticMembers: []string{"nope"}},
			wantErr: rds.ErrInstanceNotFound},
		{name: "both lists", opts: rds.ClusterEndpointOptions{
			StaticMembers: []string{"w1"}, ExcludedMembers: []string{"r1"},
		}, wantErr: rds.ErrInvalidParameterCombination},
	}

	for _, tt := range tests {
		_, merr := f.b.ModifyDBClusterEndpoint("ep-r", "", tt.opts)
		if tt.wantErr != nil {
			require.ErrorIs(t, merr, tt.wantErr, tt.name)

			continue
		}

		require.NoError(t, merr, tt.name)
		assert.Equal(t, readerAddr, f.endpointAddr(t, "ep-r"), tt.name)
		assert.Equal(t, tt.wantUp, echoes(t, readerAddr), tt.name)
	}

	_, err = f.b.DeleteDBClusterEndpoint("ep-r")
	require.NoError(t, err)
	assert.False(t, echoes(t, readerAddr))
	assert.True(t, echoes(t, writerAddr))
}

func TestEngineClusterEndpointListenerClosesWithCluster(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.clusterWithMembers(t)

	_, err := f.b.CreateDBClusterEndpoint("ep-any", "c1", "ANY", rds.ClusterEndpointOptions{})
	require.NoError(t, err)

	addr := f.endpointAddr(t, "ep-any")
	require.True(t, echoes(t, addr))

	for _, id := range []string{"w1", "r1"} {
		_, err = f.b.DeleteDBInstance(id)
		require.NoError(t, err)
	}

	_, err = f.b.DeleteDBCluster("c1")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return !echoes(t, addr) }, engineWait, 5*time.Millisecond)
}

func TestClusterEndpointInputValidation(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDBCluster("c1", "aurora-mysql", "master", "", "", 0, nil,
		rds.DBClusterOptions{MasterUserPassword: "pw"})
	require.NoError(t, err)

	_, err = b.CreateDBClusterEndpoint("ep", "c1", "BOGUS", rds.ClusterEndpointOptions{})
	require.ErrorIs(t, err, rds.ErrInvalidParameter)

	ep, err := b.CreateDBClusterEndpoint("ep", "c1", "READER", rds.ClusterEndpointOptions{})
	require.NoError(t, err)
	assert.Contains(t, ep.Endpoint, ".rds.amazonaws.com")
}
