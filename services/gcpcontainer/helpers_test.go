//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

const (
	project  = "test-project"
	zone     = "us-central1-a"
	region   = "us-central1"
	zoneLoc  = "projects/" + project + "/locations/" + zone
	regionLc = "projects/" + project + "/locations/" + region
)

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type env struct {
	h     *gcpcontainer.Handler
	b     *gcpcontainer.InMemoryBackend
	clock *fakeClock
	gc    *container.ClusterManagerClient
	rc    *container.ClusterManagerClient
}

func newEnvWith(t *testing.T, b *gcpcontainer.InMemoryBackend) *env {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
	b.SetClock(clock.Now)

	h := gcpcontainer.NewHandler(b)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	gc, err := container.NewClusterManagerClient(t.Context(),
		option.WithGRPCConn(conn), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })

	rc, err := container.NewClusterManagerRESTClient(
		t.Context(),
		option.WithEndpoint(
			"http://"+h.Addr(),
		),
		option.WithoutAuthentication(),
		option.WithHTTPClient(testclient.New(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })

	return &env{h: h, b: b, clock: clock, gc: gc, rc: rc}
}

func newEnv(t *testing.T) *env {
	t.Helper()

	return newEnvWith(t, gcpcontainer.NewInMemoryBackend())
}

func (e *env) settle() { e.clock.Advance(time.Minute) }

func clusterPath(loc, name string) string {
	return "projects/" + project + "/locations/" + loc + "/clusters/" + name
}

func poolPath(loc, cluster, pool string) string {
	return clusterPath(loc, cluster) + "/nodePools/" + pool
}

func (e *env) create(t *testing.T, loc string, cl *containerpb.Cluster) *containerpb.Operation {
	t.Helper()

	op, err := e.gc.CreateCluster(t.Context(), &containerpb.CreateClusterRequest{
		Parent: "projects/" + project + "/locations/" + loc, Cluster: cl,
	})
	require.NoError(t, err)

	return op
}

func stdCluster(name string) *containerpb.Cluster {
	return &containerpb.Cluster{Name: name, InitialNodeCount: 2}
}

func (e *env) ready(t *testing.T, loc string, cl *containerpb.Cluster) *containerpb.Cluster {
	t.Helper()

	e.create(t, loc, cl)
	e.settle()

	got, err := e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(loc, cl.GetName())})
	require.NoError(t, err)
	require.Equal(t, containerpb.Cluster_RUNNING, got.GetStatus())

	return got
}

func (e *env) getCluster(t *testing.T, loc, name string) *containerpb.Cluster {
	t.Helper()

	got, err := e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(loc, name)})
	require.NoError(t, err)

	return got
}

func (e *env) getPool(t *testing.T, loc, cluster, pool string) *containerpb.NodePool {
	t.Helper()

	got, err := e.gc.GetNodePool(t.Context(), &containerpb.GetNodePoolRequest{Name: poolPath(loc, cluster, pool)})
	require.NoError(t, err)

	return got
}

func (e *env) getOp(t *testing.T, op *containerpb.Operation) *containerpb.Operation {
	t.Helper()

	got, err := e.gc.GetOperation(t.Context(), &containerpb.GetOperationRequest{
		Name: zoneLoc + "/operations/" + op.GetName(),
	})
	require.NoError(t, err)

	return got
}

// code reads the gRPC code of a gRPC or REST client error.
func code(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	if ae, ok := errors.AsType[*apierror.APIError](err); ok {
		switch ae.HTTPCode() {
		case http.StatusNotFound:
			return codes.NotFound
		case http.StatusConflict:
			return codes.AlreadyExists
		case http.StatusBadRequest:
			return codes.InvalidArgument
		case http.StatusPreconditionFailed:
			return codes.FailedPrecondition
		}

		if st := ae.GRPCStatus(); st != nil {
			return st.Code()
		}
	}

	return status.Code(err)
}

func assertCode(t *testing.T, want codes.Code, err error, msgContains string) {
	t.Helper()

	require.Error(t, err)
	assert.Equal(t, want, code(err), err.Error())

	if msgContains != "" {
		assert.Contains(t, err.Error(), msgContains)
	}
}

func hasDetail[T any](err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, d := range st.Details() {
		if _, is := d.(T); is {
			return true
		}
	}

	return false
}
