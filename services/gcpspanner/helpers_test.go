package gcpspanner_test

import (
	"context"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const (
	project   = "test-project"
	parent    = "projects/" + project
	configRef = parent + "/instanceConfigs/regional-us-central1"
)

type env struct {
	handler   *gcpspanner.Handler
	instances *instance.InstanceAdminClient
	databases *database.DatabaseAdminClient
	opts      []option.ClientOption
}

func newEnvWith(t *testing.T, setup func(*gcpspanner.InMemoryBackend)) *env {
	t.Helper()

	h := gcpspanner.NewHandler(gcpspanner.NewInMemoryBackend())
	h.Port = 0

	if setup != nil {
		setup(h.Backend)
	}

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	ic, err := instance.NewInstanceAdminClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ic.Close() })

	dc, err := database.NewDatabaseAdminClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	return &env{handler: h, instances: ic, databases: dc, opts: opts}
}

func newEnv(t *testing.T) *env {
	t.Helper()

	return newEnvWith(t, nil)
}

type poller[T any] interface {
	Poll(ctx context.Context, opts ...gax.CallOption) (T, error)
	Done() bool
}

func await[T any](t *testing.T, op poller[T]) (T, error) {
	t.Helper()

	var (
		out T
		err error
	)

	require.Eventually(t, func() bool {
		out, err = op.Poll(t.Context())

		return op.Done()
	}, 10*time.Second, 2*time.Millisecond)

	return out, err
}

func (e *env) createInstance(t *testing.T, id string) *instancepb.Instance {
	t.Helper()

	op, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
		Parent: parent, InstanceId: id,
		Instance: &instancepb.Instance{Config: configRef, DisplayName: "Test " + id, NodeCount: 1},
	})
	require.NoError(t, err)

	inst, err := await(t, op)
	require.NoError(t, err)

	return inst
}

func deleteReq(name string) *instancepb.DeleteInstanceRequest {
	return &instancepb.DeleteInstanceRequest{Name: name}
}
