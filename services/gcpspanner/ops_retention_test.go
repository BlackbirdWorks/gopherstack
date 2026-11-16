package gcpspanner_test

import (
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func TestOperations_SweptOnBackendClock(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 0)
	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.SetClock(func() time.Time { return now }) })

	op, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
		Parent: parent, InstanceId: "old",
		Instance: &instancepb.Instance{Config: configRef, DisplayName: "Old one", NodeCount: 1},
	})
	require.NoError(t, err)

	_, err = await(t, op)
	require.NoError(t, err)

	cc, err := grpc.NewClient(e.handler.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	lro := longrunningpb.NewOperationsClient(cc)
	_, err = lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)

	now = now.Add(gcplro.DefaultMaxAge + 2*time.Hour)
	e.createInstance(t, "fresh")

	_, err = lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
