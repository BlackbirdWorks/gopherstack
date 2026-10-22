package integration_test

import (
	"testing"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	spannerParent = "projects/" + gcpProject
	spannerConfig = spannerParent + "/instanceConfigs/regional-us-central1"
)

func TestIntegration_GCP_Spanner_AdminLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	opts := gcpGRPCOpts(t, "8135")
	ctx := t.Context()

	ic, err := instance.NewInstanceAdminClient(ctx, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ic.Close() })

	dc, err := database.NewDatabaseAdminClient(ctx, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	id := gcpName("it-spanner")
	iop, err := ic.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent: spannerParent, InstanceId: id,
		Instance: &instancepb.Instance{Config: spannerConfig, DisplayName: "integration", ProcessingUnits: 100},
	})
	require.NoError(t, err)

	inst, err := iop.Wait(ctx)
	require.NoError(t, err)
	assert.Equal(t, instancepb.Instance_READY, inst.GetState())
	assert.Equal(t, int32(100), inst.GetProcessingUnits())

	t.Cleanup(func() {
		_ = ic.DeleteInstance(t.Context(), &instancepb.DeleteInstanceRequest{Name: inst.GetName()})
	})

	uop, err := ic.UpdateInstance(ctx, &instancepb.UpdateInstanceRequest{
		Instance:  &instancepb.Instance{Name: inst.GetName(), DisplayName: "renamed", NodeCount: 2},
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "node_count"}},
	})
	require.NoError(t, err)

	updated, err := uop.Wait(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(2000), updated.GetProcessingUnits())

	dop, err := dc.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent: inst.GetName(), CreateStatement: "CREATE DATABASE `app`",
		ExtraStatements: []string{"CREATE TABLE t (id INT64 NOT NULL) PRIMARY KEY (id)"},
	})
	require.NoError(t, err)

	db, err := dop.Wait(ctx)
	require.NoError(t, err)
	assert.Equal(t, inst.GetName()+"/databases/app", db.GetName())

	ddl, err := dc.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: db.GetName()})
	require.NoError(t, err)
	assert.Len(t, ddl.GetStatements(), 1)

	_, err = ic.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: spannerParent + "/instances/never-created"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	require.NoError(t, dc.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: db.GetName()}))
}

// The gopherstack container has no container runtime socket, so Spanner runs metadata only here and the data
// plane reports UNAVAILABLE; the emulator-backed path is covered by services/gcpspanner's docker test.
func TestIntegration_GCP_Spanner_DataPlaneNeedsRuntime(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()

	ic, err := instance.NewInstanceAdminClient(ctx, gcpGRPCOpts(t, "8135")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ic.Close() })

	op, err := ic.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent: spannerParent, InstanceId: gcpName("it-sp-dp"),
		Instance: &instancepb.Instance{Config: spannerConfig, DisplayName: "dataplane", NodeCount: 1},
	})
	require.NoError(t, err)

	inst, err := op.Wait(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ic.DeleteInstance(t.Context(), &instancepb.DeleteInstanceRequest{Name: inst.GetName()})
	})

	cc, err := grpc.NewClient(gcpAddr(t, "8135"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	_, err = spannerpb.NewSpannerClient(cc).CreateSession(ctx, &spannerpb.CreateSessionRequest{
		Database: inst.GetName() + "/databases/app",
	})

	got := status.Code(err)
	assert.Contains(t, []codes.Code{codes.Unavailable, codes.OK}, got, "unexpected data-plane status: %v", err)
}
