package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	inst := src.createInstance(t, "keep")
	src.createDB(t, "keep", "CREATE TABLE t (id INT64) PRIMARY KEY (id)")

	_, err := src.instances.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: inst.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@b.c"}}}},
	})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.instances.GetInstance(t.Context(), &instancepb.GetInstanceRequest{Name: inst.GetName()})
	require.NoError(t, err)
	assert.Equal(t, instancepb.Instance_READY, got.GetState())

	pol, err := dst.instances.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: inst.GetName()})
	require.NoError(t, err)
	assert.Len(t, pol.GetBindings(), 1)

	ddl, err := dst.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{
		Database: inst.GetName() + "/databases/app",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE TABLE t (id INT64) PRIMARY KEY (id)"}, ddl.GetStatements())
}

func TestPersistence_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{"garbage", []byte("not json")},
		{"empty", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			assert.Error(t, e.handler.Restore(t.Context(), tt.data))
		})
	}
}

func TestReset_ClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.createInstance(t, "gone")
	e.handler.Reset()

	_, err := e.instances.GetInstance(t.Context(), &instancepb.GetInstanceRequest{Name: inst.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPersistence_ConfigsAndPartitions(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	cfg := src.createConfig(t, "custom-nam3-ro", nam3Custom(roReplica("us-west1")))
	inst := src.createInstance(t, "parts")

	op, err := src.instances.CreateInstancePartition(t.Context(), &instancepb.CreateInstancePartitionRequest{
		Parent: inst.GetName(), InstancePartitionId: "east",
		InstancePartition: &instancepb.InstancePartition{
			Config:          configRef,
			DisplayName:     "East part",
			ComputeCapacity: &instancepb.InstancePartition_NodeCount{NodeCount: 2},
		},
	})
	require.NoError(t, err)

	part, err := await(t, op)
	require.NoError(t, err)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	gotCfg, err := dst.instances.GetInstanceConfig(
		t.Context(),
		&instancepb.GetInstanceConfigRequest{Name: cfg.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, cfg.GetEtag(), gotCfg.GetEtag())
	assert.Equal(t, instancepb.InstanceConfig_USER_MANAGED, gotCfg.GetConfigType())
	assert.Len(t, gotCfg.GetReplicas(), 6)

	gotPart, err := dst.instances.GetInstancePartition(
		t.Context(),
		&instancepb.GetInstancePartitionRequest{Name: part.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(2), gotPart.GetNodeCount())
	assert.Equal(t, part.GetEtag(), gotPart.GetEtag())
}
