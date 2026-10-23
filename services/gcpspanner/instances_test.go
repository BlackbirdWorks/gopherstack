package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestInstance_CreateValidation(t *testing.T) {
	t.Parallel()

	good := func() *instancepb.Instance {
		return &instancepb.Instance{Config: configRef, DisplayName: "Valid name", NodeCount: 1}
	}

	tests := []struct {
		mutate func(*instancepb.CreateInstanceRequest)
		name   string
		want   codes.Code
	}{
		{func(r *instancepb.CreateInstanceRequest) { r.InstanceId = "A" }, "bad id", codes.InvalidArgument},
		{func(r *instancepb.CreateInstanceRequest) { r.Parent = "projects" }, "bad parent", codes.InvalidArgument},
		{
			func(r *instancepb.CreateInstanceRequest) { r.Instance.DisplayName = "ab" },
			"short name",
			codes.InvalidArgument,
		},
		{
			func(r *instancepb.CreateInstanceRequest) { r.Instance.Config = parent + "/instanceConfigs/nope" },
			"unknown config",
			codes.NotFound,
		},
		{
			func(r *instancepb.CreateInstanceRequest) { r.Instance.Config = "regional-us-central1" },
			"short config",
			codes.InvalidArgument,
		},
		{func(r *instancepb.CreateInstanceRequest) { r.Instance.NodeCount = 0 }, "no capacity", codes.InvalidArgument},
		{
			func(r *instancepb.CreateInstanceRequest) { r.Instance.ProcessingUnits = 100 },
			"both capacities",
			codes.InvalidArgument,
		},
		{func(r *instancepb.CreateInstanceRequest) {
			r.Instance.NodeCount, r.Instance.ProcessingUnits = 0, 150
		}, "bad pu step", codes.InvalidArgument},
		{func(r *instancepb.CreateInstanceRequest) {
			r.Instance.AutoscalingConfig = &instancepb.AutoscalingConfig{}
		}, "autoscaling", codes.Unimplemented},
		{func(*instancepb.CreateInstanceRequest) {}, "ok", codes.OK},
	}

	e := newEnv(t)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := &instancepb.CreateInstanceRequest{
				Parent:     parent,
				InstanceId: "val-" + string(rune('a'+i)),
				Instance:   good(),
			}
			tt.mutate(req)

			_, err := e.instances.CreateInstance(t.Context(), req)
			assert.Equal(t, tt.want, status.Code(err))
		})
	}
}

func TestInstance_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.createInstance(t, "life")

	assert.Equal(t, parent+"/instances/life", inst.GetName())
	assert.Equal(t, instancepb.Instance_READY, inst.GetState())
	assert.Equal(t, int32(1000), inst.GetProcessingUnits())
	assert.Equal(t, instancepb.Instance_STANDARD, inst.GetEdition())

	_, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
		Parent: parent, InstanceId: "life",
		Instance: &instancepb.Instance{Config: configRef, DisplayName: "dup name", NodeCount: 1},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	uop, err := e.instances.UpdateInstance(t.Context(), &instancepb.UpdateInstanceRequest{
		Instance: &instancepb.Instance{
			Name:            inst.GetName(),
			DisplayName:     "Renamed",
			ProcessingUnits: 300,
			Labels:          map[string]string{"a": "b"},
		},
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "processing_units", "labels"}},
	})
	require.NoError(t, err)

	updated, err := await(t, uop)
	require.NoError(t, err)
	assert.Equal(t, "Renamed", updated.GetDisplayName())
	assert.Equal(t, int32(300), updated.GetProcessingUnits())
	assert.Equal(t, int32(0), updated.GetNodeCount())

	got, err := e.instances.GetInstance(t.Context(), &instancepb.GetInstanceRequest{Name: inst.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "b", got.GetLabels()["a"])

	_, err = e.instances.UpdateInstance(t.Context(), &instancepb.UpdateInstanceRequest{
		Instance:  &instancepb.Instance{Name: inst.GetName(), Config: configRef},
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"config"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	it := e.instances.ListInstances(t.Context(), &instancepb.ListInstancesRequest{Parent: parent})
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, inst.GetName(), first.GetName())

	require.NoError(t, e.instances.DeleteInstance(t.Context(), &instancepb.DeleteInstanceRequest{Name: inst.GetName()}))

	_, err = e.instances.GetInstance(t.Context(), &instancepb.GetInstanceRequest{Name: inst.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestInstanceConfigs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name string
		id   string
		want codes.Code
	}{
		{"emulator config", "emulator-config", codes.OK},
		{"regional", "regional-europe-west1", codes.OK},
		{"multi region", "nam3", codes.OK},
		{"unknown", "regional-mars-1", codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := e.instances.GetInstanceConfig(t.Context(), &instancepb.GetInstanceConfigRequest{
				Name: parent + "/instanceConfigs/" + tt.id,
			})
			assert.Equal(t, tt.want, status.Code(err))

			if tt.want == codes.OK {
				assert.Equal(t, instancepb.InstanceConfig_READY, cfg.GetState())
				assert.NotEmpty(t, cfg.GetReplicas())
			}
		})
	}

	it := e.instances.ListInstanceConfigs(t.Context(), &instancepb.ListInstanceConfigsRequest{Parent: parent})

	count := 0

	for {
		if _, err := it.Next(); err != nil {
			break
		}

		count++
	}

	assert.Greater(t, count, 30)
}

func TestInstance_IAM(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.createInstance(t, "iam-inst")
	ctx := t.Context()

	empty, err := e.instances.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: inst.GetName()})
	require.NoError(t, err)
	assert.Empty(t, empty.GetBindings())

	set, err := e.instances.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: inst.GetName(),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{
			{Role: "roles/spanner.databaseUser", Members: []string{"user:a@example.com"}},
		}},
	})
	require.NoError(t, err)
	assert.NotEqual(t, empty.GetEtag(), set.GetEtag())

	_, err = e.instances.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: inst.GetName(), Policy: &iampb.Policy{Etag: []byte("stale")},
	})
	assert.Equal(t, codes.Aborted, status.Code(err))

	_, err = e.instances.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: inst.GetName(), Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/x"}}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	perms, err := e.instances.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: inst.GetName(), Permissions: []string{"spanner.databases.select"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"spanner.databases.select"}, perms.GetPermissions())

	_, err = e.instances.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: parent + "/instances/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
