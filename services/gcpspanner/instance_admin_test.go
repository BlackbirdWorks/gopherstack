package gcpspanner_test

import (
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	nam3Ref   = parent + "/instanceConfigs/nam3"
	customRef = parent + "/instanceConfigs/custom-nam3-ro"
)

func nam3Custom(extra ...*instancepb.ReplicaInfo) []*instancepb.ReplicaInfo {
	rw := instancepb.ReplicaInfo_READ_WRITE
	reps := make([]*instancepb.ReplicaInfo, 0, 5+len(extra))

	reps = append(reps,
		&instancepb.ReplicaInfo{Location: "us-east4", Type: rw, DefaultLeaderLocation: true},
		&instancepb.ReplicaInfo{Location: "us-east4", Type: rw},
		&instancepb.ReplicaInfo{Location: "us-east1", Type: rw},
		&instancepb.ReplicaInfo{Location: "us-east1", Type: rw},
		&instancepb.ReplicaInfo{Location: "us-central1", Type: instancepb.ReplicaInfo_WITNESS},
	)

	return append(reps, extra...)
}

func roReplica(loc string) *instancepb.ReplicaInfo {
	return &instancepb.ReplicaInfo{Location: loc, Type: instancepb.ReplicaInfo_READ_ONLY}
}

func (e *env) createConfig(t *testing.T, id string, replicas []*instancepb.ReplicaInfo) *instancepb.InstanceConfig {
	t.Helper()

	op, err := e.instances.CreateInstanceConfig(t.Context(), &instancepb.CreateInstanceConfigRequest{
		Parent: parent, InstanceConfigId: id,
		InstanceConfig: &instancepb.InstanceConfig{BaseConfig: nam3Ref, DisplayName: "Custom", Replicas: replicas},
	})
	require.NoError(t, err)

	cfg, err := await(t, op)
	require.NoError(t, err)

	return cfg
}

func TestInstanceConfig_Catalog(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name        string
		config      string
		witnesses   int
		readOnly    int
		optionalMin int
	}{
		{"nam3", "nam3", 1, 0, 12},
		{"nam6", "nam6", 1, 2, 0},
		{"regional", "regional-us-central1", 0, 0, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := e.instances.GetInstanceConfig(t.Context(), &instancepb.GetInstanceConfigRequest{
				Name: parent + "/instanceConfigs/" + tt.config,
			})
			require.NoError(t, err)

			counts := make(map[instancepb.ReplicaInfo_ReplicaType]int)
			for _, r := range cfg.GetReplicas() {
				counts[r.GetType()]++
			}

			assert.Equal(t, tt.witnesses, counts[instancepb.ReplicaInfo_WITNESS])
			assert.Equal(t, tt.readOnly, counts[instancepb.ReplicaInfo_READ_ONLY])
			assert.Len(t, cfg.GetOptionalReplicas(), tt.optionalMin)
			assert.Equal(t, instancepb.InstanceConfig_GOOGLE_MANAGED, cfg.GetConfigType())
		})
	}
}

func TestUserConfig_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*instancepb.CreateInstanceConfigRequest)
		name   string
		want   codes.Code
	}{
		{func(*instancepb.CreateInstanceConfigRequest) {}, "ok", codes.OK},
		{
			func(r *instancepb.CreateInstanceConfigRequest) { r.InstanceConfigId = "nam3-ro" },
			"no prefix",
			codes.InvalidArgument,
		},
		{func(r *instancepb.CreateInstanceConfigRequest) {
			r.InstanceConfigId = "custom-" + strings.Repeat("a", 60)
		}, "too long", codes.InvalidArgument},
		{
			func(r *instancepb.CreateInstanceConfigRequest) { r.InstanceConfig.BaseConfig = "" },
			"no base",
			codes.InvalidArgument,
		},
		{func(r *instancepb.CreateInstanceConfigRequest) {
			r.InstanceConfig.BaseConfig = parent + "/instanceConfigs/nope"
		}, "unknown base", codes.NotFound},
		{func(r *instancepb.CreateInstanceConfigRequest) {
			r.InstanceConfig.Name = parent + "/instanceConfigs/custom-other"
		}, "name mismatch", codes.InvalidArgument},
		{
			func(r *instancepb.CreateInstanceConfigRequest) { r.InstanceConfig.Replicas = nam3Custom() },
			"no optional replica",
			codes.InvalidArgument,
		},
		{func(r *instancepb.CreateInstanceConfigRequest) {
			r.InstanceConfig.Replicas = nam3Custom(roReplica("us-central2"))
		}, "not optional", codes.InvalidArgument},
		{func(r *instancepb.CreateInstanceConfigRequest) {
			r.InstanceConfig.Replicas = nam3Custom(roReplica("us-west1"), roReplica("us-west1"))
		}, "too many of one", codes.InvalidArgument},
		{func(r *instancepb.CreateInstanceConfigRequest) {
			r.InstanceConfig.Replicas = nam3Custom(roReplica("us-west1"))[1:]
		}, "missing base replica", codes.InvalidArgument},
		{
			func(r *instancepb.CreateInstanceConfigRequest) { r.InstanceConfig.BaseConfig = customRef },
			"user base",
			codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			req := &instancepb.CreateInstanceConfigRequest{
				Parent: parent, InstanceConfigId: "custom-nam3-ro",
				InstanceConfig: &instancepb.InstanceConfig{
					BaseConfig: nam3Ref, DisplayName: "Custom", Replicas: nam3Custom(roReplica("us-west1")),
				},
			}
			tt.mutate(req)

			op, err := e.instances.CreateInstanceConfig(t.Context(), req)
			require.Equal(t, tt.want, status.Code(err))

			if tt.want != codes.OK {
				return
			}

			cfg, err := await(t, op)
			require.NoError(t, err)
			assert.Equal(t, instancepb.InstanceConfig_USER_MANAGED, cfg.GetConfigType())
			assert.Equal(t, instancepb.InstanceConfig_READY, cfg.GetState())
			assert.False(t, cfg.GetReconciling())
			assert.Equal(t, nam3Ref, cfg.GetBaseConfig())
			assert.ElementsMatch(t, []string{"us-east4", "us-east1"}, cfg.GetLeaderOptions())
		})
	}
}

func TestUserConfig_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	cfg := e.createConfig(t, "custom-nam3-ro", nam3Custom(roReplica("us-west1")))

	_, err := e.instances.CreateInstanceConfig(t.Context(), &instancepb.CreateInstanceConfigRequest{
		Parent: parent, InstanceConfigId: "custom-nam3-ro",
		InstanceConfig: &instancepb.InstanceConfig{BaseConfig: nam3Ref, Replicas: nam3Custom(roReplica("us-west1"))},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	validate, err := e.instances.CreateInstanceConfig(t.Context(), &instancepb.CreateInstanceConfigRequest{
		Parent: parent, InstanceConfigId: "custom-dry", ValidateOnly: true,
		InstanceConfig: &instancepb.InstanceConfig{BaseConfig: nam3Ref, Replicas: nam3Custom(roReplica("us-west1"))},
	})
	require.NoError(t, err)

	_, err = await(t, validate)
	require.NoError(t, err)

	_, err = e.instances.GetInstanceConfig(
		t.Context(),
		&instancepb.GetInstanceConfigRequest{Name: parent + "/instanceConfigs/custom-dry"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	var listed []string

	it := e.instances.ListInstanceConfigs(t.Context(), &instancepb.ListInstanceConfigsRequest{Parent: parent})

	for {
		c, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		listed = append(listed, c.GetName())
	}

	assert.Contains(t, listed, cfg.GetName())

	upd, err := e.instances.UpdateInstanceConfig(t.Context(), &instancepb.UpdateInstanceConfigRequest{
		InstanceConfig: &instancepb.InstanceConfig{
			Name:        cfg.GetName(),
			DisplayName: "Renamed",
			Labels:      map[string]string{"k": "v"},
			Etag:        cfg.GetEtag(),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "labels"}},
	})
	require.NoError(t, err)

	updated, err := await(t, upd)
	require.NoError(t, err)
	assert.Equal(t, "Renamed", updated.GetDisplayName())
	assert.Equal(t, "v", updated.GetLabels()["k"])
	assert.NotEqual(t, cfg.GetEtag(), updated.GetEtag())

	updateReq := func(mask string, etag string) error {
		_, uerr := e.instances.UpdateInstanceConfig(t.Context(), &instancepb.UpdateInstanceConfigRequest{
			InstanceConfig: &instancepb.InstanceConfig{Name: cfg.GetName(), DisplayName: "Again", Etag: etag},
			UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})

		return uerr
	}

	assert.Equal(t, codes.Aborted, status.Code(updateReq("display_name", cfg.GetEtag())))
	assert.Equal(t, codes.InvalidArgument, status.Code(updateReq("replicas", "")))

	_, err = e.instances.UpdateInstanceConfig(t.Context(), &instancepb.UpdateInstanceConfigRequest{
		InstanceConfig: &instancepb.InstanceConfig{Name: nam3Ref, DisplayName: "x"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	inst, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
		Parent: parent, InstanceId: "on-custom",
		Instance: &instancepb.Instance{Config: cfg.GetName(), DisplayName: "On custom", NodeCount: 1},
	})
	require.NoError(t, err)

	_, err = await(t, inst)
	require.NoError(t, err)

	del := func(name string) codes.Code {
		return status.Code(
			e.instances.DeleteInstanceConfig(t.Context(), &instancepb.DeleteInstanceConfigRequest{Name: name}),
		)
	}

	assert.Equal(t, codes.FailedPrecondition, del(cfg.GetName()))
	assert.Equal(t, codes.InvalidArgument, del(nam3Ref))
	assert.Equal(t, codes.NotFound, del(parent+"/instanceConfigs/custom-nope"))

	require.NoError(t, e.instances.DeleteInstance(t.Context(), deleteReq(parent+"/instances/on-custom")))
	assert.Equal(t, codes.OK, del(cfg.GetName()))
	assert.Equal(t, codes.NotFound, del(cfg.GetName()))
}

func listConfigOps(t *testing.T, e *env, filter string) ([]string, error) {
	t.Helper()

	var names []string

	it := e.instances.ListInstanceConfigOperations(
		t.Context(),
		&instancepb.ListInstanceConfigOperationsRequest{Parent: parent, Filter: filter},
	)

	for {
		op, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return names, nil
		}

		if err != nil {
			return nil, err
		}

		names = append(names, op.GetName())
	}
}

func TestListInstanceConfigOperations_Filter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	cfg := e.createConfig(t, "custom-nam3-ro", nam3Custom(roReplica("us-west1")))

	upd, err := e.instances.UpdateInstanceConfig(t.Context(), &instancepb.UpdateInstanceConfigRequest{
		InstanceConfig: &instancepb.InstanceConfig{Name: cfg.GetName(), DisplayName: "Renamed"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	require.NoError(t, err)

	_, err = await(t, upd)
	require.NoError(t, err)

	const typePrefix = "type.googleapis.com/google.spanner.admin.instance.v1."

	tests := []struct {
		name    string
		filter  string
		want    int
		wantErr codes.Code
	}{
		{"all", "", 2, codes.OK},
		{"create only", "metadata.@type=" + typePrefix + "CreateInstanceConfigMetadata", 1, codes.OK},
		{
			"update only",
			"(metadata.@type=" + typePrefix + "UpdateInstanceConfigMetadata) AND (metadata.instance_config.name:custom-nam3)",
			1,
			codes.OK,
		},
		{"done", "done:true", 2, codes.OK},
		{"not done", "done=false", 0, codes.OK},
		{"error present", "error:*", 0, codes.OK},
		{"not create", "NOT metadata.@type=" + typePrefix + "CreateInstanceConfigMetadata", 1, codes.OK},
		{
			"or",
			"metadata.@type=" + typePrefix + "CreateInstanceConfigMetadata OR " +
				"metadata.@type=" + typePrefix + "UpdateInstanceConfigMetadata",
			2,
			codes.OK,
		},
		{"start before", "metadata.progress.start_time < \"2000-01-01T00:00:00Z\"", 0, codes.OK},
		{"start after", "metadata.progress.start_time > \"2000-01-01T00:00:00Z\"", 2, codes.OK},
		{"name contains", "name:custom-nam3-ro", 2, codes.OK},
		{"unbalanced", "(done:true", 0, codes.InvalidArgument},
		{"closing only", "done:true)", 0, codes.InvalidArgument},
		{"no operator", "done", 0, codes.InvalidArgument},
		{"no value", "done:", 0, codes.InvalidArgument},
		{"unterminated quote", "name:\"abc", 0, codes.InvalidArgument},
		{"empty parens", "()", 0, codes.InvalidArgument},
		{"dangling and", "done:true AND", 0, codes.InvalidArgument},
		{"too deep", strings.Repeat("(", 64) + "done:true" + strings.Repeat(")", 64), 0, codes.InvalidArgument},
		{"too long", "name:" + strings.Repeat("a", 5000), 0, codes.InvalidArgument},
		{"too many tokens", strings.Repeat("done:true ", 300), 0, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			names, lerr := listConfigOps(t, e, tt.filter)
			require.Equal(t, tt.wantErr, status.Code(lerr))
			assert.Len(t, names, tt.want)
		})
	}
}

func TestInstance_Autoscaling(t *testing.T) {
	t.Parallel()

	limits := func(lo, hi int32) *instancepb.AutoscalingConfig_AutoscalingLimits {
		return &instancepb.AutoscalingConfig_AutoscalingLimits{
			MinLimit: &instancepb.AutoscalingConfig_AutoscalingLimits_MinNodes{MinNodes: lo},
			MaxLimit: &instancepb.AutoscalingConfig_AutoscalingLimits_MaxNodes{MaxNodes: hi},
		}
	}
	good := func() *instancepb.AutoscalingConfig {
		return &instancepb.AutoscalingConfig{
			AutoscalingLimits: limits(2, 6),
			AutoscalingTargets: &instancepb.AutoscalingConfig_AutoscalingTargets{
				HighPriorityCpuUtilizationPercent: 65, StorageUtilizationPercent: 90,
			},
		}
	}

	tests := []struct {
		mutate  func(*instancepb.AutoscalingConfig)
		name    string
		want    codes.Code
		wantPUs int32
	}{
		{func(*instancepb.AutoscalingConfig) {}, "ok", codes.OK, 2000},
		{
			func(a *instancepb.AutoscalingConfig) { a.AutoscalingLimits = limits(1, 11) },
			"over 10x",
			codes.InvalidArgument,
			0,
		},
		{
			func(a *instancepb.AutoscalingConfig) { a.AutoscalingLimits = limits(3, 2) },
			"max below min",
			codes.InvalidArgument,
			0,
		},
		{func(a *instancepb.AutoscalingConfig) { a.AutoscalingLimits = nil }, "no limits", codes.InvalidArgument, 0},
		{func(a *instancepb.AutoscalingConfig) {
			a.AutoscalingLimits.MaxLimit = &instancepb.AutoscalingConfig_AutoscalingLimits_MaxProcessingUnits{
				MaxProcessingUnits: 4000,
			}
		}, "mixed units", codes.InvalidArgument, 0},
		{func(a *instancepb.AutoscalingConfig) { a.AutoscalingTargets = nil }, "no targets", codes.InvalidArgument, 0},
		{
			func(a *instancepb.AutoscalingConfig) { a.AutoscalingTargets.StorageUtilizationPercent = 5 },
			"storage low",
			codes.InvalidArgument,
			0,
		},
		{
			func(a *instancepb.AutoscalingConfig) { a.AutoscalingTargets.StorageUtilizationPercent = 100 },
			"storage high",
			codes.InvalidArgument,
			0,
		},
		{func(a *instancepb.AutoscalingConfig) {
			a.AutoscalingTargets = &instancepb.AutoscalingConfig_AutoscalingTargets{
				HighPriorityCpuUtilizationPercent: 95, StorageUtilizationPercent: 50,
			}
		}, "cpu high", codes.InvalidArgument, 0},
		{func(a *instancepb.AutoscalingConfig) {
			a.AsymmetricAutoscalingOptions = []*instancepb.AutoscalingConfig_AsymmetricAutoscalingOption{
				{ReplicaSelection: &instancepb.ReplicaSelection{Location: "us-central1"}},
			}
		}, "asymmetric not read-only", codes.InvalidArgument, 0},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			a := good()
			tt.mutate(a)

			op, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
				Parent:     parent,
				InstanceId: "auto-" + string(rune('a'+i)),
				Instance: &instancepb.Instance{
					Config:            configRef,
					DisplayName:       "Autoscaled",
					NodeCount:         9,
					AutoscalingConfig: a,
				},
			})
			require.Equal(t, tt.want, status.Code(err))

			if tt.want != codes.OK {
				return
			}

			inst, err := await(t, op)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPUs, inst.GetProcessingUnits())
			assert.Equal(t, int32(2), inst.GetNodeCount())
			require.NotEmpty(t, inst.GetReplicaComputeCapacity())
			assert.Equal(t, int32(2), inst.GetReplicaComputeCapacity()[0].GetNodeCount())
		})
	}
}

func TestInstance_AutoscalingUpdate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	op, err := e.instances.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
		Parent:     parent,
		InstanceId: "auto-upd",
		Instance: &instancepb.Instance{
			Config:      configRef,
			DisplayName: "Autoscaled",
			AutoscalingConfig: &instancepb.AutoscalingConfig{
				AutoscalingLimits: &instancepb.AutoscalingConfig_AutoscalingLimits{
					MinLimit: &instancepb.AutoscalingConfig_AutoscalingLimits_MinNodes{MinNodes: 2},
					MaxLimit: &instancepb.AutoscalingConfig_AutoscalingLimits_MaxNodes{MaxNodes: 6},
				},
				AutoscalingTargets: &instancepb.AutoscalingConfig_AutoscalingTargets{StorageUtilizationPercent: 90},
			},
		},
	})
	require.NoError(t, err)

	inst, err := await(t, op)
	require.NoError(t, err)

	update := func(paths []string, in *instancepb.Instance) (*instancepb.Instance, error) {
		in.Name = inst.GetName()

		uop, uerr := e.instances.UpdateInstance(t.Context(), &instancepb.UpdateInstanceRequest{
			Instance: in, FieldMask: &fieldmaskpb.FieldMask{Paths: paths},
		})
		if uerr != nil {
			return nil, uerr
		}

		return await(t, uop)
	}

	got, err := update([]string{"autoscaling_config.autoscaling_limits.min_nodes"}, &instancepb.Instance{
		AutoscalingConfig: &instancepb.AutoscalingConfig{
			AutoscalingLimits: &instancepb.AutoscalingConfig_AutoscalingLimits{
				MinLimit: &instancepb.AutoscalingConfig_AutoscalingLimits_MinNodes{MinNodes: 4},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(4), got.GetAutoscalingConfig().GetAutoscalingLimits().GetMinNodes())
	assert.Equal(t, int32(6), got.GetAutoscalingConfig().GetAutoscalingLimits().GetMaxNodes())
	assert.Equal(t, int32(90), got.GetAutoscalingConfig().GetAutoscalingTargets().GetStorageUtilizationPercent())
	assert.Equal(t, int32(4000), got.GetProcessingUnits())

	_, err = update([]string{"node_count"}, &instancepb.Instance{NodeCount: 3})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	got, err = update([]string{"autoscaling_config", "node_count"}, &instancepb.Instance{NodeCount: 3})
	require.NoError(t, err)
	assert.Nil(t, got.GetAutoscalingConfig())
	assert.Equal(t, int32(3), got.GetNodeCount())
	assert.Equal(t, int32(3000), got.GetProcessingUnits())
}

func TestInstancePartition_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.createInstance(t, "parts")
	cfg := parent + "/instanceConfigs/regional-us-east1"

	create := func(id string, p *instancepb.InstancePartition) (*instancepb.InstancePartition, error) {
		op, err := e.instances.CreateInstancePartition(t.Context(), &instancepb.CreateInstancePartitionRequest{
			Parent: inst.GetName(), InstancePartitionId: id, InstancePartition: p,
		})
		if err != nil {
			return nil, err
		}

		return await(t, op)
	}

	good := func() *instancepb.InstancePartition {
		return &instancepb.InstancePartition{
			Config: cfg, DisplayName: "East part",
			ComputeCapacity: &instancepb.InstancePartition_NodeCount{NodeCount: 1},
		}
	}

	tests := []struct {
		mutate func(*instancepb.InstancePartition)
		name   string
		id     string
		want   codes.Code
	}{
		{func(*instancepb.InstancePartition) {}, "bad id", "-bad", codes.InvalidArgument},
		{
			func(p *instancepb.InstancePartition) { p.Config = parent + "/instanceConfigs/nope" },
			"config",
			"p-config",
			codes.NotFound,
		},
		{func(p *instancepb.InstancePartition) { p.DisplayName = "ab" }, "name", "p-name", codes.InvalidArgument},
		{func(p *instancepb.InstancePartition) {
			p.ComputeCapacity = &instancepb.InstancePartition_ProcessingUnits{ProcessingUnits: 150}
		}, "pu", "p-pu", codes.InvalidArgument},
		{func(p *instancepb.InstancePartition) { p.ComputeCapacity = nil }, "capacity", "p-cap", codes.InvalidArgument},
		{
			func(p *instancepb.InstancePartition) { p.Name = inst.GetName() + "/instancePartitions/other" },
			"name mismatch",
			"p-mis",
			codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := good()
			tt.mutate(p)

			_, err := create(tt.id, p)
			assert.Equal(t, tt.want, status.Code(err))
		})
	}

	pa, err := create("east", good())
	require.NoError(t, err)
	assert.Equal(t, inst.GetName()+"/instancePartitions/east", pa.GetName())
	assert.Equal(t, instancepb.InstancePartition_READY, pa.GetState())
	assert.Equal(t, int32(1), pa.GetNodeCount())
	assert.NotNil(t, pa.GetCreateTime())

	_, err = create("east", good())
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	got, err := e.instances.GetInstancePartition(
		t.Context(),
		&instancepb.GetInstancePartitionRequest{Name: pa.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, pa.GetEtag(), got.GetEtag())

	e.createInstance(t, "other")

	count := func(parentName string) int {
		n := 0
		it := e.instances.ListInstancePartitions(
			t.Context(),
			&instancepb.ListInstancePartitionsRequest{Parent: parentName},
		)

		for {
			if _, nerr := it.Next(); nerr != nil {
				return n
			}

			n++
		}
	}

	assert.Equal(t, 1, count(inst.GetName()))
	assert.Equal(t, 1, count(parent+"/instances/-"))
	assert.Equal(t, 0, count(parent+"/instances/other"))

	updOp, err := e.instances.UpdateInstancePartition(t.Context(), &instancepb.UpdateInstancePartitionRequest{
		InstancePartition: &instancepb.InstancePartition{
			Name: pa.GetName(), DisplayName: "East renamed", Etag: pa.GetEtag(),
			ComputeCapacity: &instancepb.InstancePartition_ProcessingUnits{ProcessingUnits: 300},
		},
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "processing_units"}},
	})
	require.NoError(t, err)

	upd, err := await(t, updOp)
	require.NoError(t, err)
	assert.Equal(t, int32(300), upd.GetProcessingUnits())
	assert.Equal(t, "East renamed", upd.GetDisplayName())

	_, err = e.instances.UpdateInstancePartition(t.Context(), &instancepb.UpdateInstancePartitionRequest{
		InstancePartition: &instancepb.InstancePartition{
			Name:        pa.GetName(),
			DisplayName: "Stale name",
			Etag:        pa.GetEtag(),
		},
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	assert.Equal(t, codes.Aborted, status.Code(err))

	_, err = e.instances.UpdateInstancePartition(t.Context(), &instancepb.UpdateInstancePartitionRequest{
		InstancePartition: &instancepb.InstancePartition{Name: pa.GetName(), Config: cfg},
		FieldMask:         &fieldmaskpb.FieldMask{Paths: []string{"config"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	var opNames []string

	oit := e.instances.ListInstancePartitionOperations(t.Context(), &instancepb.ListInstancePartitionOperationsRequest{
		Parent: inst.GetName(),
		Filter: "metadata.@type=type.googleapis.com/google.spanner.admin.instance.v1.CreateInstancePartitionMetadata",
	})

	for {
		o, oerr := oit.Next()
		if oerr != nil {
			break
		}

		opNames = append(opNames, o.GetName())
	}

	require.Len(t, opNames, 1)
	assert.Contains(t, opNames[0], "/instancePartitions/east/operations/")
}

func TestInstancePartition_DeleteEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.createInstance(t, "parts")

	op, err := e.instances.CreateInstancePartition(t.Context(), &instancepb.CreateInstancePartitionRequest{
		Parent: inst.GetName(), InstancePartitionId: "east",
		InstancePartition: &instancepb.InstancePartition{
			Config:          configRef,
			DisplayName:     "East part",
			ComputeCapacity: &instancepb.InstancePartition_NodeCount{NodeCount: 1},
		},
	})
	require.NoError(t, err)

	pa, err := await(t, op)
	require.NoError(t, err)

	err = e.instances.DeleteInstancePartition(
		t.Context(),
		&instancepb.DeleteInstancePartitionRequest{Name: pa.GetName(), Etag: "stale"},
	)
	assert.Equal(t, codes.Aborted, status.Code(err))
	require.NoError(
		t,
		e.instances.DeleteInstancePartition(
			t.Context(),
			&instancepb.DeleteInstancePartitionRequest{Name: pa.GetName(), Etag: pa.GetEtag()},
		),
	)
	assert.Equal(
		t,
		codes.NotFound,
		status.Code(
			e.instances.DeleteInstancePartition(
				t.Context(),
				&instancepb.DeleteInstancePartitionRequest{Name: pa.GetName()},
			),
		),
	)
}

func TestMoveInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *env, *instancepb.Instance)
		name   string
		target string
		want   codes.Code
	}{
		{nil, "ok", parent + "/instanceConfigs/regional-us-east1", codes.OK},
		{nil, "unknown target", parent + "/instanceConfigs/nope", codes.NotFound},
		{nil, "same config", configRef, codes.InvalidArgument},
		{nil, "bad name", "garbage", codes.NotFound},
		{
			func(t *testing.T, e *env, inst *instancepb.Instance) {
				t.Helper()
				e.handler.Backend.SetInstanceType(inst.GetName(), instancepb.Instance_FREE_INSTANCE)
			},
			"free instance",
			parent + "/instanceConfigs/regional-us-east1",
			codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			inst := e.createInstance(t, "mover")

			if tt.setup != nil {
				tt.setup(t, e, inst)
			}

			op, err := e.instances.MoveInstance(
				t.Context(),
				&instancepb.MoveInstanceRequest{Name: inst.GetName(), TargetConfig: tt.target},
			)
			require.Equal(t, tt.want, status.Code(err))

			if tt.want != codes.OK {
				return
			}

			_, err = await(t, op)
			require.NoError(t, err)

			got, err := e.instances.GetInstance(t.Context(), &instancepb.GetInstanceRequest{Name: inst.GetName()})
			require.NoError(t, err)
			assert.Equal(t, tt.target, got.GetConfig())
		})
	}
}
