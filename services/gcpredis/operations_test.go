package gcpredis_test

import (
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *env) update(t *testing.T, in *redispb.Instance, paths ...string) (*redispb.Instance, error) {
	t.Helper()

	op, err := e.client.UpdateInstance(t.Context(), &redispb.UpdateInstanceRequest{
		Instance: in, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	})
	if err != nil {
		return nil, err
	}

	return await(t, op)
}

func TestUpdateInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		start   *redispb.Instance
		patch   *redispb.Instance
		check   func(t *testing.T, got *redispb.Instance)
		paths   []string
		wantErr codes.Code
	}{
		{
			name:  "labels and display name",
			start: basic(),
			patch: &redispb.Instance{DisplayName: "renamed", Labels: map[string]string{"a": "b"}},
			paths: []string{"display_name", "labels"},
			check: func(t *testing.T, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, "renamed", got.GetDisplayName())
				assert.Equal(t, map[string]string{"a": "b"}, got.GetLabels())
			},
		},
		{
			name:  "resize",
			start: basic(),
			patch: &redispb.Instance{MemorySizeGb: 5},
			paths: []string{"memory_size_gb"},
			check: func(t *testing.T, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, int32(5), got.GetMemorySizeGb())
			},
		},
		{
			name: "configs replaced",
			start: basic(
				func(i *redispb.Instance) { i.RedisConfigs = map[string]string{"maxmemory-policy": "noeviction"} },
			),
			patch: &redispb.Instance{RedisConfigs: map[string]string{"activedefrag": "yes"}},
			paths: []string{"redis_configs"},
			check: func(t *testing.T, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, map[string]string{"activedefrag": "yes"}, got.GetRedisConfigs())
			},
		},
		{
			name:  "enable read replicas",
			start: basic(func(i *redispb.Instance) { i.Tier = redispb.Instance_STANDARD_HA }),
			patch: &redispb.Instance{ReadReplicasMode: redispb.Instance_READ_REPLICAS_ENABLED, ReplicaCount: 2},
			paths: []string{"read_replicas_mode", "replica_count"},
			check: func(t *testing.T, got *redispb.Instance) {
				t.Helper()
				assert.Len(t, got.GetNodes(), 3)
				assert.Equal(t, got.GetPort(), got.GetReadEndpointPort())
			},
		},
		{
			name:  "maintenance policy stamped",
			start: basic(),
			patch: &redispb.Instance{MaintenancePolicy: &redispb.MaintenancePolicy{
				WeeklyMaintenanceWindow: []*redispb.WeeklyMaintenanceWindow{{Day: dayofweek.DayOfWeek_FRIDAY}},
			}},
			paths: []string{"maintenance_policy"},
			check: func(t *testing.T, got *redispb.Instance) {
				t.Helper()
				assert.NotNil(t, got.GetMaintenancePolicy().GetUpdateTime())
			},
		},
		{
			name:  "enable rdb persistence",
			start: basic(),
			patch: &redispb.Instance{PersistenceConfig: &redispb.PersistenceConfig{
				PersistenceMode: redispb.PersistenceConfig_RDB, RdbSnapshotPeriod: redispb.PersistenceConfig_ONE_HOUR,
			}},
			paths: []string{"persistence_config"},
			check: func(t *testing.T, got *redispb.Instance) {
				t.Helper()
				assert.NotNil(t, got.GetPersistenceConfig().GetRdbNextSnapshotTime())
			},
		},
		{
			name:    "tier is immutable",
			start:   basic(),
			patch:   &redispb.Instance{Tier: redispb.Instance_STANDARD_HA},
			paths:   []string{"tier"},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "transit encryption is immutable",
			start:   basic(),
			patch:   &redispb.Instance{},
			paths:   []string{"transit_encryption_mode"},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "invalid size",
			start:   basic(),
			patch:   &redispb.Instance{MemorySizeGb: 0},
			paths:   []string{"memory_size_gb"},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "invalid config",
			start:   basic(),
			patch:   &redispb.Instance{RedisConfigs: map[string]string{"x": "y"}},
			paths:   []string{"redis_configs"},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "replicas on basic",
			start:   basic(),
			patch:   &redispb.Instance{ReplicaCount: 2},
			paths:   []string{"replica_count"},
			wantErr: codes.InvalidArgument,
		},
		{name: "no mask", start: basic(), patch: &redispb.Instance{DisplayName: "x"}, wantErr: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			start := e.create(t, "upd", tt.start)

			tt.patch.Name = start.GetName()

			got, err := e.update(t, tt.patch, tt.paths...)
			if tt.wantErr != codes.OK {
				assert.Equal(t, tt.wantErr, status.Code(err), "%v", err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, redispb.Instance_READY, got.GetState())
			tt.check(t, got)
		})
	}

	t.Run("missing instance", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.update(t, &redispb.Instance{Name: loc + "/instances/none"}, "labels")
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestUpdateAuth(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	got := e.create(t, "auth", basic())

	on, err := e.update(t, &redispb.Instance{Name: got.GetName(), AuthEnabled: true}, "auth_enabled")
	require.NoError(t, err)
	assert.True(t, on.GetAuthEnabled())

	as, err := e.client.GetInstanceAuthString(t.Context(), &redispb.GetInstanceAuthStringRequest{Name: got.GetName()})
	require.NoError(t, err)
	require.Error(t, e.dial(t, on, "").Ping(t.Context()).Err())
	require.NoError(t, e.dial(t, on, as.GetAuthString()).Ping(t.Context()).Err())

	off, err := e.update(t, &redispb.Instance{Name: got.GetName()}, "auth_enabled")
	require.NoError(t, err)
	assert.False(t, off.GetAuthEnabled())
	require.NoError(t, e.dial(t, off, "").Ping(t.Context()).Err())

	_, err = e.client.GetInstanceAuthString(t.Context(), &redispb.GetInstanceAuthStringRequest{Name: got.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestUpgradeInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		want    string
		wantErr codes.Code
	}{
		{name: "newer", target: "REDIS_7_2", want: "REDIS_7_2"},
		{name: "same", target: "REDIS_6_X", wantErr: codes.InvalidArgument},
		{name: "downgrade", target: "REDIS_5_0", wantErr: codes.InvalidArgument},
		{name: "unknown", target: "REDIS_99", wantErr: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			inst := e.create(t, "up", basic(func(i *redispb.Instance) { i.RedisVersion = "REDIS_6_X" }))

			op, err := e.client.UpgradeInstance(t.Context(), &redispb.UpgradeInstanceRequest{
				Name: inst.GetName(), RedisVersion: tt.target,
			})
			if tt.wantErr != codes.OK {
				assert.Equal(t, tt.wantErr, status.Code(err))

				return
			}

			require.NoError(t, err)

			got, err := await(t, op)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.GetRedisVersion())
			assert.Equal(t, redispb.Instance_READY, got.GetState())
		})
	}
}

func TestFailoverInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ha := e.create(t, "ha", basic(func(i *redispb.Instance) { i.Tier = redispb.Instance_STANDARD_HA }))
	single := e.create(t, "single", basic())

	_, err := e.client.FailoverInstance(t.Context(), &redispb.FailoverInstanceRequest{Name: single.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	for i, wantZone := range []string{"us-central1-b", "us-central1-a"} {
		op, foErr := e.client.FailoverInstance(t.Context(), &redispb.FailoverInstanceRequest{
			Name:               ha.GetName(),
			DataProtectionMode: redispb.FailoverInstanceRequest_FORCE_DATA_LOSS,
		})
		require.NoError(t, foErr, i)

		got, waitErr := await(t, op)
		require.NoError(t, waitErr)
		assert.Equal(t, wantZone, got.GetCurrentLocationId())
		assert.Equal(t, wantZone, got.GetNodes()[0].GetZone())
		assert.Equal(t, zone, got.GetLocationId())
	}
}

func TestRescheduleMaintenance(t *testing.T) {
	t.Parallel()

	future := timestamppb.New(time.Now().Add(48 * time.Hour))
	past := timestamppb.New(time.Now().Add(-time.Hour))

	tests := []struct {
		req     *redispb.RescheduleMaintenanceRequest
		policy  *redispb.MaintenancePolicy
		check   func(t *testing.T, s *redispb.MaintenanceSchedule)
		name    string
		wantErr codes.Code
	}{
		{
			name: "immediate",
			req:  &redispb.RescheduleMaintenanceRequest{RescheduleType: redispb.RescheduleMaintenanceRequest_IMMEDIATE},
			check: func(t *testing.T, s *redispb.MaintenanceSchedule) {
				t.Helper()
				assert.Equal(t, time.Hour, s.GetEndTime().AsTime().Sub(s.GetStartTime().AsTime()))
			},
		},
		{
			name: "specific time",
			req: &redispb.RescheduleMaintenanceRequest{
				RescheduleType: redispb.RescheduleMaintenanceRequest_SPECIFIC_TIME, ScheduleTime: future,
			},
			check: func(t *testing.T, s *redispb.MaintenanceSchedule) {
				t.Helper()
				assert.True(t, s.GetStartTime().AsTime().Equal(future.AsTime()))
			},
		},
		{
			name: "next window follows the policy day",
			req: &redispb.RescheduleMaintenanceRequest{
				RescheduleType: redispb.RescheduleMaintenanceRequest_NEXT_AVAILABLE_WINDOW,
			},
			policy: &redispb.MaintenancePolicy{WeeklyMaintenanceWindow: []*redispb.WeeklyMaintenanceWindow{
				{Day: dayofweek.DayOfWeek_WEDNESDAY},
			}},
			check: func(t *testing.T, s *redispb.MaintenanceSchedule) {
				t.Helper()
				assert.Equal(t, time.Wednesday, s.GetStartTime().AsTime().Weekday())
				assert.True(t, s.GetStartTime().AsTime().After(time.Now()))
			},
		},
		{
			name: "next window without policy",
			req: &redispb.RescheduleMaintenanceRequest{
				RescheduleType: redispb.RescheduleMaintenanceRequest_NEXT_AVAILABLE_WINDOW,
			},
			check: func(t *testing.T, s *redispb.MaintenanceSchedule) {
				t.Helper()
				assert.True(t, s.GetStartTime().AsTime().After(time.Now()))
			},
		},
		{
			name: "specific time needs a time",
			req: &redispb.RescheduleMaintenanceRequest{
				RescheduleType: redispb.RescheduleMaintenanceRequest_SPECIFIC_TIME,
			},
			wantErr: codes.InvalidArgument,
		},
		{
			name: "specific time in the past",
			req: &redispb.RescheduleMaintenanceRequest{
				RescheduleType: redispb.RescheduleMaintenanceRequest_SPECIFIC_TIME,
				ScheduleTime:   past,
			},
			wantErr: codes.InvalidArgument,
		},
		{name: "type required", req: &redispb.RescheduleMaintenanceRequest{}, wantErr: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			inst := e.create(t, "maint", basic(func(i *redispb.Instance) { i.MaintenancePolicy = tt.policy }))

			req := &redispb.RescheduleMaintenanceRequest{
				Name:           inst.GetName(),
				RescheduleType: tt.req.GetRescheduleType(),
				ScheduleTime:   tt.req.GetScheduleTime(),
			}

			op, err := e.client.RescheduleMaintenance(t.Context(), req)
			if tt.wantErr != codes.OK {
				assert.Equal(t, tt.wantErr, status.Code(err))

				return
			}

			require.NoError(t, err)

			got, err := await(t, op)
			require.NoError(t, err)
			tt.check(t, got.GetMaintenanceSchedule())
		})
	}
}

func TestImportExport(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	src := e.create(t, "src", basic())
	dst := e.create(t, "dst", basic())

	sc := e.dial(t, src, "")
	require.NoError(t, sc.Set(t.Context(), "str", "value", 0).Err())
	require.NoError(t, sc.HSet(t.Context(), "hash", "f", "v").Err())
	require.NoError(t, sc.RPush(t.Context(), "list", "a", "b").Err())
	require.NoError(t, sc.ZAdd(t.Context(), "z", goZ(2, "m")).Err())

	exp, err := e.client.ExportInstance(t.Context(), &redispb.ExportInstanceRequest{
		Name: src.GetName(),
		OutputConfig: &redispb.OutputConfig{Destination: &redispb.OutputConfig_GcsDestination{
			GcsDestination: &redispb.GcsDestination{Uri: "gs://bkt/dump.rdb"},
		}},
	})
	require.NoError(t, err)

	_, err = await(t, exp)
	require.NoError(t, err)
	assert.Equal(t, "REDIS0009", string(e.store.objs["bkt/dump.rdb"][:9]))

	dc := e.dial(t, dst, "")
	require.NoError(t, dc.Set(t.Context(), "old", "gone", 0).Err())

	imp, err := e.client.ImportInstance(t.Context(), &redispb.ImportInstanceRequest{
		Name: dst.GetName(),
		InputConfig: &redispb.InputConfig{Source: &redispb.InputConfig_GcsSource{
			GcsSource: &redispb.GcsSource{Uri: "gs://bkt/dump.rdb"},
		}},
	})
	require.NoError(t, err)

	got, err := await(t, imp)
	require.NoError(t, err)
	assert.Equal(t, redispb.Instance_READY, got.GetState())

	v, err := dc.Get(t.Context(), "str").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", v)
	assert.Equal(t, "v", dc.HGet(t.Context(), "hash", "f").Val())
	assert.Equal(t, []string{"a", "b"}, dc.LRange(t.Context(), "list", 0, -1).Val())
	assert.InDelta(t, 2, dc.ZScore(t.Context(), "z", "m").Val(), 0)
	assert.Equal(t, int64(0), dc.Exists(t.Context(), "old").Val(), "import replaces existing data")
}

func TestImportExportErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.create(t, "ie", basic())
	e.store.objs["bkt/garbage"] = []byte("not an rdb file")

	imp := func(uri string) *redispb.ImportInstanceRequest {
		return &redispb.ImportInstanceRequest{Name: inst.GetName(), InputConfig: &redispb.InputConfig{
			Source: &redispb.InputConfig_GcsSource{GcsSource: &redispb.GcsSource{Uri: uri}},
		}}
	}

	for _, uri := range []string{"", "http://x/y", "gs://bucket-only", "gs:///obj"} {
		_, err := e.client.ImportInstance(t.Context(), imp(uri))
		assert.Equal(t, codes.InvalidArgument, status.Code(err), uri)
	}

	_, err := e.client.ExportInstance(t.Context(), &redispb.ExportInstanceRequest{Name: inst.GetName()})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	tests := []struct {
		name string
		uri  string
		code codes.Code
	}{
		{name: "missing object", uri: "gs://bkt/none", code: codes.NotFound},
		{name: "garbage object", uri: "gs://bkt/garbage", code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		op, impErr := e.client.ImportInstance(t.Context(), imp(tt.uri))
		require.NoError(t, impErr, tt.name)

		_, waitErr := await(t, op)
		assert.Equal(t, tt.code, status.Code(waitErr), tt.name)
	}

	got, err := e.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: inst.GetName()})
	require.NoError(t, err)
	assert.Equal(t, redispb.Instance_READY, got.GetState(), "failed import returns the instance to READY")
}

func TestOperationsAPI(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	op, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
		Parent: loc, InstanceId: "ops", Instance: basic(),
	})
	require.NoError(t, err)

	_, err = await(t, op)
	require.NoError(t, err)

	lro := e.client.LROClient
	got, err := lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())
	assert.Contains(t, op.Name(), loc+"/operations/operation-")

	meta, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, loc+"/instances/ops", meta.GetTarget())
	assert.Equal(t, "create", meta.GetVerb())
	assert.Equal(t, "v1", meta.GetApiVersion())

	list := lro.ListOperations(t.Context(), &longrunningpb.ListOperationsRequest{Name: loc})
	first, err := list.Next()
	require.NoError(t, err)
	assert.Equal(t, op.Name(), first.GetName())

	_, err = lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: loc + "/operations/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
