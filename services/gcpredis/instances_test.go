package gcpredis_test

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCreateInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in    *redispb.Instance
		check func(t *testing.T, e *env, got *redispb.Instance)
		name  string
	}{
		{
			name: "basic defaults",
			in:   basic(),
			check: func(t *testing.T, e *env, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, redispb.Instance_READY, got.GetState())
				assert.Equal(t, loc+"/instances/inst", got.GetName())
				assert.Equal(t, "REDIS_7_0", got.GetRedisVersion())
				assert.Equal(t, zone, got.GetLocationId())
				assert.Equal(t, zone, got.GetCurrentLocationId())
				assert.Equal(t, redispb.Instance_DIRECT_PEERING, got.GetConnectMode())
				assert.Equal(t, redispb.Instance_DISABLED, got.GetTransitEncryptionMode())
				assert.Equal(t, redispb.PersistenceConfig_DISABLED, got.GetPersistenceConfig().GetPersistenceMode())
				assert.Contains(t, got.GetAuthorizedNetwork(), "global/networks/default")
				assert.NotEmpty(t, got.GetReservedIpRange())
				assert.Empty(t, got.GetNodes())
				assert.Empty(t, got.GetServerCaCerts())
				assert.Positive(t, got.GetPort())
				assert.NotNil(t, got.GetCreateTime())

				c := e.dial(t, got, "")
				require.NoError(t, c.Set(t.Context(), "k", "v", 0).Err())
				v, err := c.Get(t.Context(), "k").Result()
				require.NoError(t, err)
				assert.Equal(t, "v", v)
			},
		},
		{
			name: "standard ha with read replicas",
			in: basic(func(i *redispb.Instance) {
				i.Tier = redispb.Instance_STANDARD_HA
				i.RedisVersion = "REDIS_7_2"
				i.ReadReplicasMode = redispb.Instance_READ_REPLICAS_ENABLED
				i.ReplicaCount = 3
				i.AlternativeLocationId = "us-central1-c"
			}),
			check: func(t *testing.T, _ *env, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, int32(3), got.GetReplicaCount())
				assert.Len(t, got.GetNodes(), 4)
				assert.Equal(t, zone, got.GetNodes()[0].GetZone())
				assert.Equal(t, "us-central1-c", got.GetNodes()[1].GetZone())
				assert.Equal(t, got.GetHost(), got.GetReadEndpoint())
				assert.Equal(t, got.GetPort(), got.GetReadEndpointPort())
			},
		},
		{
			name: "standard ha single replica has no read endpoint",
			in:   basic(func(i *redispb.Instance) { i.Tier = redispb.Instance_STANDARD_HA }),
			check: func(t *testing.T, _ *env, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, int32(1), got.GetReplicaCount())
				assert.Len(t, got.GetNodes(), 2)
				assert.Equal(t, "us-central1-b", got.GetAlternativeLocationId())
				assert.Empty(t, got.GetReadEndpoint())
			},
		},
		{
			name: "auth enabled requires the generated string",
			in:   basic(func(i *redispb.Instance) { i.AuthEnabled = true }),
			check: func(t *testing.T, e *env, got *redispb.Instance) {
				t.Helper()
				assert.True(t, got.GetAuthEnabled())

				require.Error(t, e.dial(t, got, "").Ping(t.Context()).Err())

				as, err := e.client.GetInstanceAuthString(
					t.Context(),
					&redispb.GetInstanceAuthStringRequest{Name: got.GetName()},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, as.GetAuthString())
				require.NoError(t, e.dial(t, got, as.GetAuthString()).Ping(t.Context()).Err())
				require.Error(t, e.dial(t, got, "wrong").Ping(t.Context()).Err())
			},
		},
		{
			name: "server authentication serves tls",
			in: basic(func(i *redispb.Instance) {
				i.TransitEncryptionMode = redispb.Instance_SERVER_AUTHENTICATION
			}),
			check: func(t *testing.T, e *env, got *redispb.Instance) {
				t.Helper()
				require.Len(t, got.GetServerCaCerts(), 1)

				cert := got.GetServerCaCerts()[0]
				assert.Contains(t, cert.GetCert(), "BEGIN CERTIFICATE")
				assert.NotEmpty(t, cert.GetSha1Fingerprint())
				assert.NotNil(t, cert.GetExpireTime())

				require.NoError(t, e.dial(t, got, "").Ping(t.Context()).Err())

				plain, _ := proto.Clone(got).(*redispb.Instance)
				plain.ServerCaCerts = nil
				require.Error(t, e.dial(t, plain, "").Ping(t.Context()).Err())
			},
		},
		{
			name: "configs labels and maintenance are kept",
			in: basic(func(i *redispb.Instance) {
				i.DisplayName = "cache"
				i.Labels = map[string]string{"env": "test"}
				i.RedisConfigs = map[string]string{"maxmemory-policy": "allkeys-lru", "notify-keyspace-events": "KEA"}
				i.MaintenancePolicy = &redispb.MaintenancePolicy{
					WeeklyMaintenanceWindow: []*redispb.WeeklyMaintenanceWindow{{Day: 1}},
				}
			}),
			check: func(t *testing.T, _ *env, got *redispb.Instance) {
				t.Helper()
				assert.Equal(t, "cache", got.GetDisplayName())
				assert.Equal(t, map[string]string{"env": "test"}, got.GetLabels())
				assert.Equal(t, "allkeys-lru", got.GetRedisConfigs()["maxmemory-policy"])
				assert.NotNil(t, got.GetMaintenancePolicy().GetCreateTime())
			},
		},
		{
			name: "rdb persistence schedules snapshots",
			in: basic(func(i *redispb.Instance) {
				i.PersistenceConfig = &redispb.PersistenceConfig{
					PersistenceMode:   redispb.PersistenceConfig_RDB,
					RdbSnapshotPeriod: redispb.PersistenceConfig_SIX_HOURS,
				}
			}),
			check: func(t *testing.T, _ *env, got *redispb.Instance) {
				t.Helper()
				pc := got.GetPersistenceConfig()
				assert.Equal(t, redispb.PersistenceConfig_RDB, pc.GetPersistenceMode())
				assert.Equal(
					t,
					pc.GetRdbSnapshotStartTime().AsTime().Add(6*3600e9),
					pc.GetRdbNextSnapshotTime().AsTime(),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			got := e.create(t, "inst", tt.in)
			tt.check(t, e, got)

			again, err := e.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: got.GetName()})
			require.NoError(t, err)
			assert.Equal(t, got.GetHost(), again.GetHost())
			assert.Equal(t, got.GetPort(), again.GetPort())
		})
	}
}

func TestCreateInstanceInvalid(t *testing.T) {
	t.Parallel()

	ha := func(mut ...func(*redispb.Instance)) *redispb.Instance {
		return basic(append([]func(*redispb.Instance){func(i *redispb.Instance) {
			i.Tier = redispb.Instance_STANDARD_HA
		}}, mut...)...)
	}

	tests := []struct {
		in     *redispb.Instance
		name   string
		id     string
		parent string
		code   codes.Code
	}{
		{name: "missing tier", id: "i", in: &redispb.Instance{MemorySizeGb: 1}, code: codes.InvalidArgument},
		{
			name: "zero memory",
			id:   "i",
			in:   &redispb.Instance{Tier: redispb.Instance_BASIC},
			code: codes.InvalidArgument,
		},
		{
			name: "memory too big",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.MemorySizeGb = 301 }),
			code: codes.InvalidArgument,
		},
		{
			name: "bad version",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.RedisVersion = "REDIS_9_9" }),
			code: codes.InvalidArgument,
		},
		{name: "bad id", id: "Bad_ID", in: basic(), code: codes.InvalidArgument},
		{name: "id too long", id: "a" + string(make([]byte, 40)), in: basic(), code: codes.InvalidArgument},
		{name: "nil instance", id: "i", code: codes.InvalidArgument},
		{
			name:   "unknown location",
			id:     "i",
			in:     basic(),
			parent: "projects/p/locations/mars-north1",
			code:   codes.NotFound,
		},
		{name: "wildcard location", id: "i", in: basic(), parent: "projects/p/locations/-", code: codes.NotFound},
		{name: "bad parent", id: "i", in: basic(), parent: "nonsense", code: codes.InvalidArgument},
		{
			name: "zone outside region",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.LocationId = "europe-west1-b" }),
			code: codes.InvalidArgument,
		},
		{
			name: "basic with alternative zone",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.AlternativeLocationId = "us-central1-b" }),
			code: codes.InvalidArgument,
		},
		{
			name: "ha same zones",
			id:   "i",
			in:   ha(func(i *redispb.Instance) { i.LocationId, i.AlternativeLocationId = zone, zone }),
			code: codes.InvalidArgument,
		},
		{
			name: "basic with replicas",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.ReplicaCount = 2 }),
			code: codes.InvalidArgument,
		},
		{
			name: "basic with read replicas",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.ReadReplicasMode = redispb.Instance_READ_REPLICAS_ENABLED }),
			code: codes.InvalidArgument,
		},
		{name: "too many replicas", id: "i", in: ha(func(i *redispb.Instance) {
			i.ReadReplicasMode, i.ReplicaCount = redispb.Instance_READ_REPLICAS_ENABLED, 6
		}), code: codes.InvalidArgument},
		{name: "read replicas on old redis", id: "i", in: ha(func(i *redispb.Instance) {
			i.ReadReplicasMode, i.RedisVersion = redispb.Instance_READ_REPLICAS_ENABLED, "REDIS_4_0"
		}), code: codes.InvalidArgument},
		{
			name: "unsupported config",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.RedisConfigs = map[string]string{"bogus": "1"} }),
			code: codes.InvalidArgument,
		},
		{
			name: "bad config value",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.RedisConfigs = map[string]string{"maxmemory-policy": "nope"} }),
			code: codes.InvalidArgument,
		},
		{name: "config needs newer redis", id: "i", in: basic(func(i *redispb.Instance) {
			i.RedisVersion = "REDIS_3_2"
			i.RedisConfigs = map[string]string{"activedefrag": "yes"}
		}), code: codes.InvalidArgument},
		{
			name: "maxmemory-gb beyond memory",
			id:   "i",
			in:   basic(func(i *redispb.Instance) { i.RedisConfigs = map[string]string{"maxmemory-gb": "2"} }),
			code: codes.InvalidArgument,
		},
		{name: "rdb without period", id: "i", in: basic(func(i *redispb.Instance) {
			i.PersistenceConfig = &redispb.PersistenceConfig{PersistenceMode: redispb.PersistenceConfig_RDB}
		}), code: codes.InvalidArgument},
		{name: "two maintenance windows", id: "i", in: basic(func(i *redispb.Instance) {
			i.MaintenancePolicy = &redispb.MaintenancePolicy{
				WeeklyMaintenanceWindow: []*redispb.WeeklyMaintenanceWindow{{Day: 1}, {Day: 2}},
			}
		}), code: codes.InvalidArgument},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parent := tt.parent
			if parent == "" {
				parent = loc
			}

			_, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
				Parent: parent, InstanceId: tt.id, Instance: tt.in,
			})
			assert.Equal(t, tt.code, status.Code(err), "%v", err)
		})
	}
}

func TestInstanceLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	got := e.create(t, "life", basic())

	_, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
		Parent: loc, InstanceId: "life", Instance: basic(),
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.GetInstanceAuthString(t.Context(), &redispb.GetInstanceAuthStringRequest{Name: got.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	dop, err := e.client.DeleteInstance(t.Context(), &redispb.DeleteInstanceRequest{Name: got.GetName()})
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool { return dop.Poll(t.Context()) == nil && dop.Done() },
		10*time.Second,
		2*time.Millisecond,
	)

	_, err = e.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.DeleteInstance(t.Context(), &redispb.DeleteInstanceRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_ = e.create(t, "life", basic())
}

func TestListInstances(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for i := range 5 {
		e.create(t, fmt.Sprintf("list-%d", i), basic())
	}

	other, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
		Parent: "projects/test-project/locations/europe-west1", InstanceId: "eu", Instance: basic(),
	})
	require.NoError(t, err)

	_, err = await(t, other)
	require.NoError(t, err)

	tests := []struct {
		name   string
		parent string
		size   int32
		want   int
		pages  int
	}{
		{name: "one location", parent: loc, want: 5, pages: 1},
		{name: "paged", parent: loc, size: 2, want: 5, pages: 3},
		{name: "all locations", parent: "projects/test-project/locations/-", want: 6, pages: 1},
		{name: "empty location", parent: "projects/test-project/locations/us-east1", want: 0, pages: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListInstances(
				t.Context(),
				&redispb.ListInstancesRequest{Parent: tt.parent, PageSize: tt.size},
			)
			n := 0

			for {
				inst, nextErr := it.Next()
				if nextErr != nil {
					break
				}

				assert.NotEmpty(t, inst.GetName())
				n++
			}

			assert.Equal(t, tt.want, n)
		})
	}

	it := e.client.ListInstances(t.Context(), &redispb.ListInstancesRequest{Parent: loc, PageSize: -1})
	_, err = it.Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestInstanceCreatingState(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	release := make(chan struct{})
	e.handler.Backend.SetJobGate(func() { <-release })

	op, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
		Parent: loc, InstanceId: "slow", Instance: basic(),
	})
	require.NoError(t, err)
	assert.False(t, op.Done())

	got, err := e.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: loc + "/instances/slow"})
	require.NoError(t, err)
	assert.Equal(t, redispb.Instance_CREATING, got.GetState())
	assert.Empty(t, got.GetHost())

	for name, call := range map[string]func() error{
		"delete": func() error {
			_, callErr := e.client.DeleteInstance(t.Context(), &redispb.DeleteInstanceRequest{Name: got.GetName()})

			return callErr
		},
		"upgrade": func() error {
			_, callErr := e.client.UpgradeInstance(t.Context(), &redispb.UpgradeInstanceRequest{
				Name: got.GetName(), RedisVersion: "REDIS_7_2",
			})

			return callErr
		},
	} {
		assert.Equal(t, codes.FailedPrecondition, status.Code(call()), name)
	}

	close(release)

	ready, err := await(t, op)
	require.NoError(t, err)
	assert.Equal(t, redispb.Instance_READY, ready.GetState())
	assert.NotEmpty(t, ready.GetHost())
}
