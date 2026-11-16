package gcpredis_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/redis/apiv1/redispb"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestRedisConfigRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		configs map[string]string
		name    string
		version string
		memory  int32
		code    codes.Code
	}{
		{name: "documented set", memory: 2, configs: map[string]string{
			"maxmemory-gb": "1.5", "maxmemory-policy": "allkeys-lfu", "notify-keyspace-events": "KEA",
			"activedefrag": "yes", "databases": "32", "lazyfree-lazy-eviction": "yes", "lazyfree-lazy-expire": "no",
			"lazyfree-lazy-user-del": "yes", "lazyfree-lazy-user-flush": "no", "lfu-decay-time": "2",
			"lfu-log-factor": "5", "maxmemory-clients": "10%", "stream-node-max-bytes": "0",
			"stream-node-max-entries": "0", "timeout": "300",
		}},
		{
			name:    "slowlog is not modifiable",
			configs: map[string]string{"slowlog-max-len": "10"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "slowlog threshold",
			configs: map[string]string{"slowlog-log-slower-than": "1"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "replica ignore",
			configs: map[string]string{"replica-ignore-maxmemory": "yes"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "maxmemory below 20 percent",
			configs: map[string]string{"maxmemory-gb": "0.1"},
			code:    codes.InvalidArgument,
		},
		{name: "maxmemory at 20 percent", configs: map[string]string{"maxmemory-gb": "0.2"}},
		{
			name:    "lazyfree needs 7.0",
			version: "REDIS_6_X",
			configs: map[string]string{"lazyfree-lazy-user-del": "yes"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "maxmemory-clients needs 7.0",
			version: "REDIS_6_X",
			configs: map[string]string{"maxmemory-clients": "5%"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "maxmemory-clients without percent",
			configs: map[string]string{"maxmemory-clients": "50"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "maxmemory-clients over 100",
			configs: map[string]string{"maxmemory-clients": "101%"},
			code:    codes.InvalidArgument,
		},
		{name: "timeout below 120", configs: map[string]string{"timeout": "60"}, code: codes.InvalidArgument},
		{name: "timeout zero", configs: map[string]string{"timeout": "0"}},
		{name: "timeout 120", configs: map[string]string{"timeout": "120"}},
		{name: "databases zero", configs: map[string]string{"databases": "0"}, code: codes.InvalidArgument},
		{name: "databases over 100", configs: map[string]string{"databases": "101"}, code: codes.InvalidArgument},
		{
			name:    "lfu decay zero",
			version: "REDIS_7_0",
			configs: map[string]string{"lfu-decay-time": "0"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "lfu log factor negative",
			configs: map[string]string{"lfu-log-factor": "-1"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "lfu policy needs 4.0",
			version: "REDIS_3_2",
			configs: map[string]string{"maxmemory-policy": "allkeys-lfu"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "stream event flag",
			configs: map[string]string{"notify-keyspace-events": "Kt"},
			code:    codes.InvalidArgument,
		},
		{name: "empty keyspace events", configs: map[string]string{"notify-keyspace-events": ""}},
		{
			name:    "stream nodes need 5.0",
			version: "REDIS_4_0",
			configs: map[string]string{"stream-node-max-bytes": "1"},
			code:    codes.InvalidArgument,
		},
	}

	e := newEnv(t)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := basic(func(inst *redispb.Instance) {
				inst.RedisConfigs = tt.configs
				if tt.memory > 0 {
					inst.MemorySizeGb = tt.memory
				}

				if tt.version != "" {
					inst.RedisVersion = tt.version
				}
			})

			_, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
				Parent: loc, InstanceId: fmt.Sprintf("cfg-%d", i), Instance: in,
			})
			assert.Equal(t, tt.code, status.Code(err), "%v", err)
		})
	}
}

func updateConfigs(
	t *testing.T,
	e *env,
	name string,
	configs map[string]string,
	paths ...string,
) error {
	t.Helper()

	op, err := e.client.UpdateInstance(t.Context(), &redispb.UpdateInstanceRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
		Instance:   &redispb.Instance{Name: name, RedisConfigs: configs, MemorySizeGb: 2},
	})
	if err != nil {
		return err
	}

	_, err = await(t, op)

	return err
}

func memoryInfo(t *testing.T, c *goredis.Client) string {
	t.Helper()

	return c.Info(t.Context(), "memory").Val()
}

func fillKeys(t *testing.T, c *goredis.Client, keys ...string) {
	t.Helper()

	for _, k := range keys {
		require.NoError(t, c.Set(t.Context(), k, strings.Repeat("x", 200), 0).Err())
	}
}

func TestMemorySizeBecomesMaxmemory(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetMemoryUnit(2000)

	inst := e.create(t, "mem", basic())
	c := e.dial(t, inst, "")

	assert.Contains(t, memoryInfo(t, c), "maxmemory:2000")
	assert.Contains(t, memoryInfo(t, c), "maxmemory_policy:volatile-lru", "Memorystore's default policy")
	assert.Contains(t, c.Info(t.Context(), "server").Val(), "redis_version:7.0.0")

	fillKeys(t, c, "a", "b", "c", "d", "e", "f")

	err := c.Set(t.Context(), "g", strings.Repeat("x", 200), 0).Err()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OOM command not allowed when used memory > 'maxmemory'")
	require.NoError(t, c.Get(t.Context(), "a").Err(), "reads keep working")

	err = updateConfigs(t, e, inst.GetName(), nil, "memory_size_gb")
	require.NoError(t, err)
	assert.Contains(t, memoryInfo(t, c), "maxmemory:4000", "resizing the instance moves the limit")
	require.NoError(t, c.Set(t.Context(), "g", strings.Repeat("x", 200), 0).Err())
}

func TestRedisConfigsDriveEviction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		configs map[string]string
		name    string
		wantGB  string
		evicted bool
	}{
		{
			name:    "allkeys-lru",
			configs: map[string]string{"maxmemory-policy": "allkeys-lru"},
			evicted: true,
			wantGB:  "maxmemory:2000",
		},
		{name: "maxmemory-gb lowers the limit", configs: map[string]string{
			"maxmemory-policy": "allkeys-lru", "maxmemory-gb": "0.5",
		}, evicted: true, wantGB: "maxmemory:1000"},
		{name: "noeviction", configs: map[string]string{"maxmemory-policy": "noeviction"}, wantGB: "maxmemory:2000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetMemoryUnit(2000)

			inst := e.create(t, "evict", basic(func(i *redispb.Instance) { i.RedisConfigs = tt.configs }))
			c := e.dial(t, inst, "")

			assert.Contains(t, memoryInfo(t, c), tt.wantGB)

			for i := range 12 {
				err := c.Set(t.Context(), fmt.Sprintf("k%02d", i), strings.Repeat("v", 200), 0).Err()
				if tt.evicted {
					require.NoError(t, err)
				}
			}

			assert.Equal(t, tt.evicted, c.Exists(t.Context(), "k00").Val() == 0, "oldest key evicted: %v", tt.evicted)
			if tt.evicted {
				assert.Equal(t, int64(2), c.Exists(t.Context(), "k11", "k10").Val(), "newest keys survive")
			}
		})
	}
}

func TestRedisConfigUpdateReachesEngine(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetMemoryUnit(2000)

	inst := e.create(t, "upd", basic(func(i *redispb.Instance) {
		i.RedisConfigs = map[string]string{"maxmemory-policy": "noeviction"}
	}))
	c := e.dial(t, inst, "")
	fillKeys(t, c, "a", "b", "c", "d", "e", "f")
	require.Error(t, c.Set(t.Context(), "g", strings.Repeat("x", 200), 0).Err())

	err := updateConfigs(t, e, inst.GetName(), map[string]string{"maxmemory-policy": "allkeys-lru"}, "redis_configs")
	require.NoError(t, err)

	require.NoError(
		t,
		c.Set(t.Context(), "g", strings.Repeat("x", 200), 0).Err(),
		"the new policy evicts instead of refusing",
	)
	assert.Contains(t, memoryInfo(t, c), "maxmemory_policy:allkeys-lru")

	err = updateConfigs(t, e, inst.GetName(), map[string]string{"databases": "5"}, "redis_configs")
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "databases is fixed at creation")
}

func TestKeyspaceNotificationsFromConfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.create(t, "events", basic(func(i *redispb.Instance) {
		i.RedisConfigs = map[string]string{"notify-keyspace-events": "KEA"}
	}))
	c := e.dial(t, inst, "")

	ps := c.PSubscribe(t.Context(), "__keyspace@0__:*")
	t.Cleanup(func() { _ = ps.Close() })

	_, err := ps.Receive(t.Context())
	require.NoError(t, err)

	require.NoError(t, c.Set(t.Context(), "watched", "v", 0).Err())

	raw, err := ps.ReceiveTimeout(t.Context(), 5*time.Second)
	require.NoError(t, err)

	msg, ok := raw.(*goredis.Message)
	require.True(t, ok)
	assert.Equal(t, "__keyspace@0__:watched", msg.Channel)
	assert.Equal(t, "set", msg.Payload)

	err = updateConfigs(t, e, inst.GetName(), map[string]string{"notify-keyspace-events": ""}, "redis_configs")
	require.NoError(t, err)
	require.NoError(t, c.Set(t.Context(), "quiet", "v", 0).Err())
	require.NoError(t, c.Del(t.Context(), "watched").Err())

	got := make(chan *goredis.Message, 1)

	go func() {
		m, rerr := ps.ReceiveTimeout(t.Context(), 300*time.Millisecond)
		if rerr == nil {
			if recv, isMsg := m.(*goredis.Message); isMsg {
				got <- recv
			}
		}

		close(got)
	}()

	_, delivered := <-got
	assert.False(t, delivered, "emptying notify-keyspace-events stops notifications")
}

func TestDatabasesConfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.create(t, "dbs", basic(func(i *redispb.Instance) {
		i.RedisConfigs = map[string]string{"databases": "40"}
	}))

	conn := e.dial(t, inst, "").Conn()
	t.Cleanup(func() { _ = conn.Close() })

	require.NoError(t, conn.Do(t.Context(), "SELECT", "39").Err())
	require.NoError(t, conn.Set(t.Context(), "deep", "v", 0).Err())
	require.ErrorContains(t, conn.Do(t.Context(), "SELECT", "40").Err(), "DB index is out of range")

	plain := e.create(t, "dbs16", basic())
	pc := e.dial(t, plain, "").Conn()
	t.Cleanup(func() { _ = pc.Close() })
	require.ErrorContains(t, pc.Do(t.Context(), "SELECT", "16").Err(), "DB index is out of range")
}

func TestRedisConfigsReachEngineSettings(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.create(t, "settings", basic(func(i *redispb.Instance) {
		i.RedisVersion = "REDIS_7_2"
		i.RedisConfigs = map[string]string{
			"activedefrag": "yes", "lazyfree-lazy-eviction": "yes", "lazyfree-lazy-expire": "yes",
			"lazyfree-lazy-user-del": "yes", "lazyfree-lazy-user-flush": "yes", "lfu-decay-time": "7",
			"lfu-log-factor": "3", "maxmemory-clients": "25%", "stream-node-max-bytes": "512",
			"stream-node-max-entries": "8", "timeout": "240", "databases": "20", "notify-keyspace-events": "Kg",
			"maxmemory-gb": "0.5", "maxmemory-policy": "volatile-ttl",
		}
	}))

	got := e.handler.Backend.EngineSettings(inst.GetName())

	assert.True(t, got.ActiveDefrag)
	assert.True(t, got.LazyfreeLazyEviction)
	assert.True(t, got.LazyfreeLazyExpire)
	assert.True(t, got.LazyfreeLazyUserDel)
	assert.True(t, got.LazyfreeLazyUserFlsh)
	assert.Equal(t, 7, got.LFUDecayTime)
	assert.Equal(t, 3, got.LFULogFactor)
	assert.Equal(t, 25, got.MaxMemoryClients)
	assert.Equal(t, 512, got.StreamNodeMaxBytes)
	assert.Equal(t, 8, got.StreamNodeMaxEntries)
	assert.Equal(t, 240*time.Second, got.Timeout)
	assert.Equal(t, 20, got.Databases)
	assert.Equal(t, "Kg", got.NotifyKeyspaceEvents)
	assert.Equal(t, int64(1)<<29, got.MaxMemory)
	assert.Equal(t, "volatile-ttl", got.Policy)
	assert.Equal(t, "7.2.0", got.RedisVersion)
	assert.Equal(t, 128, got.SlowlogMaxLen, "slowlog settings are Memorystore's fixed defaults")
	assert.Equal(t, 10*time.Millisecond, got.SlowlogLogSlowerThan)
}

func TestImportStreamsFromRealServers(t *testing.T) {
	t.Parallel()

	files := []string{"redis-6.2-stream.rdb", "redis-7.0-stream.rdb", "redis-7.2-stream.rdb", "redis-8.0-stream.rdb"}

	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(filepath.Join("..", "..", "pkgs", "redisengine", "testdata", file))
			require.NoError(t, err)

			e := newEnv(t)
			inst := e.create(t, "streams", basic())
			e.store.objs["bkt/streams.rdb"] = data

			imp, err := e.client.ImportInstance(t.Context(), &redispb.ImportInstanceRequest{
				Name: inst.GetName(),
				InputConfig: &redispb.InputConfig{Source: &redispb.InputConfig_GcsSource{
					GcsSource: &redispb.GcsSource{Uri: "gs://bkt/streams.rdb"},
				}},
			})
			require.NoError(t, err)

			_, err = await(t, imp)
			require.NoError(t, err)

			c := e.dial(t, inst, "")
			assert.EqualValues(t, 153, c.XLen(t.Context(), "st").Val())

			groups, err := c.XInfoGroups(t.Context(), "st").Result()
			require.NoError(t, err)
			require.Len(t, groups, 2)

			pending, err := c.XPendingExt(t.Context(), &goredis.XPendingExtArgs{
				Stream: "st", Group: "g1", Start: "-", End: "+", Count: 10,
			}).Result()
			require.NoError(t, err)
			assert.Len(t, pending, 4)
		})
	}
}

func TestImportHashFieldTTLFromRealServer(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "pkgs", "redisengine", "testdata", "redis-8.0-hashttl.rdb"))
	require.NoError(t, err)

	e := newEnv(t)
	inst := e.create(t, "hfe", basic())
	e.store.objs["bkt/hfe.rdb"] = data

	imp, err := e.client.ImportInstance(t.Context(), &redispb.ImportInstanceRequest{
		Name: inst.GetName(),
		InputConfig: &redispb.InputConfig{Source: &redispb.InputConfig_GcsSource{
			GcsSource: &redispb.GcsSource{Uri: "gs://bkt/hfe.rdb"},
		}},
	})
	require.NoError(t, err)

	_, err = await(t, imp)
	require.NoError(t, err)

	c := e.dial(t, inst, "")
	ttl, err := c.HPTTL(t.Context(), "hfe", "f1", "f2").Result()
	require.NoError(t, err)
	assert.Positive(t, ttl[0])
	assert.EqualValues(t, -1, ttl[1])
}

func TestExportStreamsAndHashTTLs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	src := e.create(t, "exp-src", basic(func(i *redispb.Instance) {
		i.RedisConfigs = map[string]string{"stream-node-max-entries": "1"}
	}))
	dst := e.create(t, "exp-dst", basic())

	sc := e.dial(t, src, "")
	ctx := t.Context()

	for i := range 5 {
		require.NoError(
			t,
			sc.XAdd(ctx, &goredis.XAddArgs{Stream: "s", ID: fmt.Sprintf("%d-1", i+1), Values: []string{"f", "v"}}).
				Err(),
		)
	}

	require.NoError(t, sc.XGroupCreate(ctx, "s", "g", "0").Err())
	read := &goredis.XReadGroupArgs{Group: "g", Consumer: "c1", Streams: []string{"s", ">"}, Count: 2}
	_, err := sc.XReadGroup(ctx, read).
		Result()
	require.NoError(t, err)

	require.NoError(t, sc.HSet(ctx, "h", "a", "1", "b", "2").Err())
	require.NoError(t, sc.HExpire(ctx, "h", time.Hour, "a").Err())

	exp, err := e.client.ExportInstance(ctx, &redispb.ExportInstanceRequest{
		Name: src.GetName(),
		OutputConfig: &redispb.OutputConfig{Destination: &redispb.OutputConfig_GcsDestination{
			GcsDestination: &redispb.GcsDestination{Uri: "gs://bkt/full.rdb"},
		}},
	})
	require.NoError(t, err)

	_, err = await(t, exp)
	require.NoError(t, err)
	assert.Equal(t, "REDIS0012", string(e.store.objs["bkt/full.rdb"][:9]), "hash field TTLs need RDB 12")

	imp, err := e.client.ImportInstance(ctx, &redispb.ImportInstanceRequest{
		Name: dst.GetName(),
		InputConfig: &redispb.InputConfig{Source: &redispb.InputConfig_GcsSource{
			GcsSource: &redispb.GcsSource{Uri: "gs://bkt/full.rdb"},
		}},
	})
	require.NoError(t, err)

	_, err = await(t, imp)
	require.NoError(t, err)

	dc := e.dial(t, dst, "")
	assert.EqualValues(t, 5, dc.XLen(ctx, "s").Val())

	pending, err := dc.XPending(ctx, "s", "g").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 2, pending.Count)
	assert.Equal(t, map[string]int64{"c1": 2}, pending.Consumers)

	ttl, err := dc.HPTTL(ctx, "h", "a", "b").Result()
	require.NoError(t, err)
	assert.Positive(t, ttl[0])
	assert.EqualValues(t, -1, ttl[1])
}

func TestRestoreReappliesConfigs(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	inst := src.create(t, "restore", basic(func(i *redispb.Instance) {
		i.RedisConfigs = map[string]string{
			"maxmemory-policy":       "allkeys-lfu",
			"databases":              "30",
			"notify-keyspace-events": "KEA",
		}
	}))

	require.NoError(t, src.dial(t, inst, "").XAdd(t.Context(), &goredis.XAddArgs{
		Stream: "s", ID: "1-1", Values: []string{"f", "v"},
	}).Err())
	require.NoError(t, src.dial(t, inst, "").XGroupCreate(t.Context(), "s", "g", "0").Err())

	blob := src.handler.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), blob))

	got := dst.handler.Backend.EngineSettings(inst.GetName())
	assert.Equal(t, "allkeys-lfu", got.Policy)
	assert.Equal(t, 30, got.Databases)
	assert.Equal(t, "KEA", got.NotifyKeyspaceEvents)

	restored, err := dst.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: inst.GetName()})
	require.NoError(t, err)

	groups, err := dst.dial(t, restored, "").XInfoGroups(t.Context(), "s").Result()
	require.NoError(t, err)
	require.Len(t, groups, 1, "consumer groups survive a restore")
	assert.Equal(t, "g", groups[0].Name)
}
