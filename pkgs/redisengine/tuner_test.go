package redisengine_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func tunedEngine(t *testing.T, mutate func(*redisengine.Settings)) (*redisengine.Engine, *redis.Client) {
	t.Helper()

	e := startEngine(t)
	s := redisengine.DefaultSettings()
	mutate(&s)
	require.NoError(t, e.Tune(s))

	return e, dialEngine(t, e, "")
}

func fill(t *testing.T, c *redis.Client, keys ...string) {
	t.Helper()

	for _, k := range keys {
		require.NoError(t, c.Set(t.Context(), k, strings.Repeat("x", 200), 0).Err(), k)
	}
}

func existing(t *testing.T, c *redis.Client, keys ...string) []string {
	t.Helper()

	var out []string

	for _, k := range keys {
		if c.Exists(t.Context(), k).Val() == 1 {
			out = append(out, k)
		}
	}

	return out
}

func TestTune_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*redisengine.Settings)
		name   string
	}{
		{name: "policy", mutate: func(s *redisengine.Settings) { s.Policy = "lru" }},
		{name: "flags", mutate: func(s *redisengine.Settings) { s.NotifyKeyspaceEvents = "KQ" }},
		{name: "databases", mutate: func(s *redisengine.Settings) { s.Databases = 0 }},
		{name: "negative memory", mutate: func(s *redisengine.Settings) { s.MaxMemory = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := redisengine.DefaultSettings()
			tt.mutate(&s)
			require.ErrorIs(t, startEngine(t).Tune(s), redisengine.ErrInvalidSettings)
		})
	}
}

func TestTune_MemoryLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policy      string
		wantEvicted []string
		wantOOM     bool
	}{
		{name: "noeviction refuses writes", policy: redisengine.PolicyNoEviction, wantOOM: true},
		{
			name:        "allkeys-lru drops the least recently used",
			policy:      redisengine.PolicyAllKeysLRU,
			wantEvicted: []string{"k2"},
		},
		{name: "volatile-lru ignores keys without ttl", policy: redisengine.PolicyVolatileLRU, wantOOM: true},
		{
			name:        "allkeys-lfu drops the least frequently used",
			policy:      redisengine.PolicyAllKeysLFU,
			wantEvicted: []string{"k2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := tunedEngine(t, func(s *redisengine.Settings) {
				s.MaxMemory = 2000
				s.Policy = tt.policy
				s.LFULogFactor = 0
			})
			ctx := t.Context()

			fill(t, c, "k1", "k2", "k3")

			for range 20 {
				require.NoError(t, c.Get(ctx, "k1").Err())
				require.NoError(t, c.Get(ctx, "k3").Err())
			}

			for i := range 3 {
				require.NoError(t, c.Set(ctx, fmt.Sprintf("n%d", i), strings.Repeat("y", 200), 0).Err())
			}

			oom := c.Set(ctx, "n3", strings.Repeat("y", 200), 0).Err()

			if tt.wantOOM {
				require.Error(t, oom)
				assert.Contains(t, oom.Error(), "OOM command not allowed when used memory > 'maxmemory'")
				assert.Equal(t, []string{"k1", "k2", "k3"}, existing(t, c, "k1", "k2", "k3"), "nothing evicted")
				assert.NoError(t, c.Get(ctx, "k1").Err(), "reads still work")

				return
			}

			require.NoError(t, oom)

			for _, gone := range tt.wantEvicted {
				assert.Zero(t, c.Exists(ctx, gone).Val(), gone)
			}

			assert.Equal(t, []string{"k1", "k3"}, existing(t, c, "k1", "k3"))
		})
	}
}

func TestTune_VolatileEviction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
		want   []string
	}{
		{name: "volatile-ttl", policy: redisengine.PolicyVolatileTTL, want: []string{"short"}},
		{name: "volatile-lru", policy: redisengine.PolicyVolatileLRU, want: []string{"short"}},
		{name: "volatile-random", policy: redisengine.PolicyVolatileRandom},
		{name: "allkeys-random", policy: redisengine.PolicyAllKeysRandom},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := tunedEngine(t, func(s *redisengine.Settings) {
				s.MaxMemory = 1300
				s.Policy = tt.policy
			})
			ctx := t.Context()
			val := strings.Repeat("v", 200)

			require.NoError(t, c.Set(ctx, "short", val, time.Hour).Err())
			require.NoError(t, c.Set(ctx, "long", val, 48*time.Hour).Err())
			require.NoError(t, c.Set(ctx, "forever", val, 0).Err())
			require.NoError(t, c.Get(ctx, "long").Err())

			for i := range 4 {
				require.NoError(t, c.Set(ctx, fmt.Sprintf("extra%d", i), val, time.Duration(i+2)*100*time.Hour).Err())
			}

			if tt.policy != redisengine.PolicyVolatileRandom && tt.policy != redisengine.PolicyAllKeysRandom {
				assert.Zero(t, c.Exists(ctx, "short").Val(), "shortest ttl goes first")
				assert.Equal(t, int64(1), c.Exists(ctx, "forever").Val(), "keys without ttl are never volatile victims")
			}

			info := c.Info(ctx, "stats").Val()
			assert.Contains(t, info, "evicted_keys:")
			assert.NotContains(t, info, "evicted_keys:0")
		})
	}
}

func TestTune_InfoMemory(t *testing.T) {
	t.Parallel()

	_, c := tunedEngine(t, func(s *redisengine.Settings) {
		s.MaxMemory = 1 << 20
		s.Policy = redisengine.PolicyAllKeysLRU
		s.RedisVersion = "7.0.15"
	})
	ctx := t.Context()

	fill(t, c, "a", "b")
	require.NoError(t, c.Set(ctx, "ttl", "v", time.Hour).Err())

	mem := c.Info(ctx, "memory").Val()
	assert.Contains(t, mem, "maxmemory:1048576")
	assert.Contains(t, mem, "maxmemory_policy:allkeys-lru")
	assert.Contains(t, mem, "active_defrag_running:0")
	assert.NotContains(t, mem, "used_memory:0\r\n")

	assert.Contains(t, c.Info(ctx, "keyspace").Val(), "db0:keys=3,expires=1")
	assert.Contains(t, c.Info(ctx, "server").Val(), "redis_version:7.0.15")
	assert.Contains(t, c.Info(ctx).Val(), "connected_clients:")
	assert.Contains(t, c.Info(ctx, "clients").Val(), "connected_clients:")
}

func TestTune_SelectRange(t *testing.T) {
	t.Parallel()

	_, c := tunedEngine(t, func(s *redisengine.Settings) { s.Databases = 4 })
	ctx := t.Context()

	conn := c.Conn()
	t.Cleanup(func() { _ = conn.Close() })

	require.NoError(t, conn.Do(ctx, "SELECT", "3").Err())
	require.NoError(t, conn.Set(ctx, "in-db3", "v", 0).Err())
	require.ErrorContains(t, conn.Do(ctx, "SELECT", "4").Err(), "DB index is out of range")
	assert.Equal(t, "v", conn.Get(ctx, "in-db3").Val(), "failed SELECT keeps the database")
}

func TestTune_Slowlog(t *testing.T) {
	t.Parallel()

	_, c := tunedEngine(t, func(s *redisengine.Settings) {
		s.SlowlogLogSlowerThan = 0
		s.SlowlogMaxLen = 3
	})
	ctx := t.Context()

	for i := range 5 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v", 0).Err())
	}

	assert.EqualValues(t, 3, c.SlowLogLen(ctx).Val())

	entries, err := c.SlowLogGet(ctx, 2).Result()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Len(t, entries[0].Args, 2)
	assert.Equal(t, "SLOWLOG", strings.ToUpper(entries[0].Args[0]))
	assert.Equal(t, "LEN", strings.ToUpper(entries[0].Args[1]))
	assert.Equal(t, []string{"SET", "k4", "v"}, entries[1].Args)
	assert.Greater(t, entries[0].ID, entries[1].ID)

	require.NoError(t, c.Do(ctx, "SLOWLOG", "RESET").Err())
	assert.EqualValues(t, 1, c.SlowLogLen(ctx).Val(), "RESET itself is logged afterwards")
}

func TestTune_SlowlogDisabled(t *testing.T) {
	t.Parallel()

	_, c := tunedEngine(t, func(s *redisengine.Settings) { s.SlowlogLogSlowerThan = -1 })
	ctx := t.Context()

	require.NoError(t, c.Set(ctx, "k", "v", 0).Err())
	assert.Zero(t, c.SlowLogLen(ctx).Val())
}

type recorder struct {
	msgs []string
	mu   sync.Mutex
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.msgs...)
}

func subscribeEvents(t *testing.T, c *redis.Client) *recorder {
	t.Helper()

	ps := c.PSubscribe(t.Context(), "__key*__:*")
	t.Cleanup(func() { _ = ps.Close() })

	_, err := ps.Receive(t.Context())
	require.NoError(t, err)

	rec := &recorder{}
	ch := ps.Channel()

	go func() {
		for m := range ch {
			rec.mu.Lock()
			rec.msgs = append(rec.msgs, m.Channel+"="+m.Payload)
			rec.mu.Unlock()
		}
	}()

	return rec
}

func TestTune_KeyspaceEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run   func(ctx context.Context, c *redis.Client)
		name  string
		flags string
		want  []string
	}{
		{
			name: "string and generic", flags: "KEA",
			run: func(ctx context.Context, c *redis.Client) {
				c.Set(ctx, "k", "v", 0)
				c.Expire(ctx, "k", time.Hour)
				c.Del(ctx, "k")
			},
			want: []string{
				"__keyspace@0__:k=set", "__keyevent@0__:set=k",
				"__keyspace@0__:k=expire", "__keyevent@0__:expire=k",
				"__keyspace@0__:k=del", "__keyevent@0__:del=k",
			},
		},
		{
			name: "keyspace channel only", flags: "Kg$",
			run: func(ctx context.Context, c *redis.Client) {
				c.Set(ctx, "k", "v", 0)
				c.LPush(ctx, "l", "1")
			},
			want: []string{"__keyspace@0__:k=set"},
		},
		{
			name: "keyevent channel only list", flags: "El",
			run: func(ctx context.Context, c *redis.Client) {
				c.LPush(ctx, "l", "1", "2")
				c.RPop(ctx, "l")
				c.RPop(ctx, "l")
				c.Set(ctx, "k", "v", 0)
			},
			want: []string{"__keyevent@0__:lpush=l", "__keyevent@0__:rpop=l", "__keyevent@0__:rpop=l"},
		},
		{
			name: "hash set zset stream", flags: "KEshzt",
			run: func(ctx context.Context, c *redis.Client) {
				c.HSet(ctx, "h", "f", "v")
				c.SAdd(ctx, "s", "m")
				c.ZAdd(ctx, "z", redis.Z{Score: 1, Member: "m"})
				c.XAdd(ctx, &redis.XAddArgs{Stream: "x", Values: []string{"a", "b"}})
				c.Set(ctx, "ignored", "v", 0)
			},
			want: []string{
				"__keyspace@0__:h=hset", "__keyevent@0__:hset=h",
				"__keyspace@0__:s=sadd", "__keyevent@0__:sadd=s",
				"__keyspace@0__:z=zadd", "__keyevent@0__:zadd=z",
				"__keyspace@0__:x=xadd", "__keyevent@0__:xadd=x",
			},
		},
		{
			name: "failed conditional set", flags: "KE$",
			run: func(ctx context.Context, c *redis.Client) {
				c.Set(ctx, "k", "v", 0)
				c.SetNX(ctx, "k", "other", 0)
				c.Set(ctx, "k", "changed", 0)
			},
			want: []string{
				"__keyspace@0__:k=set", "__keyevent@0__:set=k", "__keyspace@0__:k=set", "__keyevent@0__:set=k",
			},
		},
		{
			name: "transaction", flags: "KEA",
			run: func(ctx context.Context, c *redis.Client) {
				_, _ = c.TxPipelined(ctx, func(p redis.Pipeliner) error {
					p.Set(ctx, "a", "1", 0)
					p.LPush(ctx, "b", "1")

					return nil
				})
			},
			want: []string{
				"__keyspace@0__:a=set", "__keyevent@0__:set=a", "__keyspace@0__:b=lpush", "__keyevent@0__:lpush=b",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := tunedEngine(t, func(s *redisengine.Settings) { s.NotifyKeyspaceEvents = tt.flags })
			rec := subscribeEvents(t, c)

			tt.run(t.Context(), c)

			require.Eventually(
				t,
				func() bool { return len(rec.all()) >= len(tt.want) },
				5*time.Second,
				10*time.Millisecond,
			)
			assert.ElementsMatch(t, tt.want, rec.all())
		})
	}
}

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (f *fakeClock) read() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

func newClocked(
	t *testing.T,
	mutate func(*redisengine.Settings),
) (*redisengine.Engine, *redis.Client, func(time.Duration)) {
	t.Helper()

	e, c := tunedEngine(t, mutate)
	fc := &fakeClock{now: time.Now()}

	redisengine.SetClock(e, fc.read)
	e.Mini.SetTime(fc.now)

	advance := func(d time.Duration) {
		fc.mu.Lock()
		fc.now = fc.now.Add(d)
		now := fc.now
		fc.mu.Unlock()

		e.Mini.SetTime(now)
		redisengine.Sweep(e)
	}

	return e, c, advance
}

func TestTune_ExpiredEvents(t *testing.T) {
	t.Parallel()

	_, c, advance := newClocked(t, func(s *redisengine.Settings) { s.NotifyKeyspaceEvents = "KEx" })
	ctx := t.Context()
	rec := subscribeEvents(t, c)

	require.NoError(t, c.Set(ctx, "k", "v", 500*time.Millisecond).Err())
	require.NoError(t, c.Set(ctx, "stays", "v", time.Hour).Err())

	advance(400 * time.Millisecond)
	assert.Empty(t, rec.all())
	assert.Equal(t, "v", c.Get(ctx, "k").Val())

	advance(200 * time.Millisecond)

	require.Eventually(t, func() bool { return len(rec.all()) == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{"__keyspace@0__:k=expired", "__keyevent@0__:expired=k"}, rec.all())
	assert.Equal(t, redis.Nil, c.Get(ctx, "k").Err())
	assert.Contains(t, c.Info(ctx, "stats").Val(), "expired_keys:1")
}

func TestEngine_KeysExpire(t *testing.T) {
	t.Parallel()

	e := startEngine(t)
	require.NoError(t, e.Mini.Set("k", "v"))
	e.Mini.SetTTL("k", time.Second)
	e.Mini.SetTime(time.Now().Add(2 * time.Second))

	assert.False(t, e.Mini.Exists("k"))
}

func TestTune_EvictedEvents(t *testing.T) {
	t.Parallel()

	_, c := tunedEngine(t, func(s *redisengine.Settings) {
		s.NotifyKeyspaceEvents = "KEe"
		s.MaxMemory = 1000
		s.Policy = redisengine.PolicyAllKeysLRU
	})
	ctx := t.Context()
	rec := subscribeEvents(t, c)

	fill(t, c, "a", "b", "c", "d")

	require.Eventually(t, func() bool { return len(rec.all()) >= 2 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{"__keyspace@0__:a=evicted", "__keyevent@0__:evicted=a"}, rec.all()[:2])
	assert.Zero(t, c.Exists(ctx, "a").Val())
}

func TestTune_LFUDecay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantEvict string
		decay     int
	}{
		{name: "counters decay", decay: 1, wantEvict: "hot"},
		{name: "decay disabled", decay: 0, wantEvict: "cold"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c, advance := newClocked(t, func(s *redisengine.Settings) {
				s.MaxMemory = 1200
				s.Policy = redisengine.PolicyAllKeysLFU
				s.LFULogFactor = 0
				s.LFUDecayTime = tt.decay
			})
			ctx := t.Context()

			fill(t, c, "hot", "cold")

			for range 30 {
				require.NoError(t, c.Get(ctx, "hot").Err())
			}

			advance(time.Hour)

			for range 3 {
				require.NoError(t, c.Get(ctx, "cold").Err())
			}

			fill(t, c, "new")

			for range 40 {
				require.NoError(t, c.Get(ctx, "new").Err())
			}

			big := strings.Repeat("b", 400)
			require.NoError(t, c.Set(ctx, "new", big, 0).Err())
			require.NoError(t, c.Set(ctx, "new", big, 0).Err())

			assert.Zero(t, c.Exists(ctx, tt.wantEvict).Val(), tt.wantEvict)
		})
	}
}

func TestTune_IdleTimeout(t *testing.T) {
	t.Parallel()

	e, _, advance := newClocked(t, func(s *redisengine.Settings) { s.Timeout = 120 * time.Second })

	conn, err := net.Dial("tcp", e.Addr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	rd := bufio.NewReader(conn)
	ping := func() (string, error) {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		if _, werr := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); werr != nil {
			return "", werr
		}

		return rd.ReadString('\n')
	}

	reply, err := ping()
	require.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", reply)

	advance(100 * time.Second)

	_, err = ping()
	require.NoError(t, err, "activity inside the timeout keeps the connection")

	advance(121 * time.Second)

	_, err = ping()
	assert.Error(t, err, "an idle connection past the timeout is closed")
}

func TestTune_AuthStillRequired(t *testing.T) {
	t.Parallel()

	e := startEngine(t)
	e.RequireAuth("s3cret")
	require.NoError(t, e.Tune(redisengine.DefaultSettings()))

	anon := dialEngine(t, e, "")
	ctx := t.Context()

	for _, err := range []error{
		anon.Set(ctx, "k", "v", 0).Err(),
		anon.Info(ctx, "memory").Err(),
		anon.SlowLogLen(ctx).Err(),
		anon.Do(ctx, "SELECT", "1").Err(),
	} {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NOAUTH")
	}

	authed := dialEngine(t, e, "s3cret")
	require.NoError(t, authed.Set(ctx, "k", "v", 0).Err())
	assert.Contains(t, authed.Info(ctx, "memory").Val(), "used_memory:")
}

func TestTune_MemoryFollowsDeletesAndImports(t *testing.T) {
	t.Parallel()

	e, c := tunedEngine(t, func(s *redisengine.Settings) { s.MaxMemory = 1000 })
	ctx := t.Context()

	fill(t, c, "a", "b", "c")
	require.Error(t, c.Set(ctx, "d", "v", 0).Err(), "over the limit")
	require.NoError(t, c.Del(ctx, "a", "b").Err())
	require.NoError(t, c.Set(ctx, "d", "v", 0).Err(), "deleting frees room")

	src := startEngine(t)
	require.NoError(t, src.Mini.Set("big", strings.Repeat("z", 4000)))

	blob, err := src.ExportRDB()
	require.NoError(t, err)
	require.NoError(t, e.ImportRDB(blob))

	assert.Error(t, c.Set(ctx, "e", "v", 0).Err(), "an imported dataset over the limit blocks writes")
}

func TestTune_StreamNodeLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		entries     int
		nodeBytes   int
		nodeEntries int
		want        int
	}{
		{name: "defaults", entries: 250, nodeBytes: 4096, nodeEntries: 100, want: 3},
		{name: "entries only", entries: 10, nodeBytes: 0, nodeEntries: 4, want: 3},
		{name: "unlimited", entries: 500, nodeBytes: 0, nodeEntries: 0, want: 1},
		{name: "bytes limit", entries: 10, nodeBytes: 40, nodeEntries: 0, want: 4},
		{name: "empty", entries: 0, nodeBytes: 4096, nodeEntries: 100, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, redisengine.StreamNodeCount(tt.entries, tt.nodeBytes, tt.nodeEntries))
		})
	}
}

func TestTune_StreamLimitsShapeExport(t *testing.T) {
	t.Parallel()

	sizes := map[int]int{}

	for _, limit := range []int{1, 1000} {
		e, _ := tunedEngine(t, func(s *redisengine.Settings) { s.StreamNodeMaxEntries = limit })

		for range 200 {
			_, err := e.Mini.XAdd("s", "", []string{"f", "v"})
			require.NoError(t, err)
		}

		blob, err := e.ExportRDB()
		require.NoError(t, err)

		sizes[limit] = len(blob)

		again := startEngine(t)
		require.NoError(t, again.ImportRDB(blob))
		assert.EqualValues(t, 200, dialEngine(t, again, "").XLen(t.Context(), "s").Val())
	}

	assert.Greater(t, sizes[1], sizes[1000], "one entry per node costs a listpack header each")
}

func TestTune_LuaScripts(t *testing.T) {
	t.Parallel()

	_, c := tunedEngine(t, func(s *redisengine.Settings) {
		s.MaxMemory = 1000
		s.NotifyKeyspaceEvents = "KEA"
	})

	done := make(chan error, 1)

	go func() {
		script := `redis.call("SET", KEYS[1], string.rep("x", 3000)); return redis.call("STRLEN", KEYS[1])`
		done <- c.Eval(t.Context(), script, []string{"big"}).Err()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "script deadlocked against the command hook")
	}

	err := c.Set(t.Context(), "next", "v", 0).Err()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OOM command not allowed")
}
