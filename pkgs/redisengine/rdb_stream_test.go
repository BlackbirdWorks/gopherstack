package redisengine_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func dialEngine(t *testing.T, e *redisengine.Engine, password string) *redis.Client {
	t.Helper()

	c := redis.NewClient(&redis.Options{Addr: e.Addr(), Password: password, Protocol: 2})
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func startEngine(t *testing.T) *redisengine.Engine {
	t.Helper()

	e, err := redisengine.Start(nil, "test")
	require.NoError(t, err)
	t.Cleanup(e.Close)

	return e
}

func importFixture(t *testing.T, e *redisengine.Engine, name string) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	require.NoError(t, e.ImportRDB(data))
}

func assertFixtureStreams(t *testing.T, c *redis.Client) {
	t.Helper()

	ctx := t.Context()

	n, err := c.XLen(ctx, "st").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 153, n)

	first, err := c.XRangeN(ctx, "st", "-", "+", 4).Result()
	require.NoError(t, err)
	require.Len(t, first, 4)
	assert.Equal(t, "1-1", first[0].ID)
	assert.Equal(t, map[string]any{"a": "1", "b": "2"}, first[0].Values)
	assert.Equal(t, "2-0", first[1].ID)
	assert.Equal(t, map[string]any{"c": "5"}, first[1].Values)
	assert.Equal(t, "3-5", first[2].ID)
	assert.Equal(t, "10-1", first[3].ID)
	assert.Equal(t, map[string]any{"f": "v1"}, first[3].Values)

	last, err := c.XRevRangeN(ctx, "st", "+", "-", 1).Result()
	require.NoError(t, err)
	require.Len(t, last, 1)
	assert.Equal(t, "10-150", last[0].ID)

	groups, err := c.XInfoGroups(ctx, "st").Result()
	require.NoError(t, err)
	require.Len(t, groups, 2)

	byName := map[string]redis.XInfoGroup{}
	for _, g := range groups {
		byName[g.Name] = g
	}

	assert.Equal(t, int64(3), byName["g1"].Consumers)
	assert.Equal(t, int64(4), byName["g1"].Pending)
	assert.Equal(t, "10-2", byName["g1"].LastDeliveredID)
	assert.Equal(t, "10-150", byName["g2"].LastDeliveredID)

	pending, err := c.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: "st", Group: "g1", Start: "-", End: "+", Count: 10}).
		Result()
	require.NoError(t, err)
	require.Len(t, pending, 4)

	owners := map[string]string{}
	for _, p := range pending {
		owners[p.ID] = p.Consumer
		assert.EqualValues(t, 1, p.RetryCount, p.ID)
	}

	assert.Equal(t, map[string]string{"2-0": "alice", "3-5": "bob", "10-1": "bob", "10-2": "bob"}, owners)

	consumers, err := c.XInfoConsumers(ctx, "st", "g1").Result()
	require.NoError(t, err)

	names := make([]string, 0, len(consumers))
	for _, cons := range consumers {
		names = append(names, cons.Name)
	}

	assert.ElementsMatch(t, []string{"alice", "bob", "carol"}, names)

	eg, err := c.XInfoGroups(ctx, "es").Result()
	require.NoError(t, err)
	require.Len(t, eg, 1)
	assert.Equal(t, "eg", eg[0].Name)
	assert.Equal(t, "5-5", eg[0].LastDeliveredID)

	empty, err := c.XLen(ctx, "es").Result()
	require.NoError(t, err)
	assert.Zero(t, empty)

	assert.Equal(t, "v", c.Get(ctx, "plain").Val())
}

func TestRDB_ImportRealStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
	}{
		{"redis 6.2 listpacks", "redis-6.2-stream.rdb"},
		{"redis 7.0 listpacks 2", "redis-7.0-stream.rdb"},
		{"redis 7.2 listpacks 3", "redis-7.2-stream.rdb"},
		{"redis 8.0 listpacks 3", "redis-8.0-stream.rdb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := startEngine(t)
			importFixture(t, e, tt.file)
			assertFixtureStreams(t, dialEngine(t, e, ""))

			blob, err := e.ExportRDB()
			require.NoError(t, err)
			assert.Equal(t, "REDIS0011", string(blob[:9]))

			again := startEngine(t)
			require.NoError(t, again.ImportRDB(blob))
			assertFixtureStreams(t, dialEngine(t, again, ""))
		})
	}
}

func TestRDB_StreamsWithAuth(t *testing.T) {
	t.Parallel()

	src := startEngine(t)
	importFixture(t, src, "redis-7.2-stream.rdb")

	locked := startEngine(t)
	locked.RequireAuth("s3cret")
	importFixtureAs(t, locked, "redis-7.2-stream.rdb")
	assertFixtureStreams(t, dialEngine(t, locked, "s3cret"))

	blob, err := locked.ExportRDB()
	require.NoError(t, err)

	other := startEngine(t)
	require.NoError(t, other.ImportRDB(blob))
	assertFixtureStreams(t, dialEngine(t, other, ""))

	snap, err := locked.Snapshot()
	require.NoError(t, err)

	restored := startEngine(t)
	require.NoError(t, restored.Restore(snap))
	assertFixtureStreams(t, dialEngine(t, restored, ""))
}

func importFixtureAs(t *testing.T, e *redisengine.Engine, name string) {
	t.Helper()

	importFixture(t, e, name)
}

func TestRDB_ImportHashFieldTTL(t *testing.T) {
	t.Parallel()

	e := startEngine(t)
	importFixture(t, e, "redis-8.0-hashttl.rdb")

	c := dialEngine(t, e, "")
	ctx := t.Context()

	for _, tt := range []struct {
		key    string
		field  string
		wantAt int64
	}{
		{"hfe", "f1", 4102444800000},
		{"hfe", "f2", -1},
		{"hfe", "f3", 4102444805000},
		{"hfebig", "field10", 4102444800000},
		{"hfebig", "field20", 4102444801000},
		{"hfebig", "field30", -1},
	} {
		ttl, err := c.HPTTL(ctx, tt.key, tt.field).Result()
		require.NoError(t, err, tt.field)
		require.Len(t, ttl, 1)

		if tt.wantAt < 0 {
			assert.EqualValues(t, -1, ttl[0], tt.key+" "+tt.field)

			continue
		}

		assert.InDelta(t, tt.wantAt-time.Now().UnixMilli(), ttl[0], 5000, tt.key+" "+tt.field)
	}

	assert.Equal(t, "value354", c.HGet(ctx, "hfebig", "field354").Val())
	assert.EqualValues(t, 600, c.HLen(ctx, "hfebig").Val())
	assert.Zero(t, c.Exists(ctx, "gone").Val(), "a hash whose only field already expired is not created")

	blob, err := e.ExportRDB()
	require.NoError(t, err)
	assert.Equal(t, "REDIS0012", string(blob[:9]))

	again := startEngine(t)
	require.NoError(t, again.ImportRDB(blob))

	c2 := dialEngine(t, again, "")
	ttl, err := c2.HPTTL(ctx, "hfe", "f1", "f2", "f3").Result()
	require.NoError(t, err)
	assert.InDelta(t, 4102444800000-time.Now().UnixMilli(), ttl[0], 5000)
	assert.EqualValues(t, -1, ttl[1])
	assert.InDelta(t, 4102444805000-time.Now().UnixMilli(), ttl[2], 5000)
}

func TestRDB_StreamExportShapes(t *testing.T) {
	t.Parallel()

	big := make([]byte, 5000)
	for i := range big {
		big[i] = 'a' + byte(i%26)
	}

	tests := []struct {
		entries [][]string
		name    string
		ids     []string
	}{
		{
			name:    "negative seq diff",
			ids:     []string{"5-9", "6-0", "6-1"},
			entries: [][]string{{"a", "1"}, {"a", "2"}, {"a", "3"}},
		},
		{
			name:    "mixed fields",
			ids:     []string{"1-1", "1-2", "1-3"},
			entries: [][]string{{"a", "1", "b", "2"}, {"b", "2", "a", "1"}, {"c", "3"}},
		},
		{name: "integers", ids: []string{"1-1", "2-1"}, entries: [][]string{
			{"n", "-1", "m", "300", "o", "70000"}, {"n", "3000000000", "m", "-9223372036854775808", "o", "00123"},
		}},
		{name: "large value", ids: []string{"7-7"}, entries: [][]string{{"blob", string(big), "short", "x"}}},
		{name: "empty value", ids: []string{"9-0"}, entries: [][]string{{"k", ""}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := startEngine(t)

			for i, id := range tt.ids {
				_, err := src.Mini.XAdd("s", id, tt.entries[i])
				require.NoError(t, err)
			}

			blob, err := src.ExportRDB()
			require.NoError(t, err)

			dst := startEngine(t)
			require.NoError(t, dst.ImportRDB(blob))

			want, err := dialEngine(t, src, "").XRange(t.Context(), "s", "-", "+").Result()
			require.NoError(t, err)

			got, err := dialEngine(t, dst, "").XRange(t.Context(), "s", "-", "+").Result()
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestRDB_StreamExportManyNodes(t *testing.T) {
	t.Parallel()

	src := startEngine(t)

	for i := range 450 {
		_, err := src.Mini.XAdd("s", "", []string{"i", string(rune('a' + i%26)), "n", "1"})
		require.NoError(t, err)
	}

	blob, err := src.ExportRDB()
	require.NoError(t, err)

	dst := startEngine(t)
	require.NoError(t, dst.ImportRDB(blob))

	want, err := dialEngine(t, src, "").XRange(t.Context(), "s", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, want, 450)

	got, err := dialEngine(t, dst, "").XRange(t.Context(), "s", "-", "+").Result()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
