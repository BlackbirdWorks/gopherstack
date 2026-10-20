package redisengine_test

import (
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func TestEngine_StartClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		withAlloc bool
	}{
		{name: "ephemeral"},
		{name: "allocator", withAlloc: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var alloc *portalloc.Allocator
			if tt.withAlloc {
				alloc = portalloctest.New(t, 10)
			}

			e, err := redisengine.Start(alloc, "test")
			require.NoError(t, err)

			conn, err := net.DialTimeout("tcp", e.Addr(), time.Second)
			require.NoError(t, err)
			require.NoError(t, conn.Close())

			addr := e.Addr()
			e.Close()
			e.Close()

			_, err = net.DialTimeout("tcp", addr, time.Second)
			assert.Error(t, err)
		})
	}
}

func TestEngine_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed func(mr *miniredis.Miniredis)
		want func(t *testing.T, mr *miniredis.Miniredis)
		name string
	}{
		{
			name: "string_with_ttl",
			seed: func(mr *miniredis.Miniredis) {
				require.NoError(t, mr.Set("s", "v\x00\xff"))
				mr.SetTTL("s", 90*time.Second)
			},
			want: func(t *testing.T, mr *miniredis.Miniredis) {
				t.Helper()
				v, err := mr.Get("s")
				require.NoError(t, err)
				assert.Equal(t, "v\x00\xff", v)
				assert.Equal(t, 90*time.Second, mr.TTL("s"))
			},
		},
		{
			name: "hash_list_set_zset_stream",
			seed: func(mr *miniredis.Miniredis) {
				mr.HSet("h", "a", "1", "b", "2")
				_, _ = mr.Push("l", "x", "y", "z")
				_, _ = mr.SetAdd("st", "m1", "m2")
				_, _ = mr.ZAdd("z", 1.5, "one")
				_, _ = mr.ZAdd("z", 2.5, "two")
				_, _ = mr.XAdd("x", "1-1", []string{"f", "v"})
				mr.SetTTL("l", time.Hour)
			},
			want: func(t *testing.T, mr *miniredis.Miniredis) {
				t.Helper()
				assert.Equal(t, "2", mr.HGet("h", "b"))
				l, _ := mr.List("l")
				assert.Equal(t, []string{"x", "y", "z"}, l)
				assert.True(t, mr.Exists("st"))
				zs, _ := mr.SortedSet("z")
				assert.InDelta(t, 2.5, zs["two"], 0.001)
				ents, _ := mr.Stream("x")
				require.Len(t, ents, 1)
				assert.Equal(t, "1-1", ents[0].ID)
				assert.Equal(t, time.Hour, mr.TTL("l"))
			},
		},
		{
			name: "other_db",
			seed: func(mr *miniredis.Miniredis) { require.NoError(t, mr.DB(3).Set("k", "d3")) },
			want: func(t *testing.T, mr *miniredis.Miniredis) {
				t.Helper()
				v, err := mr.DB(3).Get("k")
				require.NoError(t, err)
				assert.Equal(t, "d3", v)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, err := redisengine.Start(nil, "snap-src")
			require.NoError(t, err)
			t.Cleanup(src.Close)

			dst, err := redisengine.Start(nil, "snap-dst")
			require.NoError(t, err)
			t.Cleanup(dst.Close)

			tt.seed(src.Mini)

			blob, err := src.Snapshot()
			require.NoError(t, err)
			require.NotEmpty(t, blob)
			require.NoError(t, dst.Restore(blob))
			tt.want(t, dst.Mini)
		})
	}
}

func TestEngine_SnapshotEmptyAndBad(t *testing.T) {
	t.Parallel()

	e, err := redisengine.Start(nil, "snap-empty")
	require.NoError(t, err)
	t.Cleanup(e.Close)

	blob, err := e.Snapshot()
	require.NoError(t, err)
	assert.Nil(t, blob)
	require.NoError(t, e.Restore(nil))
	require.ErrorIs(t, e.Restore([]byte("junk")), redisengine.ErrBadSnapshot)
}
