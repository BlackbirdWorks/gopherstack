package redisengine_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func TestRDB_Checksum(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint64(0xe9c6d914c4b8d9ca), redisengine.CRC64([]byte("123456789")))
}

func TestRDB_LZF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		in      []byte
		ulen    int
		wantErr bool
	}{
		{name: "literal", in: []byte{0x02, 'a', 'b', 'c'}, ulen: 3, want: "abc"},
		{name: "backref", in: []byte{0x00, 'a', 0xE0, 0x00, 0x00}, ulen: 10, want: "aaaaaaaaaa"},
		{name: "wrong-length", in: []byte{0x00, 'a'}, ulen: 3, wantErr: true},
		{name: "bad-ref", in: []byte{0x00, 'a', 0x20, 0x05}, ulen: 4, wantErr: true},
		{name: "truncated", in: []byte{0x05, 'a'}, ulen: 6, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := redisengine.LZFDecompress(tt.in, tt.ulen)
			if tt.wantErr {
				require.ErrorIs(t, err, redisengine.ErrBadRDB)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func seedAllTypes(t *testing.T, mr *miniredis.Miniredis) {
	t.Helper()

	require.NoError(t, mr.Set("s", "hello"))
	require.NoError(t, mr.Set("big", string(make([]byte, 20000))))
	mr.HSet("h", "f1", "v1")
	mr.HSet("h", "f2", "v2")
	_, err := mr.Push("l", "a", "b", "c")
	require.NoError(t, err)
	_, err = mr.SetAdd("set", "x", "y")
	require.NoError(t, err)
	_, err = mr.ZAdd("z", 1.5, "m1")
	require.NoError(t, err)
	_, err = mr.ZAdd("z", -2, "m2")
	require.NoError(t, err)
	require.NoError(t, mr.DB(3).Set("other", "db3"))
	require.NoError(t, mr.Set("ttl", "v"))
	mr.SetTTL("ttl", 3600e9)
}

func TestRDB_RoundTrip(t *testing.T) {
	t.Parallel()

	src, err := redisengine.Start(nil, "src")
	require.NoError(t, err)
	t.Cleanup(src.Close)
	seedAllTypes(t, src.Mini)

	blob, err := src.ExportRDB()
	require.NoError(t, err)
	assert.Equal(t, "REDIS0009", string(blob[:9]))

	dst, err := redisengine.Start(nil, "dst")
	require.NoError(t, err)
	t.Cleanup(dst.Close)
	require.NoError(t, dst.Mini.Set("stale", "gone"))
	require.NoError(t, dst.ImportRDB(blob))

	assert.False(t, dst.Mini.Exists("stale"))

	got, _ := dst.Mini.Get("s")
	assert.Equal(t, "hello", got)
	big, _ := dst.Mini.Get("big")
	assert.Len(t, big, 20000)
	assert.Equal(t, "v2", dst.Mini.HGet("h", "f2"))

	list, _ := dst.Mini.List("l")
	assert.Equal(t, []string{"a", "b", "c"}, list)

	members, _ := dst.Mini.Members("set")
	assert.ElementsMatch(t, []string{"x", "y"}, members)

	score, _ := dst.Mini.ZScore("z", "m2")
	assert.InDelta(t, -2, score, 0)

	other, _ := dst.Mini.DB(3).Get("other")
	assert.Equal(t, "db3", other)
	assert.Positive(t, dst.Mini.TTL("ttl"))
}

func TestRDB_Errors(t *testing.T) {
	t.Parallel()

	src, err := redisengine.Start(nil, "src")
	require.NoError(t, err)
	t.Cleanup(src.Close)
	require.NoError(t, src.Mini.Set("k", "v"))

	valid, err := src.ExportRDB()
	require.NoError(t, err)

	tests := []struct {
		want error
		name string
		data []byte
	}{
		{name: "empty", data: nil, want: redisengine.ErrBadRDB},
		{name: "not-rdb", data: []byte("hello world"), want: redisengine.ErrBadRDB},
		{name: "future-version", data: []byte("REDIS0099\xff"), want: redisengine.ErrUnsupportedRDB},
		{name: "truncated", data: valid[:len(valid)-12], want: redisengine.ErrBadRDB},
		{name: "listpack-hash", data: []byte("REDIS0009\x00\x00\x00"), want: redisengine.ErrUnsupportedRDB},
		{name: "huge-count", data: []byte("REDIS0009\x01\x01k\x80\xff\xff\xff\xff"), want: redisengine.ErrBadRDB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dst, startErr := redisengine.Start(nil, "dst")
			require.NoError(t, startErr)
			t.Cleanup(dst.Close)
			require.NoError(t, dst.Mini.Set("keep", "me"))

			if tt.name == "listpack-hash" {
				tt.data = []byte("REDIS0009\x10\x01k\x00")
			}

			require.ErrorIs(t, dst.ImportRDB(tt.data), tt.want)
			assert.True(t, dst.Mini.Exists("keep"), "keyspace untouched on error")
		})
	}
}

func TestRDB_ExportStreamRejected(t *testing.T) {
	t.Parallel()

	e, err := redisengine.Start(nil, "stream")
	require.NoError(t, err)
	t.Cleanup(e.Close)

	_, err = e.Mini.XAdd("st", "*", []string{"a", "b"})
	require.NoError(t, err)

	_, err = e.ExportRDB()
	require.ErrorIs(t, err, redisengine.ErrUnsupportedRDB)
}
