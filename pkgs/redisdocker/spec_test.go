package redisdocker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

func TestParseSlots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    []redisdocker.SlotRange
		wantErr bool
	}{
		{name: "range", in: "0-5461", want: []redisdocker.SlotRange{{Start: 0, End: 5461}}},
		{name: "list", in: "0-100,200-300 500", want: []redisdocker.SlotRange{{0, 100}, {200, 300}, {500, 500}}},
		{name: "empty", in: "", want: []redisdocker.SlotRange{}},
		{name: "reversed", in: "10-5", wantErr: true},
		{name: "too big", in: "0-16384", wantErr: true},
		{name: "junk", in: "a-b", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := redisdocker.ParseSlots(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, redisdocker.ErrSlots)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEvenSlotsCoverAllSlots(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 3, 5, 7, 500} {
		require.NoError(t, redisdocker.ValidateSlotMap(redisdocker.EvenSlots(n)), "shards=%d", n)
	}

	assert.Equal(t, [][]redisdocker.SlotRange{{{Start: 0, End: 8191}}, {{Start: 8192, End: 16383}}},
		redisdocker.EvenSlots(2))
}

func TestValidateSlotMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		shards [][]redisdocker.SlotRange
		ok     bool
	}{
		{name: "full split", shards: [][]redisdocker.SlotRange{{{0, 99}}, {{100, 16383}}}, ok: true},
		{name: "interleaved", shards: [][]redisdocker.SlotRange{{{0, 99}, {200, 16383}}, {{100, 199}}}, ok: true},
		{name: "gap", shards: [][]redisdocker.SlotRange{{{0, 99}}, {{101, 16383}}}},
		{name: "overlap", shards: [][]redisdocker.SlotRange{{{0, 100}}, {{100, 16383}}}},
		{name: "short", shards: [][]redisdocker.SlotRange{{{0, 16382}}}},
		{name: "none", shards: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := redisdocker.ValidateSlotMap(tc.shards)
			if tc.ok {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, redisdocker.ErrSlots)
		})
	}
}

func TestImageFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine, version, image, binary string
	}{
		{"redis", "7.1", "redis:7.2-alpine", "redis-server"},
		{"redis", "7.0", "redis:7.0-alpine", "redis-server"},
		{"redis", "6.2", "redis:7.0-alpine", "redis-server"},
		{"redis", "7.2.4", "redis:7.2-alpine", "redis-server"},
		{"redis", "", "redis:7.0-alpine", "redis-server"},
		{"valkey", "8.0", "valkey/valkey:8.0-alpine", "valkey-server"},
		{"valkey", "", "valkey/valkey:8.0-alpine", "valkey-server"},
	}

	for _, tc := range tests {
		t.Run(tc.engine+"-"+tc.version, func(t *testing.T) {
			t.Parallel()

			image, binary := redisdocker.ImageFor(tc.engine, tc.version)
			assert.Equal(t, tc.image, image)
			assert.Equal(t, tc.binary, binary)
		})
	}
}

func TestBundleRoundTrip(t *testing.T) {
	t.Parallel()

	in := [][]byte{[]byte("REDIS0011a"), {}, []byte("REDIS0011bcd")}
	got, ok := redisdocker.DecodeBundle(redisdocker.EncodeBundle(in))
	require.True(t, ok)
	assert.Equal(t, in, got)

	_, ok = redisdocker.DecodeBundle([]byte("REDIS0011"))
	assert.False(t, ok)

	trunc := redisdocker.EncodeBundle(in)
	_, ok = redisdocker.DecodeBundle(trunc[:len(trunc)-3])
	assert.False(t, ok)
}

func TestSeedsFor(t *testing.T) {
	t.Parallel()

	rdb := []byte("REDIS0011xyz")

	tests := []struct {
		name    string
		blob    []byte
		want    [][]byte
		shards  int
		wantErr bool
	}{
		{name: "empty", blob: nil, shards: 2, want: [][]byte{nil, nil}},
		{name: "raw single", blob: rdb, shards: 1, want: [][]byte{rdb}},
		{name: "bundle", blob: redisdocker.EncodeBundle([][]byte{rdb, rdb}), shards: 2, want: [][]byte{rdb, rdb}},
		{name: "shape mismatch", blob: redisdocker.EncodeBundle([][]byte{rdb}), shards: 2, wantErr: true},
		{name: "raw for many", blob: rdb, shards: 3, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := redisdocker.SeedsFor(tc.blob, tc.shards)
			if tc.wantErr {
				require.ErrorIs(t, err, redisdocker.ErrShape)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
