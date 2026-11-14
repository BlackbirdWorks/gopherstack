package deltalog_test

import (
	"encoding/binary"
	"hash/crc32"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/deltalog"
)

const z85Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

func z85Encode(b []byte) string {
	out := make([]byte, 0, len(b)/4*5)

	for i := 0; i < len(b); i += 4 {
		v := binary.BigEndian.Uint32(b[i:])

		var g [5]byte

		for j := 4; j >= 0; j-- {
			g[j] = z85Chars[v%85]
			v /= 85
		}

		out = append(out, g[:]...)
	}

	return string(out)
}

func rows(m map[int64]bool) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	slices.Sort(out)

	return out
}

func arrayContainer(vals ...uint16) []byte {
	var b []byte

	b = binary.LittleEndian.AppendUint32(b, 12346)
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint16(b, 0)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(vals)-1))
	b = binary.LittleEndian.AppendUint32(b, 16)

	for _, v := range vals {
		b = binary.LittleEndian.AppendUint16(b, v)
	}

	return b
}

func portable(bitmaps map[uint32][]byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, 1681511377)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(bitmaps)))

	keys := make([]uint32, 0, len(bitmaps))
	for k := range bitmaps {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	for _, k := range keys {
		b = binary.LittleEndian.AppendUint32(b, k)
		b = append(b, bitmaps[k]...)
	}

	return b
}

func TestDeletionVector_SpecInlineExample(t *testing.T) {
	t.Parallel()

	dv := &deltalog.DeletionVector{
		StorageType:    "i",
		PathOrInlineDv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
		SizeInBytes:    40, Cardinality: 6,
	}

	got, err := dv.ParseInline()
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 4, 7, 11, 18, 29}, rows(got))
}

func TestDeletionVector_FilePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, storage, in, want string
		abs                     bool
	}{
		{
			"relative uuid with prefix", "u", "ab^-aqEH.-t@S}K{vb[*k^",
			"ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", false,
		},
		{
			"relative uuid no prefix", "u", "^-aqEH.-t@S}K{vb[*k^",
			"deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", false,
		},
		{"absolute", "p", "s3://mytable/dv.bin", "s3://mytable/dv.bin", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p, abs, err := (&deltalog.DeletionVector{StorageType: tc.storage, PathOrInlineDv: tc.in}).FilePath()
			require.NoError(t, err)
			assert.Equal(t, tc.want, p)
			assert.Equal(t, tc.abs, abs)
		})
	}
}

func TestDeletionVector_Formats(t *testing.T) {
	t.Parallel()

	big := make([]byte, 0, 8192)
	big = binary.LittleEndian.AppendUint32(big, 12346)
	big = binary.LittleEndian.AppendUint32(big, 1)
	big = binary.LittleEndian.AppendUint16(big, 0)
	big = binary.LittleEndian.AppendUint16(big, 4999)
	big = binary.LittleEndian.AppendUint32(big, 16)
	words := make([]byte, 8192)

	for v := range 5000 {
		words[v/8] |= 1 << (v % 8)
	}

	big = append(big, words...)

	var run []byte

	run = binary.LittleEndian.AppendUint32(run, 12347)
	run = append(run, 0x01)

	for _, v := range []uint16{0, 4, 2, 7, 2, 10, 1} {
		run = binary.LittleEndian.AppendUint16(run, v)
	}

	cases := []struct {
		name string
		data []byte
		want []int64
	}{
		{"array", portable(map[uint32][]byte{0: arrayContainer(1, 5, 9)}), []int64{1, 5, 9}},
		{"high keys", portable(map[uint32][]byte{0: arrayContainer(2), 1: arrayContainer(3)}), []int64{2, 1<<32 | 3}},
		{"run container", portable(map[uint32][]byte{0: run}), []int64{7, 8, 9, 10, 11}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			framed := []byte{1}
			framed = binary.BigEndian.AppendUint32(framed, uint32(len(tc.data)))
			framed = append(framed, tc.data...)
			framed = binary.BigEndian.AppendUint32(framed, crc32.ChecksumIEEE(tc.data))

			off := int64(1)
			dv := &deltalog.DeletionVector{
				StorageType: "u", Offset: &off, SizeInBytes: int64(len(tc.data)), Cardinality: int64(len(tc.want)),
			}

			got, err := dv.ParseFileDV(framed)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rows(got))
		})
	}

	t.Run("bitmap container", func(t *testing.T) {
		t.Parallel()

		data := portable(map[uint32][]byte{0: big})
		framed := binary.BigEndian.AppendUint32([]byte{1}, uint32(len(data)))
		framed = append(framed, data...)
		framed = binary.BigEndian.AppendUint32(framed, crc32.ChecksumIEEE(data))

		off := int64(1)
		dv := &deltalog.DeletionVector{StorageType: "u", Offset: &off, SizeInBytes: int64(len(data)), Cardinality: 5000}

		got, err := dv.ParseFileDV(framed)
		require.NoError(t, err)
		assert.Len(t, got, 5000)
		assert.True(t, got[4999])
		assert.False(t, got[5000])
	})
}

func TestDeletionVector_Errors(t *testing.T) {
	t.Parallel()

	data := portable(map[uint32][]byte{0: arrayContainer(1, 2)})
	good := binary.BigEndian.AppendUint32([]byte{1}, uint32(len(data)))
	good = append(good, data...)
	good = binary.BigEndian.AppendUint32(good, crc32.ChecksumIEEE(data))

	off := int64(1)
	size := int64(len(data))

	badSum := append(slices.Clone(good[:len(good)-1]), 0xFF)

	cases := []struct {
		name string
		file []byte
		dv   deltalog.DeletionVector
	}{
		{"size mismatch", good, deltalog.DeletionVector{Offset: &off, SizeInBytes: size + 1, Cardinality: 2}},
		{"cardinality mismatch", good, deltalog.DeletionVector{Offset: &off, SizeInBytes: size, Cardinality: 3}},
		{"offset outside", good, deltalog.DeletionVector{Offset: new(int64(9999)), SizeInBytes: size, Cardinality: 2}},
		{"bad checksum", badSum, deltalog.DeletionVector{Offset: &off, SizeInBytes: size, Cardinality: 2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tc.dv.ParseFileDV(tc.file)
			require.ErrorIs(t, err, deltalog.ErrBadDeletionVector)
		})
	}
}

func TestDeletionVector_InlineRoundTrip(t *testing.T) {
	t.Parallel()

	data := portable(map[uint32][]byte{0: arrayContainer(1, 5, 9, 12)})
	require.Zero(t, len(data)%4)

	dv := &deltalog.DeletionVector{
		StorageType: "i", PathOrInlineDv: z85Encode(data), SizeInBytes: int64(len(data)), Cardinality: 4,
	}

	got, err := dv.ParseInline()
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 5, 9, 12}, rows(got))
}
