package httputils_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestBufferPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputData []byte
	}{
		{
			name:      "empty_buffer",
			inputData: []byte{},
		},
		{
			name:      "small_data",
			inputData: []byte("hello world"),
		},
		{
			name:      "large_data",
			inputData: bytes.Repeat([]byte("a"), 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := httputils.GetBuffer()
			require.NotNil(t, buf)
			assert.Equal(t, 0, buf.Len())

			n, err := buf.Write(tt.inputData)
			require.NoError(t, err)
			assert.Equal(t, len(tt.inputData), n)
			if len(tt.inputData) == 0 {
				assert.Empty(t, buf.Bytes())
			} else {
				assert.Equal(t, tt.inputData, buf.Bytes())
			}

			httputils.PutBuffer(buf)
		})
	}
}

func TestHasherPools(t *testing.T) {
	t.Parallel()

	type poolArgs struct {
		acquire func() hash.Hash
		release func(hash.Hash)
		compute func([]byte) []byte
		payload []byte
	}

	type poolWant struct {
		payloadSum []byte
		emptySum   []byte
	}

	payload := []byte("gopherstack fast hashing test payload")

	tests := []struct {
		name string
		args poolArgs
		want poolWant
	}{
		{
			name: "crc32",
			args: poolArgs{
				payload: payload,
				acquire: func() hash.Hash {
					return httputils.GetCRC32()
				},
				release: func(h hash.Hash) {
					if h32, ok := h.(hash.Hash32); ok {
						httputils.PutCRC32(h32)
					}
				},
				compute: func(b []byte) []byte {
					h := crc32.NewIEEE()
					h.Write(b)

					return h.Sum(nil)
				},
			},
			want: poolWant{
				payloadSum: func() []byte {
					h := crc32.NewIEEE()
					h.Write(payload)

					return h.Sum(nil)
				}(),
				emptySum: crc32.NewIEEE().Sum(nil),
			},
		},
		{
			name: "crc32c",
			args: poolArgs{
				payload: payload,
				acquire: func() hash.Hash {
					return httputils.GetCRC32C()
				},
				release: func(h hash.Hash) {
					if h32, ok := h.(hash.Hash32); ok {
						httputils.PutCRC32C(h32)
					}
				},
				compute: func(b []byte) []byte {
					h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
					h.Write(b)

					return h.Sum(nil)
				},
			},
			want: poolWant{
				payloadSum: func() []byte {
					h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
					h.Write(payload)

					return h.Sum(nil)
				}(),
				emptySum: crc32.New(crc32.MakeTable(crc32.Castagnoli)).Sum(nil),
			},
		},
		{
			name: "sha256",
			args: poolArgs{
				payload: payload,
				acquire: httputils.GetSHA256,
				release: func(h hash.Hash) {
					httputils.PutSHA256(h)
				},
				compute: func(b []byte) []byte {
					h := sha256.New()
					h.Write(b)

					return h.Sum(nil)
				},
			},
			want: poolWant{
				payloadSum: func() []byte {
					h := sha256.New()
					h.Write(payload)

					return h.Sum(nil)
				}(),
				emptySum: sha256.New().Sum(nil),
			},
		},
		{
			name: "md5",
			args: poolArgs{
				payload: payload,
				acquire: httputils.GetMD5,
				release: func(h hash.Hash) {
					httputils.PutMD5(h)
				},
				compute: func(b []byte) []byte {
					h := md5.New()
					h.Write(b)

					return h.Sum(nil)
				},
			},
			want: poolWant{
				payloadSum: func() []byte {
					h := md5.New()
					h.Write(payload)

					return h.Sum(nil)
				}(),
				emptySum: md5.New().Sum(nil),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := tt.args.acquire()
			require.NotNil(t, h)
			_, err := h.Write(tt.args.payload)
			require.NoError(t, err)
			assert.Equal(t, tt.want.payloadSum, h.Sum(nil))

			tt.args.release(h)

			h2 := tt.args.acquire()
			require.NotNil(t, h2)
			assert.Equal(t, tt.want.emptySum, h2.Sum(nil))
			tt.args.release(h2)
		})
	}
}
