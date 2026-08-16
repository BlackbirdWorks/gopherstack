package httputils_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
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

	payload := []byte("gopherstack fast hashing test payload")

	tests := []struct {
		getHasher func() any
		compute   func([]byte) []byte
		name      string
	}{
		{
			name: "crc32",
			getHasher: func() any {
				return httputils.GetCRC32()
			},
			compute: func(b []byte) []byte {
				h := crc32.NewIEEE()
				h.Write(b)

				return h.Sum(nil)
			},
		},
		{
			name: "crc32c",
			getHasher: func() any {
				return httputils.GetCRC32C()
			},
			compute: func(b []byte) []byte {
				h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
				h.Write(b)

				return h.Sum(nil)
			},
		},
		{
			name: "sha256",
			getHasher: func() any {
				return httputils.GetSHA256()
			},
			compute: func(b []byte) []byte {
				h := sha256.New()
				h.Write(b)

				return h.Sum(nil)
			},
		},
		{
			name: "md5",
			getHasher: func() any {
				return httputils.GetMD5()
			},
			compute: func(b []byte) []byte {
				h := md5.New()
				h.Write(b)

				return h.Sum(nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			expected := tt.compute(payload)

			h1 := httputils.GetCRC32()
			h1.Write(payload)
			sum1 := h1.Sum(nil)
			httputils.PutCRC32(h1)

			h2 := httputils.GetCRC32C()
			h2.Write(payload)
			sum2 := h2.Sum(nil)
			httputils.PutCRC32C(h2)

			h3 := httputils.GetSHA256()
			h3.Write(payload)
			sum3 := h3.Sum(nil)
			httputils.PutSHA256(h3)

			h4 := httputils.GetMD5()
			h4.Write(payload)
			sum4 := h4.Sum(nil)
			httputils.PutMD5(h4)

			switch tt.name {
			case "crc32":
				assert.Equal(t, expected, sum1)
			case "crc32c":
				assert.Equal(t, expected, sum2)
			case "sha256":
				assert.Equal(t, expected, sum3)
			case "md5":
				assert.Equal(t, expected, sum4)
			}
		})
	}
}
