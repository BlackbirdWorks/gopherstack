// Package httputils provides reusable HTTP utility components.
package httputils

import (
	"bytes"
	"crypto/md5" //nolint:gosec // MD5 is required for AWS S3 Content-MD5 and ETag compatibility.
	"crypto/sha256"
	"hash"
	"hash/crc32"
	"sync"
)

const maxPooledBufferSize = 64 * 1024 // 64 KiB

var bufferPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	New: func() any {
		return new(bytes.Buffer)
	},
}

// GetBuffer acquires a clean *bytes.Buffer from the pool.
func GetBuffer() *bytes.Buffer {
	buf, ok := bufferPool.Get().(*bytes.Buffer)
	if !ok {
		return new(bytes.Buffer)
	}
	buf.Reset()

	return buf
}

// PutBuffer returns a *bytes.Buffer to the pool.
// Buffers larger than maxPooledBufferSize are discarded to bound memory retention.
func PutBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxPooledBufferSize {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

var crc32Pool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	New: func() any {
		return crc32.NewIEEE()
	},
}

// GetCRC32 retrieves a pooled CRC32 IEEE hasher with state reset.
func GetCRC32() hash.Hash32 {
	h, ok := crc32Pool.Get().(hash.Hash32)
	if !ok {
		return crc32.NewIEEE()
	}
	h.Reset()

	return h
}

// PutCRC32 returns a CRC32 IEEE hasher to the pool.
func PutCRC32(h hash.Hash32) {
	if h != nil {
		h.Reset()
		crc32Pool.Put(h)
	}
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // read-only lookup table

var crc32cPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	New: func() any {
		return crc32.New(crc32cTable)
	},
}

// GetCRC32C retrieves a pooled CRC32C (Castagnoli) hasher with state reset.
func GetCRC32C() hash.Hash32 {
	h, ok := crc32cPool.Get().(hash.Hash32)
	if !ok {
		return crc32.New(crc32cTable)
	}
	h.Reset()

	return h
}

// PutCRC32C returns a CRC32C hasher to the pool.
func PutCRC32C(h hash.Hash32) {
	if h != nil {
		h.Reset()
		crc32cPool.Put(h)
	}
}

var sha256Pool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	New: func() any {
		return sha256.New()
	},
}

// GetSHA256 retrieves a pooled SHA256 hasher with state reset.
func GetSHA256() hash.Hash {
	h, ok := sha256Pool.Get().(hash.Hash)
	if !ok {
		return sha256.New()
	}
	h.Reset()

	return h
}

// PutSHA256 returns a SHA256 hasher to the pool.
func PutSHA256(h hash.Hash) {
	if h != nil {
		h.Reset()
		sha256Pool.Put(h)
	}
}

var md5Pool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	New: func() any {
		return md5.New() //nolint:gosec // MD5 is required for S3 Content-MD5 and ETag compatibility.
	},
}

// GetMD5 retrieves a pooled MD5 hasher with state reset.
func GetMD5() hash.Hash {
	h, ok := md5Pool.Get().(hash.Hash)
	if !ok {
		return md5.New() //nolint:gosec // MD5 is required for S3 Content-MD5 and ETag compatibility.
	}
	h.Reset()

	return h
}

// PutMD5 returns an MD5 hasher to the pool.
func PutMD5(h hash.Hash) {
	if h != nil {
		h.Reset()
		md5Pool.Put(h)
	}
}

// FNV32a computes the 32-bit FNV-1a hash of a string with zero heap allocations.
func FNV32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)

	h := uint32(offset32)
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= prime32
	}

	return h
}

// FNV64a computes the 64-bit FNV-1a hash of a string with zero heap allocations.
func FNV64a(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	h := uint64(offset64)
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= prime64
	}

	return h
}
