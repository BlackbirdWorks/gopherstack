package s3

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"math"
)

type GzipCompressor struct{}

// Compress gzips data at BestSpeed. Compression is an internal storage-format
// choice (GetObject always decompresses back to the exact original bytes), so
// trading ratio for speed here is invisible to callers; DefaultCompression's
// CPU cost dominated the object-write hot path under profiling.
// The buffer is not pre-sized to len(data): output is usually much smaller.
func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = w.Write(data); err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// gzipTrailerMinLen is the smallest a valid gzip stream can be: a 10-byte
// header plus an 8-byte trailer (CRC32 + ISIZE).
const gzipTrailerMinLen = 18

// gzipISizeHint reads the trailer's ISIZE (uncompressed size mod 2^32, RFC 1952
// §2.3.1) as a pre-size hint; a wrong value only costs extra growth.
func gzipISizeHint(data []byte) int {
	if len(data) < gzipTrailerMinLen {
		return 0
	}

	isize := binary.LittleEndian.Uint32(data[len(data)-4:])
	if isize > math.MaxInt32 {
		return 0
	}

	return int(isize)
}

func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if hint := gzipISizeHint(data); hint > 0 {
		// ReadFrom reserves MinRead before its final EOF read; without it the
		// buffer doubles once at the end.
		buf.Grow(hint + bytes.MinRead)
	}
	//nolint:gosec // G110: decompresses our own previously Compress'd bytes, not attacker-supplied gzip
	if _, err = io.Copy(&buf, r); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
