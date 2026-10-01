package s3

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"math"
	"sync"
)

type GzipCompressor struct{}

// gzipScratchMaxCap bounds the scratch buffer retained between calls.
const gzipScratchMaxCap = 16 * 1024 * 1024

var (
	gzipWriterPool  sync.Pool //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	gzipScratchPool sync.Pool //nolint:gochecknoglobals // sync.Pool requires package-level allocation
)

// Compress gzips data at BestSpeed. Compression is an internal storage-format
// choice (GetObject always decompresses back to the exact original bytes), so
// trading ratio for speed here is invisible to callers; DefaultCompression's
// CPU cost dominated the object-write hot path under profiling.
// The buffer is not pre-sized to len(data): output is usually much smaller.
func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {
	return c.CompressParts([][]byte{data})
}

// CompressParts gzips the concatenation of parts without materialising it.
func (c *GzipCompressor) CompressParts(parts [][]byte) ([]byte, error) {
	w, ok := gzipWriterPool.Get().(*gzip.Writer)
	buf, _ := gzipScratchPool.Get().(*bytes.Buffer)
	if buf == nil {
		buf = new(bytes.Buffer)
	}

	buf.Reset()

	if ok {
		w.Reset(buf)
	} else {
		var err error
		if w, err = gzip.NewWriterLevel(buf, gzip.BestSpeed); err != nil {
			return nil, err
		}
	}

	defer gzipWriterPool.Put(w)

	for _, p := range parts {
		if _, err := w.Write(p); err != nil {
			return nil, err
		}
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	out := buf.Bytes()
	if buf.Cap() <= gzipScratchMaxCap {
		out = bytes.Clone(out)
		gzipScratchPool.Put(buf)
	}

	w.Reset(io.Discard)

	return out, nil
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

// gzipReaderState pairs a reusable gzip.Reader with its source so a pooled
// reader never pins the stored blob it last read.
type gzipReaderState struct {
	zr  *gzip.Reader
	src bytes.Reader
}

var gzipReaderPool sync.Pool //nolint:gochecknoglobals // sync.Pool requires package-level allocation

// Decompress gunzips data. Readers are pooled; each is owned by one call.
func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	st, _ := gzipReaderPool.Get().(*gzipReaderState)
	if st == nil {
		st = new(gzipReaderState)
	}

	st.src.Reset(data)

	defer func() {
		st.src.Reset(nil)
		gzipReaderPool.Put(st)
	}()

	if st.zr == nil {
		zr, err := gzip.NewReader(&st.src)
		if err != nil {
			return nil, err
		}

		st.zr = zr
	} else if err := st.zr.Reset(&st.src); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if hint := gzipISizeHint(data); hint > 0 {
		// ReadFrom reserves MinRead before its final EOF read; without it the
		// buffer doubles once at the end.
		buf.Grow(hint + bytes.MinRead)
	}
	if _, err := io.Copy(&buf, st.zr); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
