package s3

import (
	"bytes"
	"compress/gzip"
	"io"
)

type GzipCompressor struct{}

// Compress gzips data at BestSpeed. Compression is an internal storage-format
// choice (GetObject always decompresses back to the exact original bytes), so
// trading ratio for speed here is invisible to callers; DefaultCompression's
// CPU cost dominated the object-write hot path under profiling.
func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(data))
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

func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}
