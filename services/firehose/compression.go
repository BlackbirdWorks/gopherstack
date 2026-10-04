package firehose

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"math"
	"strings"

	"github.com/klauspost/compress/snappy"
)

const (
	compressionGZIP         = "GZIP"
	compressionZIP          = "ZIP"
	compressionSnappy       = "SNAPPY"
	compressionHadoopSnappy = "HADOOP_SNAPPY"

	hadoopSnappyBlockSize = 256 * 1024
)

// compressedBody is a payload encoded per an S3 CompressionFormat.
type compressedBody struct {
	extension string
	body      []byte
	gzip      bool
}

// compressBody encodes body per CompressionFormat (UNCOMPRESSED, GZIP, ZIP, Snappy, HADOOP_SNAPPY).
func compressBody(format string, body []byte) (compressedBody, error) {
	switch strings.ToUpper(format) {
	case compressionGZIP:
		out, err := gzipCompress(body)

		return compressedBody{body: out, extension: ".gz", gzip: true}, err
	case compressionZIP:
		out, err := zipCompress(body)

		return compressedBody{body: out, extension: ".zip"}, err
	case compressionSnappy:
		return compressedBody{body: snappy.Encode(nil, body), extension: ".snappy"}, nil
	case compressionHadoopSnappy:
		return compressedBody{body: hadoopSnappyEncode(body), extension: ".hsnappy"}, nil
	default:
		return compressedBody{body: body}, nil
	}
}

func zipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	w, err := zw.Create("data")
	if err != nil {
		return nil, err
	}

	if _, err = w.Write(data); err != nil {
		return nil, err
	}

	if err = zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// hadoopSnappyEncode frames Snappy blocks as Hadoop does: [raw len][compressed len][block].
func hadoopSnappyEncode(data []byte) []byte {
	var out bytes.Buffer

	for len(data) > 0 {
		n := min(len(data), hadoopSnappyBlockSize)
		block := snappy.Encode(nil, data[:n])

		out.Write(binary.BigEndian.AppendUint32(nil, uint32(n&math.MaxUint32)))
		out.Write(binary.BigEndian.AppendUint32(nil, uint32(len(block)&math.MaxUint32)))
		out.Write(block)
		data = data[n:]
	}

	return out.Bytes()
}
