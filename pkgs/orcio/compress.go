package orcio

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	chunkHeaderLen = 3
	chunkOriginal  = 1
)

// decompress expands an ORC compressed stream: chunks with 3-byte headers, or raw bytes for codec NONE.
// Each chunk may inflate to at most blockSize bytes and the stream to at most maxStreamBytes.
func decompress(codec int, blockSize uint64, in []byte) ([]byte, error) {
	if codec == compNone {
		return in, nil
	}

	if blockSize == 0 || blockSize > maxBlockSize {
		return nil, fmt.Errorf("%w: compression block size %d", ErrBadFile, blockSize)
	}

	var out []byte

	for len(in) > 0 {
		if len(in) < chunkHeaderLen {
			return nil, fmt.Errorf("%w: truncated compression chunk header", ErrBadFile)
		}

		h := uint32(in[0]) | uint32(in[1])<<shiftByte1 | uint32(in[2])<<shiftByte2
		n := int(h >> 1)
		in = in[chunkHeaderLen:]

		if n > len(in) {
			return nil, fmt.Errorf("%w: compression chunk overruns stream", ErrBadFile)
		}

		chunk := in[:n]
		in = in[n:]

		dec := chunk
		if h&chunkOriginal != chunkOriginal {
			var err error
			if dec, err = inflateChunk(codec, blockSize, chunk); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrBadFile, err)
			}
		}

		if u64(len(dec)) > blockSize || len(out)+len(dec) > maxStreamBytes {
			return nil, fmt.Errorf("%w: compressed chunk exceeds the block size", ErrBadFile)
		}

		out = append(out, dec...)
	}

	return out, nil
}

func inflateChunk(codec int, blockSize uint64, chunk []byte) ([]byte, error) {
	switch codec {
	case compZlib:
		return io.ReadAll(io.LimitReader(flate.NewReader(bytes.NewReader(chunk)), capInt64(blockSize)+1))
	case compSnappy:
		n, err := snappy.DecodedLen(chunk)
		if err != nil || u64(n) > blockSize {
			return nil, fmt.Errorf("%w: snappy chunk length", ErrBadFile)
		}

		return snappy.Decode(nil, chunk)
	case compLZ4:
		dst := make([]byte, blockSize)

		n, err := lz4.UncompressBlock(chunk, dst)
		if err != nil {
			return nil, err
		}

		return dst[:n], nil
	case compZstd:
		dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(blockSize), zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}

		defer dec.Close()

		return dec.DecodeAll(chunk, nil)
	default:
		return nil, fmt.Errorf("%w: compression kind %d", ErrUnsupported, codec)
	}
}

func codecByName(name string) (int, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "ZLIB":
		return compZlib, nil
	case "SNAPPY":
		return compSnappy, nil
	case "LZ4":
		return compLZ4, nil
	case "ZSTD":
		return compZstd, nil
	case "NONE", "UNCOMPRESSED":
		return compNone, nil
	default:
		return 0, fmt.Errorf("%w: compression %q", ErrUnsupported, name)
	}
}

// compress frames raw into ORC chunks of at most blockSize bytes.
func compress(codec int, blockSize int, raw []byte) ([]byte, error) {
	if codec == compNone {
		return raw, nil
	}

	var out []byte

	for len(raw) > 0 {
		n := min(blockSize, len(raw))
		chunk := raw[:n]
		raw = raw[n:]

		enc, err := deflateChunk(codec, chunk)
		if err != nil {
			return nil, err
		}

		body, orig := enc, uint64(0)
		if len(enc) >= len(chunk) {
			body, orig = chunk, chunkOriginal
		}

		h := u64(len(body))<<1 | orig
		out = append(out, lowByte(h), lowByte(h>>shiftByte1), lowByte(h>>shiftByte2))
		out = append(out, body...)
	}

	return out, nil
}

func deflateChunk(codec int, chunk []byte) ([]byte, error) {
	switch codec {
	case compZlib:
		var buf bytes.Buffer

		w, err := flate.NewWriter(&buf, flate.DefaultCompression)
		if err != nil {
			return nil, err
		}

		if _, err = w.Write(chunk); err != nil {
			return nil, err
		}

		if err = w.Close(); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	case compSnappy:
		return snappy.Encode(nil, chunk), nil
	case compLZ4:
		dst := make([]byte, lz4.CompressBlockBound(len(chunk)))

		var c lz4.Compressor

		n, err := c.CompressBlock(chunk, dst)
		if err != nil || n == 0 {
			return chunk, nil
		}

		return dst[:n], nil
	case compZstd:
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}

		defer func() { _ = enc.Close() }()

		return enc.EncodeAll(chunk, nil), nil
	default:
		return nil, fmt.Errorf("%w: compression kind %d", ErrUnsupported, codec)
	}
}
