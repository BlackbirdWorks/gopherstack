package s3

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	zstdWindowBytes = 4 << 20
	// zstdMaxObject caps decoded size; larger bodies are stored raw so a stored
	// blob can always be decoded under WithDecoderMaxMemory.
	zstdMaxObject      = 1 << 30
	zstdMaxDecodeWin   = 2 * zstdWindowBytes
	zstdScratchMaxCap  = 16 * 1024 * 1024
	zstdSampleBytes    = 4 << 10
	zstdSampleMinTotal = 64 << 10
	// zstdSampleRatioPct is the sample output/input percentage above which a
	// body is treated as incompressible.
	zstdSampleRatioPct = 95
	zstdPercent        = 100
	zstdMagic          = "\x28\xb5\x2f\xfd"
)

var (
	errZstdTooLarge = errors.New("s3: object too large for zstd storage")
)

// ZstdCompressor stores objects zstd-compressed and decodes both zstd and
// legacy gzip blobs, detected by magic bytes. The zero value is ready to use.
type ZstdCompressor struct {
	streams sync.Pool
	scratch sync.Pool
	encErr  error
	decErr  error
	enc     *zstd.Encoder
	dec     *zstd.Decoder
	encOnce sync.Once
	decOnce sync.Once
}

type zstdStream struct {
	enc *zstd.Encoder
}

func (c *ZstdCompressor) encoder() (*zstd.Encoder, error) {
	c.encOnce.Do(func() {
		c.enc, c.encErr = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)),
			zstd.WithWindowSize(zstdWindowBytes),
			zstd.WithZeroFrames(true))
	})

	return c.enc, c.encErr
}

func (c *ZstdCompressor) decoder() (*zstd.Decoder, error) {
	c.decOnce.Do(func() {
		c.dec, c.decErr = zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)),
			zstd.WithDecoderMaxMemory(zstdMaxObject),
			zstd.WithDecoderMaxWindow(zstdMaxDecodeWin))
	})

	return c.dec, c.decErr
}

// Compress zstd-encodes data unconditionally.
func (c *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	return c.CompressParts([][]byte{data})
}

// CompressParts zstd-encodes the concatenation of parts without materialising it.
func (c *ZstdCompressor) CompressParts(parts [][]byte) ([]byte, error) {
	total := 0
	for _, p := range parts {
		total += len(p)
	}

	if total > zstdMaxObject {
		return nil, errZstdTooLarge
	}

	if len(parts) == 1 {
		return c.encodeAll(parts[0])
	}

	return c.encodeStream(parts, total)
}

func (c *ZstdCompressor) getScratch() []byte {
	if b, ok := c.scratch.Get().(*[]byte); ok {
		return (*b)[:0]
	}

	return nil
}

func (c *ZstdCompressor) putScratch(b []byte) {
	if cap(b) <= zstdScratchMaxCap {
		c.scratch.Put(&b)
	}
}

func (c *ZstdCompressor) encodeAll(data []byte) ([]byte, error) {
	enc, err := c.encoder()
	if err != nil {
		return nil, err
	}

	buf := enc.EncodeAll(data, c.getScratch())
	out := bytes.Clone(buf)
	c.putScratch(buf)

	return out, nil
}

func (c *ZstdCompressor) encodeStream(parts [][]byte, total int) ([]byte, error) {
	st, _ := c.streams.Get().(*zstdStream)
	if st == nil {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithWindowSize(zstdWindowBytes),
			zstd.WithLowerEncoderMem(true))
		if err != nil {
			return nil, err
		}

		st = &zstdStream{enc: enc}
	}

	sink := bytes.NewBuffer(c.getScratch())
	st.enc.ResetContentSize(sink, int64(total))

	for _, p := range parts {
		if _, err := st.enc.Write(p); err != nil {
			st.enc.Reset(io.Discard)

			return nil, err
		}
	}

	if err := st.enc.Close(); err != nil {
		st.enc.Reset(io.Discard)

		return nil, err
	}

	st.enc.Reset(io.Discard)
	c.streams.Put(st)

	out := bytes.Clone(sink.Bytes())
	c.putScratch(sink.Bytes())

	return out, nil
}

// CompressForStore returns the body to store and whether it is compressed.
// It stays raw when the data is incompressible or too large to decode safely.
func (c *ZstdCompressor) CompressForStore(parts [][]byte) ([]byte, bool, error) {
	total := 0
	for _, p := range parts {
		total += len(p)
	}

	if total > zstdMaxObject || (total >= zstdSampleMinTotal && !c.sampleCompresses(parts[0])) {
		return joinParts(parts), false, nil
	}

	out, err := c.CompressParts(parts)
	if err != nil {
		return nil, false, err
	}

	if len(out) >= total {
		return joinParts(parts), false, nil
	}

	return out, true, nil
}

// sampleCompresses trial-compresses the head of the body to skip full encodes of random data.
func (c *ZstdCompressor) sampleCompresses(first []byte) bool {
	if len(first) == 0 {
		return true
	}

	enc, err := c.encoder()
	if err != nil {
		return true
	}

	sample := first[:min(len(first), zstdSampleBytes)]
	buf := enc.EncodeAll(sample, c.getScratch())
	ok := len(buf)*zstdPercent < len(sample)*zstdSampleRatioPct
	c.putScratch(buf)

	return ok
}

// Decompress decodes a zstd or gzip blob, chosen by magic bytes.
func (c *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	if isZstd(data) {
		dec, err := c.decoder()
		if err != nil {
			return nil, err
		}

		return dec.DecodeAll(data, nil)
	}

	return (&GzipCompressor{}).gunzip(data)
}

func joinParts(parts [][]byte) []byte {
	if len(parts) == 1 {
		return parts[0]
	}

	return slices.Concat(parts...)
}

func isZstd(data []byte) bool {
	return len(data) >= len(zstdMagic) && string(data[:len(zstdMagic)]) == zstdMagic
}
