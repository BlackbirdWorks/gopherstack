// Package compress is runtime content-negotiating response compression (zstd, br, gzip)
// with pooled encoders and an optional cache for immutable assets.
package compress

import (
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

const (
	// DefaultMinSize is the smallest body worth compressing.
	DefaultMinSize = 1024

	defaultBrotliLevel = 4
	defaultGzipLevel   = 5
	defaultCacheBytes  = 64 << 20
	zstdWindow         = 512 << 10
	crc32Header        = "X-Amz-Crc32"
	typicalRatio       = 2
)

// Config tunes a Compressor. Zero values pick benchmarked defaults.
type Config struct {
	// Cacheable marks requests whose compressed output is cached by path and coding.
	Cacheable func(*http.Request) bool
	// Encodings restricts the enabled codings (default: all).
	Encodings []Encoding
	// MinSize is the minimum body size to compress (default 1 KiB).
	MinSize int
	// ZstdLevel, BrotliLevel and GzipLevel override encoder levels.
	ZstdLevel   zstd.EncoderLevel
	BrotliLevel int
	GzipLevel   int
	// CacheMaxBytes bounds the cache (default 64 MiB).
	CacheMaxBytes int64
	// AlwaysVary sets Vary: Accept-Encoding even when the client sent none.
	AlwaysVary bool
	// RewriteCRC32 recomputes X-Amz-Crc32 over the compressed bytes.
	RewriteCRC32 bool
}

type cacheEntry struct {
	header http.Header
	body   []byte
}

// Compressor compresses responses. It is safe for concurrent use.
type Compressor struct {
	pools      [numEncodings]sync.Pool
	zstdAll    *zstd.Encoder
	cache      sync.Map
	cfg        Config
	cacheBytes atomic.Int64
	zstdOnce   sync.Once
	enabled    [numEncodings]bool
}

type encoder interface {
	io.WriteCloser
	Flush() error
	Reset(io.Writer)
}

// New builds a Compressor from cfg.
func New(cfg Config) *Compressor {
	if cfg.MinSize <= 0 {
		cfg.MinSize = DefaultMinSize
	}

	if cfg.BrotliLevel == 0 {
		cfg.BrotliLevel = defaultBrotliLevel
	}

	if cfg.GzipLevel == 0 {
		cfg.GzipLevel = defaultGzipLevel
	}

	if cfg.ZstdLevel == 0 {
		cfg.ZstdLevel = zstd.SpeedDefault
	}

	if cfg.CacheMaxBytes <= 0 {
		cfg.CacheMaxBytes = defaultCacheBytes
	}

	c := &Compressor{cfg: cfg}

	for i, enc := range preference() {
		c.enabled[i] = len(cfg.Encodings) == 0 || slices.Contains(cfg.Encodings, enc)
	}

	return c
}

func (c *Compressor) isEnabled(e Encoding) bool {
	i := indexOf(string(e))

	return i >= 0 && c.enabled[i]
}

func (c *Compressor) getEncoder(e Encoding, dst io.Writer) encoder {
	i := indexOf(string(e))
	if v, ok := c.pools[i].Get().(encoder); ok {
		v.Reset(dst)

		return v
	}

	switch e {
	case Zstd:
		zw, _ := zstd.NewWriter(dst,
			zstd.WithEncoderLevel(c.cfg.ZstdLevel),
			zstd.WithEncoderConcurrency(1),
			zstd.WithWindowSize(zstdWindow),
			zstd.WithLowerEncoderMem(true))

		return zw
	case Brotli:
		return brotli.NewWriterLevel(dst, c.cfg.BrotliLevel)
	default:
		gw, _ := gzip.NewWriterLevel(dst, c.cfg.GzipLevel)

		return gw
	}
}

func (c *Compressor) putEncoder(e Encoding, w encoder) {
	w.Reset(io.Discard)
	c.pools[indexOf(string(e))].Put(w)
}

func (c *Compressor) encodeAll(e Encoding, src []byte) []byte {
	if e == Zstd {
		c.zstdOnce.Do(func() {
			c.zstdAll, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(c.cfg.ZstdLevel))
		})

		return c.zstdAll.EncodeAll(src, make([]byte, 0, len(src)/typicalRatio))
	}

	buf := make([]byte, 0, len(src)/typicalRatio)
	sink := (*appendWriter)(&buf)
	w := c.getEncoder(e, sink)
	_, _ = w.Write(src)
	_ = w.Close()
	c.putEncoder(e, w)

	return buf
}

type appendWriter []byte

func (a *appendWriter) Write(p []byte) (int, error) {
	*a = append(*a, p...)

	return len(p), nil
}

func (c *Compressor) cacheKey(r *http.Request, e Encoding) string {
	if c.cfg.Cacheable == nil || r.Method != http.MethodGet || r.Header.Get("Range") != "" || !c.cfg.Cacheable(r) {
		return ""
	}

	return string(e) + " " + r.URL.Path
}

func (c *Compressor) store(key string, h http.Header, body []byte) {
	n := int64(len(body))
	if c.cacheBytes.Add(n) > c.cfg.CacheMaxBytes {
		c.cacheBytes.Add(-n)

		return
	}

	if _, loaded := c.cache.LoadOrStore(key, &cacheEntry{header: h.Clone(), body: body}); loaded {
		c.cacheBytes.Add(-n)
	}
}

func (c *Compressor) serveCached(w http.ResponseWriter, key string) bool {
	v, ok := c.cache.Load(key)
	if !ok {
		return false
	}

	e, _ := v.(*cacheEntry)
	h := w.Header()

	for k, vals := range e.header {
		h[k] = append(h[k][:0:0], vals...)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(e.body) // #nosec G705 -- bytes produced by the wrapped handler

	return true
}

// Handler wraps next with response compression.
func (c *Compressor) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, finish, served := c.Begin(w, r)
		if served {
			return
		}

		next.ServeHTTP(rw, r)

		if finish != nil {
			finish()
		}
	})
}

// Begin negotiates for r: served=true means a cached response was written; otherwise use the
// returned writer and run finish (nil when not compressing) after the handler returns.
func (c *Compressor) Begin(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, func(), bool) {
	enc := Negotiate(r.Header[acceptEncoding], c.isEnabled)
	if enc == "" || r.Method == http.MethodHead || r.Method == http.MethodConnect ||
		r.Header.Get("Upgrade") != "" || r.Header.Get("Range") != "" {
		if c.cfg.AlwaysVary {
			addVary(w.Header())
		}

		return w, nil, false
	}

	key := c.cacheKey(r, enc)
	if key != "" && c.serveCached(w, key) {
		return w, nil, true
	}

	rw := &responseWriter{ResponseWriter: w, c: c, enc: enc, key: key, buffered: key != ""}

	return rw, rw.finish, false
}

func addVary(h http.Header) {
	for _, v := range h.Values("Vary") {
		if v == "*" || strings.Contains(strings.ToLower(v), "accept-encoding") {
			return
		}
	}

	h.Add("Vary", acceptEncoding)
}
