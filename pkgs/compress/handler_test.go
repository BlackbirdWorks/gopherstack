package compress_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/andybalholm/brotli"
	kgzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/compress"
)

func bigJSON(n int) []byte {
	var sb strings.Builder

	sb.WriteString(`{"items":[`)

	for i := 0; sb.Len() < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}

		fmt.Fprintf(&sb, `{"id":%d,"name":"item-%d","tags":["alpha","beta","gamma"]}`, i, i)
	}

	sb.WriteString(`]}`)

	return []byte(sb.String())
}

func decode(t *testing.T, enc string, body []byte) []byte {
	t.Helper()

	var (
		r   io.Reader
		err error
	)

	switch enc {
	case "":
		return body
	case "gzip":
		r, err = kgzip.NewReader(bytes.NewReader(body))
	case "br":
		r = brotli.NewReader(bytes.NewReader(body))
	case "zstd":
		var zr *zstd.Decoder

		zr, err = zstd.NewReader(bytes.NewReader(body))
		if err == nil {
			defer zr.Close()
		}

		r = zr
	default:
		t.Fatalf("unknown encoding %q", enc)
	}

	require.NoError(t, err)

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return out
}

func serve(
	t *testing.T,
	c *compress.Compressor,
	h http.HandlerFunc,
	method, ae string,
	hdr map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, "/x", nil)
	if ae != "" {
		req.Header.Set("Accept-Encoding", ae)
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	c.Handler(h).ServeHTTP(rec, req)

	return rec
}

func jsonHandler(body []byte, extra func(http.Header)) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))

		if extra != nil {
			extra(w.Header())
		}

		_, _ = w.Write(body)
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	body := bigJSON(20 << 10)
	sizes := []int{1024, 5000, 300 << 10}

	for _, enc := range []string{"zstd", "br", "gzip"} {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%s/%d", enc, size), func(t *testing.T) {
				t.Parallel()

				b := bigJSON(size)
				if size == 5000 {
					b = body[:5000]
				}

				c := compress.New(compress.Config{})
				rec := serve(t, c, func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")

					for off := 0; off < len(b); off += 777 {
						_, _ = w.Write(b[off:min(off+777, len(b))])
					}
				}, http.MethodGet, enc, nil)

				assert.Equal(t, enc, rec.Header().Get("Content-Encoding"))
				assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
				assert.Empty(t, rec.Header().Get("Content-Length"))
				assert.Less(t, rec.Body.Len(), len(b))
				assert.Equal(t, b, decode(t, enc, rec.Body.Bytes()))
			})
		}
	}
}

func TestBehavior(t *testing.T) {
	t.Parallel()

	big := bigJSON(8 << 10)
	small := []byte(`{"a":1}`)

	tests := []struct {
		hdr       map[string]string
		h         http.HandlerFunc
		name      string
		method    string
		ae        string
		wantEnc   string
		wantETag  string
		wantBody  []byte
		wantCode  int
		wantVary  bool
		wantNoLen bool
		raw       bool
	}{
		{name: "no accept-encoding", method: "GET", h: jsonHandler(big, nil), wantBody: big, wantCode: 200},
		{name: "identity", method: "GET", ae: "identity", h: jsonHandler(big, nil), wantBody: big, wantCode: 200},
		{
			name:      "compresses",
			method:    "GET",
			ae:        "gzip",
			h:         jsonHandler(big, nil),
			wantEnc:   "gzip",
			wantVary:  true,
			wantBody:  big,
			wantCode:  200,
			wantNoLen: true,
		},
		{
			name:     "below threshold",
			method:   "GET",
			ae:       "gzip",
			h:        jsonHandler(small, nil),
			wantBody: small,
			wantCode: 200,
			wantVary: true,
		},
		{
			name:   "unknown length below threshold",
			method: "GET",
			ae:     "br",
			h: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(small)
			},
			wantBody: small,
			wantCode: 200,
			wantVary: true,
		},
		{name: "binary type", method: "GET", ae: "gzip", h: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(big)
		}, wantBody: big, wantCode: 200},
		{
			name:     "already encoded",
			method:   "GET",
			ae:       "gzip",
			h:        jsonHandler(big, func(h http.Header) { h.Set("Content-Encoding", "br") }),
			wantEnc:  "br",
			wantBody: big,
			wantCode: 200,
			raw:      true,
		},
		{name: "head", method: "HEAD", ae: "gzip", h: jsonHandler(big, nil), wantBody: big, wantCode: 200},
		{name: "204", method: "GET", ae: "gzip", h: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNoContent)
		}, wantCode: 204},
		{name: "304", method: "GET", ae: "gzip", h: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotModified)
		}, wantCode: 304},
		{name: "206", method: "GET", ae: "gzip", h: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(big)
		}, wantBody: big, wantCode: 206},
		{
			name:     "range request",
			method:   "GET",
			ae:       "gzip",
			hdr:      map[string]string{"Range": "bytes=0-10"},
			h:        jsonHandler(big, nil),
			wantBody: big,
			wantCode: 200,
		},
		{
			name:     "no-transform",
			method:   "GET",
			ae:       "gzip",
			h:        jsonHandler(big, func(h http.Header) { h.Set("Cache-Control", "no-transform") }),
			wantBody: big,
			wantCode: 200,
		},
		{
			name:     "upgrade",
			method:   "GET",
			ae:       "gzip",
			hdr:      map[string]string{"Upgrade": "websocket"},
			h:        jsonHandler(big, nil),
			wantBody: big,
			wantCode: 200,
		},
		{name: "error status compressed", method: "GET", ae: "gzip", h: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write(big)
		}, wantEnc: "gzip", wantVary: true, wantBody: big, wantCode: 500},
		{
			name:     "strong etag weakened",
			method:   "GET",
			ae:       "zstd",
			h:        jsonHandler(big, func(h http.Header) { h.Set("ETag", `"abc"`) }),
			wantEnc:  "zstd",
			wantVary: true,
			wantBody: big,
			wantCode: 200,
			wantETag: `W/"abc"`,
		},
		{
			name:     "weak etag kept",
			method:   "GET",
			ae:       "zstd",
			h:        jsonHandler(big, func(h http.Header) { h.Set("ETag", `W/"abc"`) }),
			wantEnc:  "zstd",
			wantVary: true,
			wantBody: big,
			wantCode: 200,
			wantETag: `W/"abc"`,
		},
		{name: "sniffed text", method: "GET", ae: "gzip", h: func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(bytes.Repeat([]byte("hello world "), 500))
		}, wantEnc: "gzip", wantVary: true, wantBody: bytes.Repeat([]byte("hello world "), 500), wantCode: 200},
		{
			name:     "vary merged",
			method:   "GET",
			ae:       "gzip",
			h:        jsonHandler(big, func(h http.Header) { h.Set("Vary", "Origin") }),
			wantEnc:  "gzip",
			wantVary: true,
			wantBody: big,
			wantCode: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := serve(t, compress.New(compress.Config{}), tt.h, tt.method, tt.ae, tt.hdr)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, tt.wantEnc, rec.Header().Get("Content-Encoding"))
			assert.Equal(
				t,
				tt.wantVary,
				strings.Contains(strings.Join(rec.Header().Values("Vary"), ","), "Accept-Encoding"),
			)

			if tt.wantBody != nil {
				enc := tt.wantEnc
				if tt.raw {
					enc = ""
				}

				assert.Equal(t, tt.wantBody, decode(t, enc, rec.Body.Bytes()))
			} else {
				assert.Empty(t, rec.Body.Bytes())
			}

			if tt.wantNoLen {
				assert.Empty(t, rec.Header().Get("Content-Length"))
			}

			if tt.wantETag != "" {
				assert.Equal(t, tt.wantETag, rec.Header().Get("ETag"))
			}
		})
	}
}

func TestVaryMergedWithExisting(t *testing.T) {
	t.Parallel()

	rec := serve(t, compress.New(compress.Config{}), jsonHandler(bigJSON(4096), func(h http.Header) {
		h.Set("Vary", "Origin")
	}), http.MethodGet, "gzip", nil)

	assert.ElementsMatch(t, []string{"Origin", "Accept-Encoding"}, rec.Header().Values("Vary"))
}

func TestAlwaysVaryWithoutAcceptEncoding(t *testing.T) {
	t.Parallel()

	body := bigJSON(4096)
	rec := serve(t, compress.New(compress.Config{AlwaysVary: true}), jsonHandler(body, nil), http.MethodGet, "", nil)

	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, rec.Body.Bytes())
}

func TestUntouchedWithoutAcceptEncoding(t *testing.T) {
	t.Parallel()

	body := bigJSON(4096)
	base := httptest.NewRecorder()
	jsonHandler(body, nil)(base, httptest.NewRequest(http.MethodGet, "/x", nil))

	got := serve(t, compress.New(compress.Config{}), jsonHandler(body, nil), http.MethodGet, "identity", nil)

	assert.Equal(t, base.Header(), got.Header())
	assert.Equal(t, base.Body.Bytes(), got.Body.Bytes())
}

func TestDisabledEncodings(t *testing.T) {
	t.Parallel()

	body := bigJSON(4096)
	c := compress.New(compress.Config{Encodings: []compress.Encoding{compress.Gzip}})
	rec := serve(t, c, jsonHandler(body, nil), http.MethodGet, "zstd, br, gzip", nil)

	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
}

func TestMinSizeConfig(t *testing.T) {
	t.Parallel()

	body := bigJSON(1500)
	rec := serve(t, compress.New(compress.Config{MinSize: 4096}), jsonHandler(body, nil), http.MethodGet, "gzip", nil)

	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, rec.Body.Bytes())
}

func TestStreamingFlush(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ct       string
		wantEnc  string
		first    []byte
		wantFlsh bool
	}{
		{name: "sse passthrough", ct: "text/event-stream", first: []byte("data: one\n\n")},
		{name: "eventstream passthrough", ct: "application/vnd.amazon.eventstream", first: []byte("frame-1")},
		{name: "connect stream passthrough", ct: "application/connect+json", first: []byte("{}")},
		{name: "small json flush stays identity", ct: "application/json", first: []byte(`{"a":1}`)},
		{name: "large json flush compresses", ct: "application/json", first: bigJSON(4096), wantEnc: "gzip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			release := make(chan struct{})
			done := make(chan struct{})

			srv := httptest.NewServer(
				compress.New(compress.Config{}).Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					defer close(done)

					w.Header().Set("Content-Type", tt.ct)
					_, _ = w.Write(tt.first)
					assert.NoError(t, http.NewResponseController(w).Flush())
					<-release
					_, _ = w.Write([]byte("tail"))
				})),
			)
			defer srv.Close()

			req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
			require.NoError(t, err)
			req.Header.Set("Accept-Encoding", "gzip")

			resp, err := http.DefaultTransport.RoundTrip(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantEnc, resp.Header.Get("Content-Encoding"))

			var first []byte

			if tt.wantEnc == "" {
				first = make([]byte, len(tt.first))
				_, err = io.ReadFull(resp.Body, first)
				require.NoError(t, err)
				assert.Equal(t, tt.first, first)
			} else {
				zr, zerr := kgzip.NewReader(resp.Body)
				require.NoError(t, zerr)

				first = make([]byte, len(tt.first))
				_, err = io.ReadFull(zr, first)
				require.NoError(t, err)
				assert.Equal(t, tt.first, first)
			}

			close(release)
			<-done
		})
	}
}

func TestCRC32Rewrite(t *testing.T) {
	t.Parallel()

	body := bigJSON(8192)
	h := jsonHandler(body, func(h http.Header) { h.Set("X-Amz-Crc32", "12345") })

	on := serve(t, compress.New(compress.Config{RewriteCRC32: true}), h, http.MethodGet, "gzip", nil)
	off := serve(t, compress.New(compress.Config{}), h, http.MethodGet, "gzip", nil)

	assert.NotEqual(t, "12345", on.Header().Get("X-Amz-Crc32"))
	assert.Equal(t, "12345", off.Header().Get("X-Amz-Crc32"))
	assert.Equal(t, strconv.Itoa(on.Body.Len()), on.Header().Get("Content-Length"))
	assert.Equal(t, body, decode(t, "gzip", on.Body.Bytes()))

	idn := serve(t, compress.New(compress.Config{RewriteCRC32: true}), h, http.MethodGet, "", nil)
	assert.Equal(t, "12345", idn.Header().Get("X-Amz-Crc32"))
}

func TestCache(t *testing.T) {
	t.Parallel()

	body := bigJSON(16 << 10)

	var calls atomic.Int32

	h := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		jsonHandler(body, nil)(w, nil)
	}

	c := compress.New(compress.Config{Cacheable: func(*http.Request) bool { return true }})

	first := serve(t, c, h, http.MethodGet, "br", nil)
	second := serve(t, c, h, http.MethodGet, "br", nil)
	other := serve(t, c, h, http.MethodGet, "gzip", nil)

	assert.Equal(t, int32(2), calls.Load())
	assert.Equal(t, "br", second.Header().Get("Content-Encoding"))
	assert.Equal(t, first.Body.Bytes(), second.Body.Bytes())
	assert.Equal(t, first.Header(), second.Header())
	assert.Equal(t, strconv.Itoa(second.Body.Len()), second.Header().Get("Content-Length"))
	assert.Equal(t, body, decode(t, "br", second.Body.Bytes()))
	assert.Equal(t, "gzip", other.Header().Get("Content-Encoding"))
}

func TestCacheBounded(t *testing.T) {
	t.Parallel()

	body := bigJSON(16 << 10)

	var calls atomic.Int32

	h := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		jsonHandler(body, nil)(w, nil)
	}

	c := compress.New(compress.Config{
		Cacheable:     func(*http.Request) bool { return true },
		CacheMaxBytes: 1,
	})

	serve(t, c, h, http.MethodGet, "gzip", nil)
	serve(t, c, h, http.MethodGet, "gzip", nil)

	assert.Equal(t, int32(2), calls.Load())
}

func TestCacheSkippedForRangeAndHead(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	h := func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		jsonHandler(bigJSON(4096), nil)(w, r)
	}

	c := compress.New(compress.Config{Cacheable: func(*http.Request) bool { return true }})

	serve(t, c, h, http.MethodGet, "gzip", nil)
	serve(t, c, h, http.MethodHead, "gzip", nil)
	serve(t, c, h, http.MethodGet, "gzip", map[string]string{"Range": "bytes=0-5"})

	assert.Equal(t, int32(3), calls.Load())
}

func TestConcurrent(t *testing.T) {
	t.Parallel()

	body := bigJSON(32 << 10)
	c := compress.New(compress.Config{})
	h := c.Handler(jsonHandler(body, nil))

	var wg sync.WaitGroup

	for i := range 64 {
		enc := []string{"zstd", "br", "gzip"}[i%3]

		wg.Go(func() {
			for range 10 {
				req := httptest.NewRequest(http.MethodGet, "/x", nil)
				req.Header.Set("Accept-Encoding", enc)

				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)

				assert.Equal(t, enc, rec.Header().Get("Content-Encoding"))
				assert.Equal(t, body, decode(t, enc, rec.Body.Bytes()))
			}
		})
	}

	wg.Wait()
}

// Not parallel (global alloc counters); min-of-N because sync.Pool randomly drops under -race.
//
//nolint:paralleltest // measures process-wide allocation counters
func TestEncoderPoolReuse(t *testing.T) {
	body := bigJSON(4096)

	for _, enc := range []string{"zstd", "br", "gzip"} {
		t.Run(enc, func(t *testing.T) {
			c := compress.New(compress.Config{})
			h := c.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			}))
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.Header.Set("Accept-Encoding", enc)

			w := &nopWriter{h: http.Header{}}
			run := func() {
				clear(w.h)
				h.ServeHTTP(w, req)
			}
			run()

			best := uint64(1 << 62)

			for range 50 {
				var before, after runtime.MemStats

				runtime.ReadMemStats(&before)
				run()
				runtime.ReadMemStats(&after)

				best = min(best, after.TotalAlloc-before.TotalAlloc)
			}

			perOp := best
			assert.Less(t, perOp, uint64(48<<10), "encoder should be pooled, not rebuilt per request")
		})
	}
}
