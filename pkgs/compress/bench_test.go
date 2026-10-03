package compress_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/compress"
)

type nopWriter struct{ h http.Header }

func (w *nopWriter) Header() http.Header         { return w.h }
func (w *nopWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *nopWriter) WriteHeader(int)             {}

func BenchmarkHandler(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10, 512 << 10} {
		body := bigJSON(size)

		for _, enc := range []string{"identity", "gzip", "br", "zstd"} {
			b.Run(enc+"/"+byteLabel(size), func(b *testing.B) {
				h := compress.New(compress.Config{}).
					Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write(body)
					}))
				req := httptest.NewRequest(http.MethodGet, "/x", nil)
				req.Header.Set("Accept-Encoding", enc)
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()

				w := &nopWriter{h: http.Header{}}

				for b.Loop() {
					clear(w.h)
					h.ServeHTTP(w, req)
				}
			})
		}
	}
}

func byteLabel(n int) string {
	if n >= 1<<10 {
		return itoa(n>>10) + "KiB"
	}

	return itoa(n)
}

func itoa(n int) string {
	const digits = "0123456789"

	if n == 0 {
		return "0"
	}

	var out []byte

	for ; n > 0; n /= 10 {
		out = append([]byte{digits[n%10]}, out...)
	}

	return string(out)
}
