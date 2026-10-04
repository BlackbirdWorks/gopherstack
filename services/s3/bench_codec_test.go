package s3_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func benchPayload(kind string, size int) []byte {
	switch kind {
	case "random":
		r := rand.New(rand.NewPCG(1, 2))
		out := make([]byte, size)

		for i := range out {
			out[i] = byte(r.Uint32())
		}

		return out
	default:
		var buf bytes.Buffer

		for i := 0; buf.Len() < size; i++ {
			fmt.Fprintf(
				&buf,
				`{"id":%d,"name":"user-%d","email":"user%d@example.com","active":true,"tags":["a","b"]}`+"\n",
				i,
				i%97,
				i,
			)
		}

		return buf.Bytes()[:size]
	}
}

type benchCodec struct {
	c    s3.Compressor
	name string
}

func benchCodecs() []benchCodec {
	return []benchCodec{{&s3.GzipCompressor{}, "gzip"}, {&s3.ZstdCompressor{}, "zstd"}}
}

func BenchmarkCodecObjectRoundTrip(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{{"2KiB", 2 << 10}, {"1MiB", 1 << 20}, {"16MiB", 16 << 20}}

	for _, cd := range benchCodecs() {
		for _, kind := range []string{"json", "random"} {
			for _, sz := range sizes {
				data := benchPayload(kind, sz.n)

				b.Run(fmt.Sprintf("put/%s/%s/%s", cd.name, kind, sz.name), func(b *testing.B) {
					backend := s3.NewInMemoryBackend(cd.c)
					_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("b")})
					b.ReportAllocs()
					b.SetBytes(int64(sz.n))
					b.ResetTimer()

					for i := range b.N {
						_, err := backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
							Bucket: aws.String("b"), Key: aws.String(fmt.Sprintf("k%d", i%64)),
							Body: bytes.NewReader(data),
						})
						if err != nil {
							b.Fatal(err)
						}
					}

					stored := storedSize(b, cd.c, data)
					b.ReportMetric(float64(stored), "stored-B")
				})

				b.Run(fmt.Sprintf("get/%s/%s/%s", cd.name, kind, sz.name), func(b *testing.B) {
					backend := s3.NewInMemoryBackend(cd.c)
					_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("b")})
					_, err := backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
						Bucket: aws.String("b"), Key: aws.String("k"), Body: bytes.NewReader(data),
					})
					if err != nil {
						b.Fatal(err)
					}

					b.ReportAllocs()
					b.SetBytes(int64(sz.n))
					b.ResetTimer()

					for range b.N {
						out, gErr := backend.GetObject(b.Context(), &sdk_s3.GetObjectInput{
							Bucket: aws.String("b"), Key: aws.String("k"),
						})
						if gErr != nil {
							b.Fatal(gErr)
						}

						_ = out.Body.Close()
					}
				})
			}
		}
	}
}

func storedSize(b *testing.B, c s3.Compressor, data []byte) int {
	b.Helper()

	if z, ok := c.(*s3.ZstdCompressor); ok {
		out, _, err := z.CompressForStore([][]byte{data})
		if err != nil {
			b.Fatal(err)
		}

		return len(out)
	}

	out, err := c.Compress(data)
	if err != nil {
		b.Fatal(err)
	}

	return len(out)
}

func BenchmarkGetObjectRange16MiB(b *testing.B) {
	for _, cd := range benchCodecs() {
		b.Run(cd.name, func(b *testing.B) {
			backend := s3.NewInMemoryBackend(cd.c)
			handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
			_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("bkt")})
			benchServe(handler, http.MethodPut, "/bkt/k", bytes.NewReader(benchPayload("json", 16<<20)))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				req := httptest.NewRequest(http.MethodGet, "/bkt/k", nil)
				req.Header.Set("Range", "bytes=1000-5000")
				w := httptest.NewRecorder()
				serveS3Handler(handler, w, req)

				if w.Code != http.StatusPartialContent {
					b.Fatalf("status %d", w.Code)
				}
			}
		})
	}
}

func BenchmarkListObjectsV2_10kKeys(b *testing.B) {
	backend := s3.NewInMemoryBackend(nil)
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lb")})

	for i := range 10_000 {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String("lb"), Key: aws.String(fmt.Sprintf("dir%d/sub%d/key-%d", i%50, i%7, i)),
			Body: bytes.NewReader([]byte("x")),
		})
	}

	tests := []struct {
		name   string
		prefix string
		delim  string
	}{
		{"flat", "", ""},
		{"prefix", "dir1/", ""},
		{"prefix_delimiter", "dir1/", "/"},
		{"delimiter_root", "", "/"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			in := &sdk_s3.ListObjectsV2Input{Bucket: aws.String("lb"), MaxKeys: aws.Int32(1000)}
			if tt.prefix != "" {
				in.Prefix = aws.String(tt.prefix)
			}

			if tt.delim != "" {
				in.Delimiter = aws.String(tt.delim)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if _, err := backend.ListObjectsV2(b.Context(), in); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
