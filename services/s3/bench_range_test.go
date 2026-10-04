package s3_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func newRangeBenchHandler(b *testing.B, size int) *s3.S3Handler {
	b.Helper()

	backend := s3.NewInMemoryBackend(&s3.ZstdCompressor{})
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("bkt")})

	if rec := benchServe(
		handler,
		http.MethodPut,
		"/bkt/k",
		bytes.NewReader(benchPayload("json", size)),
	); rec.Code != http.StatusOK {
		b.Fatalf("put status %d", rec.Code)
	}

	return handler
}

func BenchmarkRangeGetLarge(b *testing.B) {
	for _, objMiB := range []int{16, 128} {
		size := objMiB << 20
		handler := newRangeBenchHandler(b, size)

		for _, win := range []int{4 << 10, 1 << 20} {
			for _, pos := range []string{"start", "middle", "end"} {
				var start int

				switch pos {
				case "middle":
					start = size/2 - win/2
				case "end":
					start = size - win
				}

				b.Run(fmt.Sprintf("%dMiB/range%dKiB/%s", objMiB, win>>10, pos), func(b *testing.B) {
					header := fmt.Sprintf("bytes=%d-%d", start, start+win-1)
					b.ReportAllocs()
					b.ResetTimer()

					for range b.N {
						req := httptest.NewRequest(http.MethodGet, "/bkt/k", nil)
						req.Header.Set("Range", header)
						rec := httptest.NewRecorder()
						serveS3Handler(handler, rec, req)

						if rec.Code != http.StatusPartialContent {
							b.Fatalf("status %d", rec.Code)
						}
					}
				})
			}
		}

		b.Run(fmt.Sprintf("%dMiB/full", objMiB), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for range b.N {
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, httptest.NewRequest(http.MethodGet, "/bkt/k", nil))

				if rec.Code != http.StatusOK {
					b.Fatalf("status %d", rec.Code)
				}
			}
		})
	}
}

func BenchmarkPutLarge(b *testing.B) {
	for _, objMiB := range []int{16, 128} {
		data := benchPayload("json", objMiB<<20)

		b.Run(fmt.Sprintf("%dMiB", objMiB), func(b *testing.B) {
			backend := s3.NewInMemoryBackend(&s3.ZstdCompressor{})
			handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
			_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("bkt")})
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for range b.N {
				if rec := benchServe(
					handler,
					http.MethodPut,
					"/bkt/k",
					bytes.NewReader(data),
				); rec.Code != http.StatusOK {
					b.Fatalf("status %d", rec.Code)
				}
			}

			b.ReportMetric(float64(len(s3.PeekStoredBytes(backend, "bkt", "k"))), "stored-B")
		})
	}
}
