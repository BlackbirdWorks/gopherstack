package s3_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const benchObjectSize64KiB = 64 * 1024

// benchHandler builds a handler+backend pair for full-HTTP-path benchmarks,
// mirroring newTestHandler (which is typed for *testing.T, not usable from
// *testing.B).
func benchHandler(b *testing.B) (*s3.S3Handler, *s3.InMemoryBackend) {
	b.Helper()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})

	return handler, backend
}

func benchServe(handler *s3.S3Handler, method, path string, body *bytes.Reader) *httptest.ResponseRecorder {
	var req *http.Request
	if body == nil {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, body)
	}
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	return rec
}

// BenchmarkPutObject_64KiB drives PutObject through the full HTTP handler
// with a body above the compression threshold, so it captures compression
// and body-buffer costs on the object-write hot path.
func BenchmarkPutObject_64KiB(b *testing.B) {
	handler, backend := benchHandler(b)
	bucketName := "bench-put-64k"
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	data := bytes.Repeat([]byte("a"), benchObjectSize64KiB)

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		rec := benchServe(handler, http.MethodPut,
			fmt.Sprintf("/%s/key-%d", bucketName, i), bytes.NewReader(data))
		if rec.Code != http.StatusOK {
			b.Fatalf("PutObject failed: %d", rec.Code)
		}
	}
}

// BenchmarkGetObject_64KiB drives GetObject through the full HTTP handler
// against pre-populated 64KiB (compressed) objects.
func BenchmarkGetObject_64KiB(b *testing.B) {
	handler, backend := benchHandler(b)
	bucketName := "bench-get-64k"
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	data := bytes.Repeat([]byte("a"), benchObjectSize64KiB)

	const objectCount = 1000
	for i := range objectCount {
		rec := benchServe(handler, http.MethodPut,
			fmt.Sprintf("/%s/key-%d", bucketName, i), bytes.NewReader(data))
		if rec.Code != http.StatusOK {
			b.Fatalf("setup PutObject failed: %d", rec.Code)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		rec := benchServe(handler, http.MethodGet,
			fmt.Sprintf("/%s/key-%d", bucketName, i%objectCount), nil)
		if rec.Code != http.StatusOK {
			b.Fatalf("GetObject failed: %d", rec.Code)
		}
	}
}

// BenchmarkHeadObject drives HeadObject through the full HTTP handler.
func BenchmarkHeadObject(b *testing.B) {
	handler, backend := benchHandler(b)
	bucketName := "bench-head"
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	data := bytes.Repeat([]byte("a"), benchObjectSize64KiB)
	setupRec := benchServe(handler, http.MethodPut, "/"+bucketName+"/key", bytes.NewReader(data))
	if setupRec.Code != http.StatusOK {
		b.Fatalf("setup PutObject failed: %d", setupRec.Code)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		rec := benchServe(handler, http.MethodHead, "/"+bucketName+"/key", nil)
		if rec.Code != http.StatusOK {
			b.Fatalf("HeadObject failed: %d", rec.Code)
		}
	}
}

// BenchmarkListObjectsV2_1000Keys drives ListObjectsV2 through the full HTTP
// handler over a 1000-key bucket.
func BenchmarkListObjectsV2_1000Keys(b *testing.B) {
	handler, backend := benchHandler(b)
	bucketName := "bench-list-1000"
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})

	const objectCount = 1000
	data := []byte("x")
	for i := range objectCount {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fmt.Sprintf("key-%d", i)),
			Body:   bytes.NewReader(data),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		rec := benchServe(handler, http.MethodGet, "/"+bucketName+"?list-type=2&max-keys=1000", nil)
		if rec.Code != http.StatusOK {
			b.Fatalf("ListObjectsV2 failed: %d", rec.Code)
		}
	}
}

// BenchmarkCompleteMultipartUpload_5MiB mirrors cmd/pgoload's multipart
// pattern (a >=5MiB first part, required by the non-final-part size floor)
// through the full HTTP handler: CreateMultipartUpload, UploadPart,
// CompleteMultipartUpload. This is the object-path hot spot pgoload's CPU
// profile pointed at (GzipCompressor.Compress under assembleMultipartData).
func BenchmarkCompleteMultipartUpload_5MiB(b *testing.B) {
	handler, backend := benchHandler(b)
	bucketName := "bench-mpu-5m"
	_, _ = backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	partData := bytes.Repeat([]byte("a"), 5*1024*1024)

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		key := fmt.Sprintf("key-%d", i)

		createRec := benchServe(handler, http.MethodPost, "/"+bucketName+"/"+key+"?uploads", nil)
		if createRec.Code != http.StatusOK {
			b.Fatalf("CreateMultipartUpload failed: %d", createRec.Code)
		}
		var initResult s3.InitiateMultipartUploadResult
		if err := xml.NewDecoder(createRec.Body).Decode(&initResult); err != nil {
			b.Fatalf("decode CreateMultipartUpload response: %v", err)
		}
		uploadID := initResult.UploadID

		partPath := fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucketName, key, uploadID)
		partRec := benchServe(handler, http.MethodPut, partPath, bytes.NewReader(partData))
		if partRec.Code != http.StatusOK {
			b.Fatalf("UploadPart failed: %d", partRec.Code)
		}
		etag := partRec.Header().Get("ETag")

		completeBody := fmt.Sprintf(
			`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
			etag,
		)
		completePath := fmt.Sprintf("/%s/%s?uploadId=%s", bucketName, key, uploadID)
		completeRec := benchServe(
			handler, http.MethodPost, completePath, bytes.NewReader([]byte(completeBody)),
		)
		if completeRec.Code != http.StatusOK {
			b.Fatalf("CompleteMultipartUpload failed: %d", completeRec.Code)
		}
	}
}

func BenchmarkPutObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_, _ = backend.CreateBucket(
		b.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("benchmarking data")

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}
}

func BenchmarkGetObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	bucketName := "bench-bucket"
	_, _ = backend.CreateBucket(
		b.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("benchmarking data")

	for i := range 1000 {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = backend.GetObject(b.Context(), &sdk_s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fmt.Sprintf("key-%d", i%1000)),
		})
	}
}

func BenchmarkCalculateChecksum(b *testing.B) {
	data := []byte("some data to calculate checksum for benchmarking purpose")

	b.Run("SHA256", func(b *testing.B) {
		for range b.N {
			_ = s3.CalculateChecksum(data, "SHA256")
		}
	})

	b.Run("CRC32", func(b *testing.B) {
		for range b.N {
			_ = s3.CalculateChecksum(data, "CRC32")
		}
	})
}

// BenchmarkListObjectsV2 measures listing throughput over a large bucket,
// with and without a prefix/delimiter, to give the s3 deep pass (gopherstack-3dqa)
// a real number for its previously-unmeasured optimization axis.
func BenchmarkListObjectsV2(b *testing.B) {
	const objectCount = 50_000

	backend := s3.NewInMemoryBackend(nil)
	bucketName := "bench-list-bucket"
	_, _ = backend.CreateBucket(
		b.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	data := []byte("x")
	for i := range objectCount {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fmt.Sprintf("dir%d/key-%d", i%100, i)),
			Body:   bytes.NewReader(data),
		})
	}

	b.Run("flat_maxkeys1000", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
				Bucket:  aws.String(bucketName),
				MaxKeys: aws.Int32(1000),
			})
		}
	})

	b.Run("prefix_delimiter", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
				Bucket:    aws.String(bucketName),
				Prefix:    aws.String("dir1/"),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(1000),
			})
		}
	})

	b.Run("common_prefix_only", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
				Bucket:    aws.String(bucketName),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(1000),
			})
		}
	})
}

// BenchmarkDeleteObjects measures DeleteObjects throughput when removing many
// keys from the same bucket. The single-lock-per-batch implementation avoids
// the per-object lock churn of the previous per-object DeleteObject loop.
func BenchmarkDeleteObjects(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprintf("%d_objects", count), func(b *testing.B) {
			b.StopTimer()
			backend := s3.NewInMemoryBackend(nil)
			bucketName := "bench-delete-bucket"
			_, _ = backend.CreateBucket(
				b.Context(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)},
			)
			objects := make([]sdk_s3_types.ObjectIdentifier, count)
			for i := range count {
				key := aws.String(fmt.Sprintf("key-%d", i))
				_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    key,
					Body:   bytes.NewReader([]byte("data")),
				})
				objects[i] = sdk_s3_types.ObjectIdentifier{Key: key}
			}
			b.StartTimer()

			for range b.N {
				_, _ = backend.DeleteObjects(b.Context(), &sdk_s3.DeleteObjectsInput{
					Bucket: aws.String(bucketName),
					Delete: &sdk_s3_types.Delete{Objects: objects},
				})
			}
		})
	}
}
