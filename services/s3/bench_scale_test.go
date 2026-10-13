package s3_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

const benchScaleBucket = "scale"

func benchScrambledKey(i int) string {
	return "obj/" + strconv.FormatUint(uint64(i)*2654435761%4294967296, 36)
}

func benchScaleBackend(b *testing.B, keys int, versioned bool) *s3.InMemoryBackend {
	b.Helper()

	backend := s3.NewInMemoryBackend(nil)
	_, err := backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(benchScaleBucket)})
	if err != nil {
		b.Fatal(err)
	}

	if versioned {
		_, err = backend.PutBucketVersioning(b.Context(), &sdk_s3.PutBucketVersioningInput{
			Bucket: aws.String(benchScaleBucket),
			VersioningConfiguration: &sdk_s3_types.VersioningConfiguration{
				Status: sdk_s3_types.BucketVersioningStatusEnabled,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := range keys {
		_, err = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(
				benchScaleBucket,
			),
			Key:  aws.String(benchScrambledKey(i)),
			Body: bytes.NewReader([]byte("d")),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	return backend
}

func BenchmarkPutObject_NewKeyScale(b *testing.B) {
	for _, tc := range []struct {
		name      string
		keys      int
		versioned bool
	}{
		{"10k", 10_000, false},
		{"100k", 100_000, false},
		{"100k_versioned", 100_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			backend := benchScaleBackend(b, tc.keys, tc.versioned)
			body := []byte("payload")
			b.ReportAllocs()
			b.ResetTimer()

			i := tc.keys
			for b.Loop() {
				i++
				_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(
						benchScaleBucket,
					),
					Key:  aws.String(benchScrambledKey(i)),
					Body: bytes.NewReader(body),
				})
			}
		})
	}
}

func BenchmarkDeleteObject_Scale(b *testing.B) {
	backend := benchScaleBackend(b, 100_000, false)
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		key := benchScrambledKey(i % 100_000)
		_, _ = backend.DeleteObject(b.Context(), &sdk_s3.DeleteObjectInput{
			Bucket: aws.String(benchScaleBucket), Key: aws.String(key),
		})
		i++
		if i%100_000 == 0 {
			b.StopTimer()
			for j := range 100_000 {
				_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(
						benchScaleBucket,
					),
					Key:  aws.String(benchScrambledKey(j)),
					Body: bytes.NewReader([]byte("d")),
				})
			}
			b.StartTimer()
		}
	}
}

func BenchmarkCopyObject_Scale(b *testing.B) {
	backend := benchScaleBackend(b, 100_000, false)
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPut, "/"+benchScaleBucket+"/copy/"+strconv.Itoa(i), nil)
		req.Header.Set("X-Amz-Copy-Source", "/"+benchScaleBucket+"/"+benchScrambledKey(i%100_000))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		if rec.Code != http.StatusOK {
			b.Fatalf("status %d", rec.Code)
		}

		i++
	}
}

func BenchmarkListObjectsV2_Versioned100k(b *testing.B) {
	backend := benchScaleBackend(b, 100_000, true)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, _ = backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(benchScaleBucket), Prefix: aws.String("obj/1"), MaxKeys: aws.Int32(1000),
		})
	}
}
