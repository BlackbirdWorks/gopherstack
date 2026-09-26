package s3_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestHandler_AccessLogDispatch(t *testing.T) {
	t.Parallel()

	const loggingXML = `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<LoggingEnabled><TargetBucket>log-bkt</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled>` +
		`</BucketLoggingStatus>`

	tests := []struct {
		setup       func(*testing.T, *s3.InMemoryBackend)
		name        string
		bucket      string
		key         string
		wantLog     bool
		wantObjects int
	}{
		{
			name:        "logging_enabled_writes_access_log",
			bucket:      "src-bkt",
			key:         "doc.txt",
			wantLog:     true,
			wantObjects: 1,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()

				mustCreateBucket(t, backend, "src-bkt")
				mustCreateBucket(t, backend, "log-bkt")
				mustPutObject(t, backend, "src-bkt", "doc.txt", []byte("payload"))
				require.NoError(t, backend.PutBucketLogging(t.Context(), "src-bkt", loggingXML))
			},
		},
		{
			name:        "no_logging_config_writes_no_access_log",
			bucket:      "quiet-bkt",
			key:         "doc.txt",
			wantObjects: 1,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()

				mustCreateBucket(t, backend, "quiet-bkt")
				mustPutObject(t, backend, "quiet-bkt", "doc.txt", []byte("payload"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				handler, backend := newTestHandler(t)
				tt.setup(t, backend)

				req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key, nil)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				synctest.Wait()

				if tt.wantLog {
					logKey := findAccessLog(t, backend)
					out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
						Bucket: aws.String("log-bkt"),
						Key:    aws.String(logKey),
					})
					require.NoError(t, err)

					body, err := io.ReadAll(out.Body)
					require.NoError(t, err)

					line := string(body)
					require.Contains(t, line, "REST.GET.OBJECT")
					require.Contains(t, line, tt.bucket)
					require.Contains(t, line, tt.key)
					require.True(t, strings.HasSuffix(line, "\n"), "log line must end with newline")
				}

				if !tt.wantLog {
					out, err := backend.ListObjectsV2(context.Background(), &sdk_s3.ListObjectsV2Input{
						Bucket: aws.String(tt.bucket),
					})
					require.NoError(t, err)
					require.Len(t, out.Contents, tt.wantObjects)
				}
			})
		})
	}
}

func findAccessLog(t *testing.T, backend *s3.InMemoryBackend) string {
	t.Helper()

	out, err := backend.ListObjectsV2(context.Background(), &sdk_s3.ListObjectsV2Input{
		Bucket: aws.String("log-bkt"),
		Prefix: aws.String("logs/"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Contents, "expected an access-log object under logs/")

	return aws.ToString(out.Contents[0].Key)
}
