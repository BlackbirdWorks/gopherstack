package s3_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestHandler_AccessLogDispatch confirms that a successful GetObject on a
// bucket with logging enabled appends an access log record to the target
// bucket under the configured prefix.
func TestHandler_AccessLogDispatch(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "src-bkt")
	mustCreateBucket(t, backend, "log-bkt")
	mustPutObject(t, backend, "src-bkt", "doc.txt", []byte("payload"))

	const loggingXML = `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<LoggingEnabled><TargetBucket>log-bkt</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled>` +
		`</BucketLoggingStatus>`
	require.NoError(t, backend.PutBucketLogging(context.Background(), "src-bkt", loggingXML))

	// GET on the source bucket should trigger a log dispatch to log-bkt.
	req := httptest.NewRequest(http.MethodGet, "/src-bkt/doc.txt", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Dispatch is async; wait for the object to appear (max ~1s).
	deadline := time.Now().Add(time.Second)
	var logKey string
	for time.Now().Before(deadline) {
		out, err := backend.ListObjectsV2(context.Background(), &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String("log-bkt"),
			Prefix: aws.String("logs/"),
		})
		if err == nil && len(out.Contents) > 0 {
			logKey = aws.ToString(out.Contents[0].Key)

			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, logKey, "expected an access-log object under logs/")

	out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("log-bkt"),
		Key:    aws.String(logKey),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(out.Body)
	line := string(body)
	require.Contains(t, line, "REST.GET.OBJECT")
	require.Contains(t, line, "src-bkt")
	require.Contains(t, line, "doc.txt")
	require.True(t, strings.HasSuffix(line, "\n"), "log line must end with newline")
}

// TestHandler_AccessLogDispatch_NoConfig confirms that buckets without
// logging configured do NOT incur any dispatch overhead — useful regression
// guard so adding logging doesn't accidentally write to every bucket.
func TestHandler_AccessLogDispatch_NoConfig(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "quiet-bkt")
	mustPutObject(t, backend, "quiet-bkt", "doc.txt", []byte("payload"))

	req := httptest.NewRequest(http.MethodGet, "/quiet-bkt/doc.txt", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Give dispatch goroutines a chance to (incorrectly) run, then verify
	// no log objects appeared. Polling with a hard cap keeps this fast.
	time.Sleep(100 * time.Millisecond)

	out, err := backend.ListObjectsV2(context.Background(), &sdk_s3.ListObjectsV2Input{
		Bucket: aws.String("quiet-bkt"),
	})
	require.NoError(t, err)
	// Only the original doc.txt should be present.
	require.Len(t, out.Contents, 1)
}
