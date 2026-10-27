package app

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const failedPutBodySize = 8 << 20

type countingBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n.Add(int64(n))

	return n, err
}

type zeroSeeker struct {
	size, off int64
}

func (z *zeroSeeker) Read(p []byte) (int, error) {
	if z.off >= z.size {
		return 0, io.EOF
	}

	n := min(int64(len(p)), z.size-z.off)
	clear(p[:n])
	z.off += n

	return int(n), nil
}

func (z *zeroSeeker) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		z.off = off
	case io.SeekCurrent:
		z.off += off
	case io.SeekEnd:
		z.off = z.size + off
	}

	return z.off, nil
}

func TestS3FailedPutDoesNotDrainBody(t *testing.T) {
	t.Parallel()

	var consumed atomic.Int64

	fx := newWrappedOptionsFixture(t, func(*CLI) {}, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.Body != nil {
				r.Body = countingBody{ReadCloser: r.Body, n: &consumed}
			}

			next.ServeHTTP(w, r)
		})
	})

	sc := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })

	_, err := sc.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("no-such-bucket"),
		Key:    aws.String("big"),
		Body:   &zeroSeeker{size: failedPutBodySize},
	})
	require.Error(t, err)

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
	assert.Less(t, consumed.Load(), int64(failedPutBodySize), "handler must not read the whole failed body")
}
