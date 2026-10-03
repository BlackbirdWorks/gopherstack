package cloudwatch

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// maxInflatedBodyBytes is the CloudWatch 1 MB PutMetricData payload quota:
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
const maxInflatedBodyBytes = 1 << 20

var (
	errUnsupportedEncoding = errors.New("unsupported Content-Encoding")
	errBadGzipBody         = errors.New("invalid gzip request body")
	errInflatedTooLarge    = errors.New("decompressed request body exceeds 1 MB")
)

// inflateRequestBody decodes the gzip body the SDK sends for PutMetricData (@requestCompression);
// scoped to CloudWatch because S3 keeps Content-Encoding verbatim as object metadata.
func inflateRequestBody(r *http.Request) error {
	enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	if enc == "" || enc == "identity" {
		return nil
	}

	if enc != "gzip" {
		return errUnsupportedEncoding
	}

	raw, err := httputils.ReadBody(r)
	if err != nil {
		return err
	}

	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return errBadGzipBody
	}

	out, err := io.ReadAll(io.LimitReader(zr, maxInflatedBodyBytes+1))
	if err != nil {
		return errBadGzipBody
	}

	if len(out) > maxInflatedBodyBytes {
		return errInflatedTooLarge
	}

	r.Body = io.NopCloser(bytes.NewReader(out))
	r.ContentLength = int64(len(out))
	r.Header.Del("Content-Encoding")
	r.Header.Del("Content-Length")

	return nil
}
