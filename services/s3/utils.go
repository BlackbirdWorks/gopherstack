package s3

import (
	"bytes"
	"context"
	"crypto/md5"  //nolint:gosec // S3 checksum algorithm, not a security use of MD5
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// encodeListKey URL-encodes v when the request asked for encoding-type=url, which
// is what the AWS SDK URL-decodes on receipt. Returns v unchanged otherwise.
// Apply to Key/Prefix/Delimiter/markers — NOT to opaque continuation tokens.
func encodeListKey(encodingType, v string) string {
	if strings.EqualFold(encodingType, "url") {
		return url.QueryEscape(v)
	}

	return v
}

func parseUserMetadata(h http.Header) map[string]string {
	meta := make(map[string]string)
	for k, v := range h {
		lowerK := strings.ToLower(k)
		if key, ok := strings.CutPrefix(lowerK, "x-amz-meta-"); ok {
			if len(v) > 0 {
				meta[key] = v[0]
			}
		}
	}

	return meta
}

const maxUserMetadataBytes = 2048

func validateUserMetadataSize(h http.Header) error {
	total := 0
	for k, vals := range h {
		if key, ok := strings.CutPrefix(strings.ToLower(k), "x-amz-meta-"); ok && len(vals) > 0 {
			total += len(key) + len(vals[0])
		}
	}

	if total > maxUserMetadataBytes {
		return ErrMetadataTooLarge
	}

	return nil
}

const (
	crc32Len = 4

	// preGrowCap bounds how far a client-declared Content-Length can pre-size
	// a body buffer, so a spoofed huge header can't force a large allocation
	// before any bytes are read.
	preGrowCap = 8 * 1024 * 1024
)

// contentLengthPtr returns r.ContentLength as *int64, or nil when unknown
// (chunked/streaming bodies report -1).
func contentLengthPtr(r *http.Request) *int64 {
	if r.ContentLength < 0 {
		return nil
	}

	return aws.Int64(r.ContentLength)
}

// bufferGrowHint clamps a declared body size to preGrowCap, for pre-sizing a
// pooled buffer once instead of paying repeated doubling reallocations as
// io.Copy fills it (the pool only retains buffers up to 64KiB, so anything
// larger starts from zero capacity every request).
func bufferGrowHint(contentLength *int64) int {
	n := aws.ToInt64(contentLength)
	if n <= 0 {
		return 0
	}
	if n > preGrowCap {
		return preGrowCap
	}

	return int(n)
}

func CalculateChecksum(data []byte, algorithm string) string {
	var sum []byte

	switch strings.ToUpper(algorithm) {
	case ChecksumCRC32:
		c := crc32.ChecksumIEEE(data)
		sum = make([]byte, crc32Len)
		binary.BigEndian.PutUint32(sum, c)
	case ChecksumCRC32C:
		c := crc32.Checksum(data, crc32CastagnoliTable)
		sum = make([]byte, crc32Len)
		binary.BigEndian.PutUint32(sum, c)
	case ChecksumSHA1:
		//nolint:gosec // SHA1 supported as per S3 spec
		h := sha1.Sum(data)
		sum = h[:]
	case ChecksumSHA256:
		h := sha256.Sum256(data)
		sum = h[:]
	case ChecksumMD5:
		h := md5.Sum(data) //nolint:gosec // S3 checksum algorithm, not a security use of MD5
		sum = h[:]
	case ChecksumSHA512:
		h := sha512.Sum512(data)
		sum = h[:]
	default:
		return ""
	}

	return base64.StdEncoding.EncodeToString(sum)
}

// queryParam returns the first value of a query parameter without parsing an empty query.
func queryParam(r *http.Request, name string) string {
	if r.URL.RawQuery == "" {
		return ""
	}

	return r.URL.Query().Get(name)
}

// decodeContentMD5 returns ErrInvalidDigest unless header is base64 of a 16-byte MD5.
func decodeContentMD5(header string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(header)
	if err != nil || len(decoded) != md5.Size {
		return nil, ErrInvalidDigest
	}

	return decoded, nil
}

// contentMD5FromContext returns the decoded Content-MD5 stored by the handler, if any.
func contentMD5FromContext(ctx context.Context) ([]byte, error) {
	header, ok := ctx.Value(md5Key).(string)
	if !ok || header == "" {
		return nil, nil
	}

	return decodeContentMD5(header)
}

// withContentMD5 validates the Content-MD5 header format and stashes it on ctx for the backend.
func withContentMD5(ctx context.Context, r *http.Request) (context.Context, error) {
	header := r.Header.Get("Content-MD5")
	if header == "" {
		return ctx, nil
	}

	if _, err := decodeContentMD5(header); err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, md5Key, header), nil
}

// verifyBodyContentMD5 checks Content-MD5 against the request body and restores the body.
func verifyBodyContentMD5(r *http.Request) error {
	header := r.Header.Get("Content-MD5")
	if header == "" {
		return nil
	}

	want, err := decodeContentMD5(header)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	r.Body = io.NopCloser(bytes.NewReader(body))

	if sum := md5.Sum(body); !bytes.Equal(sum[:], want) { //nolint:gosec // Content-MD5 is MD5 by spec
		return ErrBadChecksum
	}

	return nil
}
