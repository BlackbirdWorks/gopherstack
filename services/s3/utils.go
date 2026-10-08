package s3

import (
	"crypto/md5"  //nolint:gosec // S3 checksum algorithm, not a security use of MD5
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
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
