package s3

import (
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"net/http"
	"net/url"
	"strings"
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
)

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
	default:
		return ""
	}

	return base64.StdEncoding.EncodeToString(sum)
}
