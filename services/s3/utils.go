package s3

import (
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"net/http"
	"strings"
)

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
	case "CRC32":
		c := crc32.ChecksumIEEE(data)
		sum = make([]byte, crc32Len)
		binary.BigEndian.PutUint32(sum, c)
	case "CRC32C":
		c := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		sum = make([]byte, crc32Len)
		binary.BigEndian.PutUint32(sum, c)
	case "SHA1":
		//nolint:gosec // SHA1 supported as per S3 spec
		h := sha1.Sum(data)
		sum = h[:]
	case "SHA256":
		h := sha256.Sum256(data)
		sum = h[:]
	default:
		return ""
	}

	return base64.StdEncoding.EncodeToString(sum)
}
