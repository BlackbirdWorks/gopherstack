package ociobjectstorage

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"
)

const (
	algCRC32C = "CRC32C"
	algSHA256 = "SHA256"
	algSHA384 = "SHA384"

	hdrChecksumAlg = "opc-checksum-algorithm"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Checksum is the optional checksum stored next to the default MD5 of an object.
// Content is the whole-object value (always set for CRC32C, single-part uploads only for SHA);
// Multipart is "<b64(hash(concat part hashes))>-<parts>" for multipart SHA uploads.
type Checksum struct {
	Algorithm string `json:"algorithm,omitempty"`
	Content   string `json:"content,omitempty"`
	Multipart string `json:"multipart,omitempty"`
}

// ChecksumReq carries the opc-checksum-algorithm and opc-content-* request headers.
type ChecksumReq struct {
	Algorithm string
	CRC32C    string
	SHA256    string
	SHA384    string
}

func (c ChecksumReq) supplied() string {
	switch c.Algorithm {
	case algCRC32C:
		return c.CRC32C
	case algSHA256:
		return c.SHA256
	case algSHA384:
		return c.SHA384
	default:
		return ""
	}
}

// normalizeAlg canonicalises an opc-checksum-algorithm value; "" means none.
func normalizeAlg(s string) (string, error) {
	switch strings.ToUpper(s) {
	case "":
		return "", nil
	case algCRC32C:
		return algCRC32C, nil
	case algSHA256:
		return algSHA256, nil
	case algSHA384:
		return algSHA384, nil
	default:
		return "", errParam("opc-checksum-algorithm must be one of CRC32C, SHA256, SHA384")
	}
}

func digestRaw(alg string, data []byte) []byte {
	switch alg {
	case algCRC32C:
		v := crc32.Checksum(data, crc32cTable)

		return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	case algSHA256:
		s := sha256.Sum256(data)

		return s[:]
	default:
		s := sha512.Sum384(data)

		return s[:]
	}
}

func digestB64(alg string, data []byte) string {
	return base64.StdEncoding.EncodeToString(digestRaw(alg, data))
}

// compute derives the checksum of data and verifies a supplied opc-content-* header against it.
func (c ChecksumReq) compute(data []byte) (string, error) {
	if c.Algorithm == "" {
		return "", nil
	}

	sum := digestB64(c.Algorithm, data)
	if got := c.supplied(); got != "" && got != sum {
		return "", mismatch(c.Algorithm, sum, got)
	}

	return sum, nil
}

func mismatch(alg, actual, header string) *APIError {
	return newErr(http.StatusBadRequest, "InvalidDigest",
		"The computed "+alg+" of the request body ("+actual+") does not match the opc-content-"+
			strings.ToLower(alg)+" header ("+header+")")
}

// forObject builds the stored checksum of a single-part object.
func (c ChecksumReq) forObject(data []byte) (Checksum, error) {
	sum, err := c.compute(data)
	if err != nil || sum == "" {
		return Checksum{}, err
	}

	return Checksum{Algorithm: c.Algorithm, Content: sum}, nil
}

// assembleChecksum combines per-part checksums of a multipart upload.
func assembleChecksum(alg string, parts []Part, data []byte) Checksum {
	if alg == "" {
		return Checksum{}
	}

	if alg == algCRC32C {
		return Checksum{Algorithm: alg, Content: digestB64(alg, data)}
	}

	var concat []byte

	for _, p := range parts {
		raw, _ := base64.StdEncoding.DecodeString(p.Checksum)
		concat = append(concat, raw...)
	}

	return Checksum{Algorithm: alg, Multipart: digestB64(alg, concat) + "-" + strconv.Itoa(len(parts))}
}

// headers returns the response headers for a stored checksum.
func (c Checksum) headers() map[string]string {
	if c.Algorithm == "" {
		return nil
	}

	lower := strings.ToLower(c.Algorithm)
	out := map[string]string{}

	if c.Content != "" {
		out["opc-content-"+lower] = c.Content
	}

	if c.Multipart != "" {
		out["opc-multipart-"+lower] = c.Multipart
	}

	return out
}
