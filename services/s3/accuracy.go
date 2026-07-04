// Package s3 — accuracy.go implements AWS-accuracy improvements per issue #1676:
// SSE-* header handling, CopyObject conditional headers, ListParts pagination,
// multipart 5 MiB minimum enforcement, CRC64NVME checksum, x-amz-expected-bucket-owner,
// CopyObject metadata/tagging directives, GetObject response-content-* overrides,
// CopyObject x-amz-copy-source-version-id, CreateMultipartUpload SSE propagation,
// MalformedXML for CompleteMultipartUpload, object key/bucket name validation,
// Content-MD5 validation, and ListObjectsV2 URL encoding.
package s3

import (
	"crypto/md5" //nolint:gosec // G501: MD5 is required by the SSE-C key validation specification
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc64"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// ─── Constants ───────────────────────────────────────────────────────────────

const (
	// ChecksumCRC64NVME is the algorithm name for CRC64/NVME checksums.
	ChecksumCRC64NVME = "CRC64NVME"

	// headerSSEAlgorithm is the request header that names the SSE algorithm.
	headerSSEAlgorithm = "X-Amz-Server-Side-Encryption"
	// headerSSEKMSKeyID is the request header for the KMS master key ID.
	headerSSEKMSKeyID = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	// headerSSECAlgorithm is the request header for SSE-C customer algorithm.
	headerSSECAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"
	// headerSSECKey is the request header for the base64-encoded SSE-C customer key.
	headerSSECKey = "X-Amz-Server-Side-Encryption-Customer-Key"
	// headerSSECKeyMD5 is the request header for the MD5 of the SSE-C customer key.
	headerSSECKeyMD5 = "X-Amz-Server-Side-Encryption-Customer-Key-Md5"

	// headerExpectedBucketOwner is the request header that asserts bucket ownership.
	headerExpectedBucketOwner = "X-Amz-Expected-Bucket-Owner"

	// mockAccountID is the account ID used by the mock for ownership checks.
	mockAccountID = "123456789012"

	// multipartMinPartSize is the AWS minimum non-last part size for multipart uploads.
	multipartMinPartSize = 5 * 1024 * 1024
)

// ─── SSE Info ────────────────────────────────────────────────────────────────

// sseInfo captures SSE parameters extracted from an HTTP request.
type sseInfo struct {
	// Algorithm is one of "AES256", "aws:kms", "aws:kms:dsse", or "" (none).
	Algorithm string
	// KMSKeyID is the KMS key ID, populated when Algorithm is aws:kms/dsse.
	KMSKeyID string
	// SSECAlgorithm is "AES256" when SSE-C is requested.
	SSECAlgorithm string
	// SSECKeyMD5 is the base64-encoded MD5 of the customer-supplied key.
	SSECKeyMD5 string
	// SSECKeyB64 is the base64-encoded raw customer key. Kept on the
	// request-scoped sseInfo only — not persisted — so the backend can
	// encrypt the body on PUT and the GET handler can decrypt when the
	// caller re-supplies it.
	SSECKeyB64 string
}

// extractSSEInfo reads SSE-* request headers and validates SSE-C when present.
// Returns an error when the SSE-C key MD5 does not match the supplied key.
func extractSSEInfo(r *http.Request) (sseInfo, error) {
	info := sseInfo{
		Algorithm:     r.Header.Get(headerSSEAlgorithm),
		KMSKeyID:      r.Header.Get(headerSSEKMSKeyID),
		SSECAlgorithm: r.Header.Get(headerSSECAlgorithm),
		SSECKeyMD5:    r.Header.Get(headerSSECKeyMD5),
		SSECKeyB64:    r.Header.Get(headerSSECKey),
	}

	rawKey := r.Header.Get(headerSSECKey)
	if rawKey == "" {
		return info, nil
	}

	if err := validateSSECKey(rawKey, info.SSECKeyMD5); err != nil {
		return sseInfo{}, err
	}

	return info, nil
}

// validateSSECKey decodes the base64 key and verifies the supplied MD5.
func validateSSECKey(rawKeyB64, suppliedMD5B64 string) error {
	keyBytes, err := base64.StdEncoding.DecodeString(rawKeyB64)
	if err != nil {
		return ErrInvalidArgument
	}

	if suppliedMD5B64 == "" {
		return ErrInvalidArgument
	}

	sum := md5.Sum(keyBytes) //nolint:gosec // MD5 required by SSE-C specification
	computedMD5 := base64.StdEncoding.EncodeToString(sum[:])

	if computedMD5 != suppliedMD5B64 {
		return ErrBadChecksum
	}

	return nil
}

// setSSEResponseHeaders writes the appropriate SSE response headers.
func setSSEResponseHeaders(w http.ResponseWriter, info sseInfo) {
	if info.Algorithm != "" {
		w.Header().Set(headerSSEAlgorithm, info.Algorithm)
	}

	if info.KMSKeyID != "" {
		w.Header().Set(headerSSEKMSKeyID, info.KMSKeyID)
	}

	if info.SSECAlgorithm != "" {
		w.Header().Set(headerSSECAlgorithm, info.SSECAlgorithm)
	}

	if info.SSECKeyMD5 != "" {
		w.Header().Set(headerSSECKeyMD5, info.SSECKeyMD5)
	}
}

// ─── CopyObject conditionals ─────────────────────────────────────────────────

// copyConditionalParams holds the header name prefixes for conditional evaluation.
type copyConditionalParams struct {
	ifMatch           string
	ifUnmodifiedSince string
	ifNoneMatch       string
	ifModifiedSince   string
}

// standardConditionals are the standard HTTP/S3 conditional header names.
var standardConditionals = copyConditionalParams{ //nolint:gochecknoglobals // fixed constant set
	ifMatch:           "If-Match",
	ifUnmodifiedSince: "If-Unmodified-Since",
	ifNoneMatch:       "If-None-Match",
	ifModifiedSince:   "If-Modified-Since",
}

// copySourceConditionals are the x-amz-copy-source-if-* conditional header names.
var copySourceConditionals = copyConditionalParams{ //nolint:gochecknoglobals // fixed constant set
	ifMatch:           "X-Amz-Copy-Source-If-Match",
	ifUnmodifiedSince: "X-Amz-Copy-Source-If-Unmodified-Since",
	ifNoneMatch:       "X-Amz-Copy-Source-If-None-Match",
	ifModifiedSince:   "X-Amz-Copy-Source-If-Modified-Since",
}

// evaluatePreconditions checks only the 412-returning conditions (if-match and
// if-unmodified-since) from a set of conditional headers. Returns (412, false) on failure.
func evaluatePreconditions(
	h http.Header,
	params copyConditionalParams,
	etag string,
	lastModified time.Time,
) (int, bool) {
	stripQ := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQ(etag)

	// if-match: fail 412 when ETag does NOT match. "*" matches any existing
	// representation, so it always passes here (callers only evaluate this for
	// objects that exist).
	if v := h.Get(params.ifMatch); v != "" && v != "*" {
		if stripQ(v) != normalizedETag {
			return http.StatusPreconditionFailed, false
		}
	}

	// if-unmodified-since: fail 412 when modified after the given date.
	if v := h.Get(params.ifUnmodifiedSince); v != "" {
		if t, err := http.ParseTime(v); err == nil && lastModified.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}

	return 0, true
}

// evaluateCopySourceConditionals checks all four x-amz-copy-source-if-* headers.
// All failures return 412 (copy conditionals don't have a 304 variant).
func evaluateCopySourceConditionals(
	h http.Header,
	params copyConditionalParams,
	etag string,
	lastModified time.Time,
) (int, bool) {
	if status, ok := evaluatePreconditions(h, params, etag, lastModified); !ok {
		return status, false
	}

	stripQ := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQ(etag)

	// if-none-match: fail 412 when ETag matches (copy variant → 412 not 304).
	// "*" matches any existing representation.
	if v := h.Get(params.ifNoneMatch); v != "" {
		if v == "*" || stripQ(v) == normalizedETag {
			return http.StatusPreconditionFailed, false
		}
	}

	// if-modified-since: fail 412 when NOT modified after the given date (copy → 412).
	if v := h.Get(params.ifModifiedSince); v != "" {
		if t, err := http.ParseTime(v); err == nil && !lastModified.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}

	return 0, true
}

// checkCopySourceConditionals evaluates the four x-amz-copy-source-if-* headers
// against the source object's ETag and LastModified. Returns false and a 412
// status when a precondition fails.
func checkCopySourceConditionals(
	r *http.Request,
	srcETag string,
	srcLastModified time.Time,
) (int, bool) {
	return evaluateCopySourceConditionals(
		r.Header,
		copySourceConditionals,
		srcETag,
		srcLastModified,
	)
}

// ─── CopyObject metadata / tagging directives ────────────────────────────────

// buildCopyMetadata returns the metadata and content-type to use for the
// destination object, applying the x-amz-metadata-directive logic.
// On REPLACE: use metadata from the copy request headers.
// On COPY (default): preserve source metadata.
func buildCopyMetadata(
	r *http.Request,
	srcMetadata map[string]string,
	srcContentType *string,
) (map[string]string, *string) {
	if r.Header.Get("X-Amz-Metadata-Directive") != "REPLACE" {
		return maps.Clone(srcMetadata), srcContentType
	}

	ct := r.Header.Get("Content-Type")
	var destContentType *string
	if ct != "" && !strings.Contains(ct, "form-urlencoded") {
		destContentType = aws.String(ct)
	} else {
		destContentType = srcContentType
	}

	return parseUserMetadata(r.Header), destContentType
}

// buildCopyTagging returns the tagging string to apply to the destination.
// On REPLACE: use the x-amz-tagging header from the request.
// On COPY (default): return empty string so the source tags are preserved by caller.
func buildCopyTagging(r *http.Request) (string, bool) {
	directive := r.Header.Get("X-Amz-Tagging-Directive")
	if directive == "REPLACE" {
		return r.Header.Get("X-Amz-Tagging"), true
	}

	return "", false
}

// copyChangesAttributes reports whether a CopyObject request changes any object
// attribute. AWS only permits a self-copy (identical source and destination) when
// at least one attribute changes; otherwise it returns InvalidRequest.
func copyChangesAttributes(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get("X-Amz-Metadata-Directive"), "REPLACE") {
		return true
	}
	if strings.EqualFold(r.Header.Get("X-Amz-Tagging-Directive"), "REPLACE") {
		return true
	}

	for _, hdr := range []string{
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Storage-Class",
		"X-Amz-Website-Redirect-Location",
	} {
		if r.Header.Get(hdr) != "" {
			return true
		}
	}

	return false
}

// ─── x-amz-expected-bucket-owner ─────────────────────────────────────────────

// validateExpectedBucketOwner checks the x-amz-expected-bucket-owner header.
// Returns ErrAccessDenied (403) when the header is present and does not match
// the mock account ID.
func validateExpectedBucketOwner(r *http.Request) error {
	expected := r.Header.Get(headerExpectedBucketOwner)
	if expected == "" {
		return nil
	}

	if expected != mockAccountID {
		return ErrAccessDenied
	}

	return nil
}

// ─── CRC64NVME checksum ───────────────────────────────────────────────────────

// crc64NVMEPoly is the CRC-64/NVME polynomial (Rocksoft^tm model) reflected
// (reversed) for use with standard little-endian (right-shifting) algorithms.
const crc64NVMEPoly = uint64(0x9a6c9329ac4bc9b5)

// crc64NVMETable is the lookup table for the CRC64/NVME polynomial.
var crc64NVMETable = crc64.MakeTable(crc64NVMEPoly) //nolint:gochecknoglobals // pre-computed lookup table

// NewCRC64NVME returns a new CRC64/NVME hash.
func NewCRC64NVME() hash.Hash {
	return crc64.New(crc64NVMETable)
}

// CalculateCRC64NVME computes the base64-encoded CRC64/NVME checksum of data.
func CalculateCRC64NVME(data []byte) string {
	h := NewCRC64NVME()
	_, _ = h.Write(data)
	sum := h.Sum(nil)

	return base64.StdEncoding.EncodeToString(sum)
}

// checksumBytesToB64 converts a checksum hash's Sum bytes to base64 string,
// handling big-endian conversion for 32-bit hashes.
func checksumBytesToB64(h hash.Hash) string {
	if h32, ok := h.(interface{ Sum32() uint32 }); ok {
		const size = 4
		b := make([]byte, size)
		binary.BigEndian.PutUint32(b, h32.Sum32())

		return base64.StdEncoding.EncodeToString(b)
	}

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// ─── MalformedXML sentinel ────────────────────────────────────────────────────

// ErrMalformedXML is returned when an XML request body cannot be decoded.
// The error table maps it to HTTP 400 with code "MalformedXML".
var ErrMalformedXML = errors.New(errMalformedXML)
