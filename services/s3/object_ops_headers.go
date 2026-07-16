package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (h *S3Handler) setCommonHeaders(w http.ResponseWriter, out objectCommonDetails) {
	if out.ETag != nil {
		w.Header().Set("ETag", *out.ETag)
	}

	if out.LastModified != nil {
		w.Header().Set("Last-Modified", out.LastModified.Format(http.TimeFormat))
	}

	if out.ContentType != nil {
		w.Header().Set("Content-Type", *out.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	if out.ContentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*out.ContentLength, 10))
	}

	for k, v := range out.Metadata {
		w.Header().Set("X-Amz-Meta-"+k, v)
	}

	if out.VersionID != nil && *out.VersionID != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *out.VersionID)
	}

	// Advertise byte-range support. AWS returns x-amz-storage-class for every
	// object EXCEPT those in the STANDARD class, for which the header is omitted
	// (see HeadObject/GetObject output docs) — so a blank or STANDARD class
	// produces no header here.
	w.Header().Set("Accept-Ranges", "bytes")
	if sc := out.StorageClass; sc != "" && sc != storageStandard {
		w.Header().Set("X-Amz-Storage-Class", sc)
	}

	h.setChecksumHeaders(w, out)
}

func (h *S3Handler) setChecksumHeaders(w http.ResponseWriter, out objectCommonDetails) {
	var algo, val string

	switch {
	case out.ChecksumCRC32 != nil:
		algo, val = ChecksumCRC32, *out.ChecksumCRC32
	case out.ChecksumCRC32C != nil:
		algo, val = ChecksumCRC32C, *out.ChecksumCRC32C
	case out.ChecksumSHA1 != nil:
		algo, val = ChecksumSHA1, *out.ChecksumSHA1
	case out.ChecksumSHA256 != nil:
		algo, val = ChecksumSHA256, *out.ChecksumSHA256
	case out.ChecksumCRC64NVME != nil:
		algo, val = ChecksumCRC64NVME, *out.ChecksumCRC64NVME
	}

	if algo != "" {
		w.Header().Set("X-Amz-Checksum-"+algo, val)
		w.Header().Set("X-Amz-Checksum-Algorithm", algo)
	}
}

// setSSEHeaders writes SSE response headers based on stored object SSE info.
// validateSSECOnRead checks that a GET/HEAD request includes the required SSE-C
// headers when the stored object uses SSE-C, and that the supplied key-MD5 matches.
func validateSSECOnRead(r *http.Request, storedAlg, storedKeyMD5 string) error {
	if storedAlg == "" {
		return nil
	}

	if r.Header.Get(headerSSECAlgorithm) == "" || r.Header.Get(headerSSECKeyMD5) == "" {
		return ErrSSECRequired
	}

	suppliedMD5 := r.Header.Get(headerSSECKeyMD5)
	if storedKeyMD5 != "" && suppliedMD5 != storedKeyMD5 {
		return ErrBadChecksum
	}

	return nil
}

func setSSEHeaders(w http.ResponseWriter, out objectCommonDetails) {
	if out.SSEAlgorithm != "" {
		w.Header().Set(headerSSEAlgorithm, out.SSEAlgorithm)
	}

	if out.SSEKMSKeyID != "" {
		w.Header().Set(headerSSEKMSKeyID, out.SSEKMSKeyID)
	}

	if out.SSECAlgorithm != "" {
		w.Header().Set(headerSSECAlgorithm, out.SSECAlgorithm)
	}

	if out.SSECKeyMD5 != "" {
		w.Header().Set(headerSSECKeyMD5, out.SSECKeyMD5)
	}
}

func extractChecksumPointers(h http.Header, algo string) (*string, *string, *string, *string) {
	if algo == "" {
		return nil, nil, nil, nil
	}

	headerName := "X-Amz-Checksum-" + strings.ToLower(algo)
	checksum := h.Get(headerName)

	if checksum == "" {
		return nil, nil, nil, nil
	}

	switch algo {
	case ChecksumCRC32:
		return aws.String(checksum), nil, nil, nil
	case ChecksumCRC32C:
		return nil, aws.String(checksum), nil, nil
	case ChecksumSHA1:
		return nil, nil, aws.String(checksum), nil
	case ChecksumSHA256:
		return nil, nil, nil, aws.String(checksum)
	default:
		// CRC64NVME and future algorithms: not mapped to individual pointer fields.
		return nil, nil, nil, nil
	}
}

// extractCRC64NVMEChecksum reads the x-amz-checksum-crc64nvme header if present.
func extractCRC64NVMEChecksum(r *http.Request) *string {
	// Use the canonical header name per Go's net/http canonicalization.
	v := r.Header.Get("X-Amz-Checksum-Crc64nvme")
	if v == "" {
		return nil
	}

	return aws.String(v)
}

// tagSetToQueryString converts a tag set to the URL query-string format used by
// PutObject's Tagging field (e.g., "key1=val1&key2=val2").
func tagSetToQueryString(tags []types.Tag) string {
	v := url.Values{}
	for _, t := range tags {
		v.Set(aws.ToString(t.Key), aws.ToString(t.Value))
	}

	return v.Encode()
}

func (h *S3Handler) handleChecksumMode(
	w http.ResponseWriter,
	ver *s3.GetObjectOutput,
	details objectCommonDetails,
) {
	algo, val := h.getStoredChecksum(details)
	if algo == "" {
		data, _ := io.ReadAll(ver.Body)
		ver.Body = io.NopCloser(bytes.NewReader(data))

		algo = ChecksumCRC32
		val = CalculateChecksum(data, algo)
	}

	w.Header().Set("X-Amz-Checksum-Algorithm", algo)
	w.Header().Set("X-Amz-Checksum-"+algo, val)
}

func (h *S3Handler) getStoredChecksum(out objectCommonDetails) (string, string) {
	switch {
	case out.ChecksumCRC32 != nil:
		return ChecksumCRC32, *out.ChecksumCRC32
	case out.ChecksumCRC32C != nil:
		return ChecksumCRC32C, *out.ChecksumCRC32C
	case out.ChecksumSHA1 != nil:
		return ChecksumSHA1, *out.ChecksumSHA1
	case out.ChecksumSHA256 != nil:
		return ChecksumSHA256, *out.ChecksumSHA256
	case out.ChecksumCRC64NVME != nil:
		return ChecksumCRC64NVME, *out.ChecksumCRC64NVME
	default:
		return "", ""
	}
}

// setExpirationHeader evaluates lifecycle rules and sets the X-Amz-Expiration header.
func (h *S3Handler) setExpirationHeader(
	ctx context.Context,
	w http.ResponseWriter,
	bucketName, key string,
	lastModified *time.Time,
) {
	if h.janitor == nil {
		return
	}

	lcXML, lcErr := h.Backend.GetBucketLifecycleConfiguration(ctx, bucketName)
	if lcErr != nil || lcXML == "" {
		return
	}

	var objTags []types.Tag

	tagOut, tagErr := h.Backend.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if tagErr == nil {
		objTags = tagOut.TagSet
	}

	if exp := h.janitor.GetExpirationHeader(lcXML, key, objTags, aws.ToTime(lastModified)); exp != "" {
		w.Header().Set("X-Amz-Expiration", exp)
	}
}
