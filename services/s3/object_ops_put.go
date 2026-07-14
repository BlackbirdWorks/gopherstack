package s3

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// setPutObjectResponseHeaders sets ETag, version, and checksum headers on the response.
func (h *S3Handler) setPutObjectResponseHeaders(w http.ResponseWriter, ver *s3.PutObjectOutput) {
	w.Header().Set("ETag", *ver.ETag)
	details := objectCommonDetails{
		ETag:              ver.ETag,
		VersionID:         ver.VersionId,
		ChecksumCRC32:     ver.ChecksumCRC32,
		ChecksumCRC32C:    ver.ChecksumCRC32C,
		ChecksumSHA1:      ver.ChecksumSHA1,
		ChecksumSHA256:    ver.ChecksumSHA256,
		ChecksumCRC64NVME: ver.ChecksumCRC64NVME,
	}
	h.setChecksumHeaders(w, details)
	if ver.VersionId != nil && *ver.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *ver.VersionId)
	}
}

func (h *S3Handler) putObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObject")

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err := h.authorizeObjectAccess(ctx, r, bucketName, key, actionPutObject); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Strip aws-chunked / STREAMING-* framing so the stored payload (and its
	// ETag) is the real object bytes, not the chunk-signature envelope.
	r = maybeDecodeChunkedBody(r)

	// Reject invalid tag sets (>10 tags, over-long key/value) before writing.
	if err := validateTaggingHeader(r.Header.Get("X-Amz-Tagging")); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Conditional PUT: AWS S3 supports If-Match and If-None-Match on PutObject.
	// `If-None-Match: *` is the canonical "create only if absent" pattern used by
	// S3-based distributed locks; If-Match enforces ETag-based optimistic updates.
	if err := h.enforcePutObjectPreconditions(ctx, r, bucketName, key); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Extract and validate SSE-* headers.
	sse, sseErr := extractSSEInfo(r)
	if sseErr != nil {
		WriteError(ctx, w, r, sseErr)

		return
	}

	ctx = context.WithValue(ctx, sseKey, sse)

	logger.Load(ctx).DebugContext(ctx, "S3 putObject input",
		"bucket", bucketName, "key", key, "contentType", r.Header.Get("Content-Type"))

	if md5Header := r.Header.Get("Content-MD5"); md5Header != "" {
		ctx = context.WithValue(ctx, md5Key, md5Header)
	}

	algo, crc32p, crc32cp, sha1p, sha256p := extractAlgoAndChecksums(r)
	crc64nvmeP := extractCRC64NVMEChecksum(r)

	// We pass r.Body directly to the backend to avoid an intermediate buffer in the handler.
	// The backend computes ETag/checksums while reading.
	ver, err := h.Backend.PutObject(ctx, buildPutObjectInput(r, bucketName, key, r.Body,
		algo, crc32p, crc32cp, sha1p, sha256p, crc64nvmeP, parseUserMetadata(r.Header)))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	h.setPutObjectResponseHeaders(w, ver)
	setSSEResponseHeaders(w, sse)

	logger.Load(ctx).DebugContext(ctx, "S3 putObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"versionId", aws.ToString(ver.VersionId))

	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx, bucketName,
		); ncErr == nil && notifXML != "" {
			etag := aws.ToString(ver.ETag)
			size := aws.ToInt64(ver.Size)
			go h.notifier.DispatchObjectCreated(
				h.notificationDispatchContext(),
				bucketName,
				key,
				etag,
				size,
				notifXML,
			)
		}
	}

	h.dispatchAccessLog(
		ctx,
		r,
		bucketName,
		"REST.PUT.OBJECT",
		key,
		http.StatusOK,
		aws.ToInt64(ver.Size),
	)

	w.WriteHeader(http.StatusOK)
}

// extractAlgoAndChecksums reads the checksum algorithm and individual checksum
// headers from the request.
func extractAlgoAndChecksums(r *http.Request) (string, *string, *string, *string, *string) {
	algo := strings.ToUpper(r.Header.Get("X-Amz-Checksum-Algorithm"))
	if algo == "" {
		algo = strings.ToUpper(r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"))
	}

	crc32, crc32c, sha1, sha256 := extractChecksumPointers(r.Header, algo)

	return algo, crc32, crc32c, sha1, sha256
}

// buildPutObjectInput assembles an s3.PutObjectInput from the HTTP request fields.
func buildPutObjectInput(
	r *http.Request,
	bucketName, key string,
	body io.Reader,
	algo string, crc32p, crc32cp, sha1p, sha256p, crc64nvmeP *string,
	userMeta map[string]string,
) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(key),
		Body:               body,
		Metadata:           userMeta,
		ContentType:        aws.String(r.Header.Get("Content-Type")),
		ContentEncoding:    ptrconv.NilIfEmpty(r.Header.Get("Content-Encoding")),
		ContentDisposition: ptrconv.NilIfEmpty(r.Header.Get("Content-Disposition")),
		StorageClass:       types.StorageClass(r.Header.Get("X-Amz-Storage-Class")),
		ChecksumAlgorithm:  types.ChecksumAlgorithm(algo),
		ChecksumCRC32:      crc32p,
		ChecksumCRC32C:     crc32cp,
		ChecksumSHA1:       sha1p,
		ChecksumSHA256:     sha256p,
		ChecksumCRC64NVME:  crc64nvmeP,
		Tagging:            aws.String(r.Header.Get("X-Amz-Tagging")),
	}
}
