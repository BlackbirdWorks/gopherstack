package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
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
		ChecksumMD5:       ver.ChecksumMD5,
		ChecksumSHA512:    ver.ChecksumSHA512,
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
	extra := extractExtraChecksums(r)
	algo = extra.algoOrInferred(algo)

	in := buildPutObjectInput(r, bucketName, key, r.Body,
		algo, crc32p, crc32cp, sha1p, sha256p, extra, parseUserMetadata(r.Header))

	appended, appendErr := h.applyWriteOffset(ctx, r, in)
	if appendErr != nil {
		WriteError(ctx, w, r, appendErr)

		return
	}

	ver, err := h.Backend.PutObject(ctx, in)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	h.setPutObjectResponseHeaders(w, ver)

	if appended {
		w.Header().Set("X-Amz-Object-Size", strconv.FormatInt(aws.ToInt64(ver.Size), 10))
	}
	setSSEResponseHeaders(w, sse)

	logger.Load(ctx).DebugContext(ctx, "S3 putObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"versionId", aws.ToString(ver.VersionId))

	h.notifyObjectCreated(ctx, bucketName, key, ver)

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

func (h *S3Handler) notifyObjectCreated(ctx context.Context, bucketName, key string, ver *s3.PutObjectOutput) {
	if h.notifier == nil {
		return
	}

	notifXML, err := h.Backend.GetBucketNotificationConfiguration(ctx, bucketName)
	if err != nil || notifXML == "" {
		return
	}

	go h.notifier.DispatchObjectCreated(
		h.notificationDispatchContext(),
		bucketName,
		key,
		aws.ToString(ver.ETag),
		aws.ToInt64(ver.Size),
		notifXML,
	)
}

func isDirectoryBucketName(name string) bool { return strings.HasSuffix(name, "--x-s3") }

// applyWriteOffset turns a directory-bucket PutObject carrying
// x-amz-write-offset-bytes into an append: the body becomes existing+new bytes
// and client checksums (which cover only the appended part) are dropped so the
// server computes them over the whole object.
func (h *S3Handler) applyWriteOffset(ctx context.Context, r *http.Request, in *s3.PutObjectInput) (bool, error) {
	offsetHdr := r.Header.Get("X-Amz-Write-Offset-Bytes")
	if offsetHdr == "" || !isDirectoryBucketName(aws.ToString(in.Bucket)) {
		return false, nil
	}

	existing, err := h.existingBodyForAppend(ctx, offsetHdr, aws.ToString(in.Bucket), aws.ToString(in.Key))
	if err != nil {
		return false, err
	}

	in.Body = io.MultiReader(bytes.NewReader(existing), r.Body)
	in.ContentLength = nil
	in.ChecksumCRC32, in.ChecksumCRC32C, in.ChecksumSHA1, in.ChecksumSHA256, in.ChecksumCRC64NVME = nil, nil, nil, nil, nil
	in.ChecksumMD5, in.ChecksumSHA512 = nil, nil

	return true, nil
}

// existingBodyForAppend returns the current object bytes after checking the
// x-amz-write-offset-bytes value equals the object's size (0 when absent).
func (h *S3Handler) existingBodyForAppend(
	ctx context.Context, offsetHdr, bucketName, key string,
) ([]byte, error) {
	offset, err := strconv.ParseInt(offsetHdr, 10, 64)
	if err != nil || offset < 0 {
		return nil, ErrInvalidArgument
	}

	out, getErr := h.Backend.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String(key)})

	var existing []byte

	switch {
	case errors.Is(getErr, ErrNoSuchKey):
	case getErr != nil:
		return nil, getErr
	default:
		defer out.Body.Close()

		if existing, err = io.ReadAll(out.Body); err != nil {
			return nil, err
		}
	}

	if int64(len(existing)) != offset {
		return nil, ErrInvalidWriteOffset
	}

	return existing, nil
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
	algo string, crc32p, crc32cp, sha1p, sha256p *string, extra extraChecksums,
	userMeta map[string]string,
) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(key),
		Body:               body,
		ContentLength:      contentLengthPtr(r),
		Metadata:           userMeta,
		ContentType:        aws.String(r.Header.Get("Content-Type")),
		ContentEncoding:    ptrconv.NilIfEmpty(r.Header.Get("Content-Encoding")),
		ContentDisposition: ptrconv.NilIfEmpty(r.Header.Get("Content-Disposition")),
		CacheControl:       ptrconv.NilIfEmpty(r.Header.Get("Cache-Control")),
		ContentLanguage:    ptrconv.NilIfEmpty(r.Header.Get("Content-Language")),
		WebsiteRedirectLocation: ptrconv.NilIfEmpty(
			r.Header.Get("X-Amz-Website-Redirect-Location"),
		),
		Expires:           parseExpiresHeader(r),
		StorageClass:      types.StorageClass(r.Header.Get("X-Amz-Storage-Class")),
		ACL:               types.ObjectCannedACL(r.Header.Get("X-Amz-Acl")),
		ChecksumAlgorithm: types.ChecksumAlgorithm(algo),
		ChecksumCRC32:     crc32p,
		ChecksumCRC32C:    crc32cp,
		ChecksumSHA1:      sha1p,
		ChecksumSHA256:    sha256p,
		ChecksumCRC64NVME: extra.crc64nvme,
		ChecksumMD5:       extra.md5,
		ChecksumSHA512:    extra.sha512,
		Tagging:           aws.String(r.Header.Get("X-Amz-Tagging")),
	}
}
