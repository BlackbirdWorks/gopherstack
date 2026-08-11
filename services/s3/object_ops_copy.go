package s3

import (
	"context"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// copySourceData reads source object metadata for CopyObject/UploadPartCopy.
// When the source object is SSE-C encrypted, the caller must supply the
// x-amz-copy-source-server-side-encryption-customer-* headers so the backend
// can decrypt it; without them, decryptVersionForGet would otherwise hand
// back the raw ciphertext, silently corrupting the copy.
func (h *S3Handler) copySourceData(
	ctx context.Context, r *http.Request,
) (*s3.GetObjectOutput, error) {
	srcBucket, srcKey, srcVersionID, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		return nil, ErrInvalidArgument
	}

	if hID := r.Header.Get("X-Amz-Copy-Source-Version-Id"); hID != "" {
		srcVersionID = hID
	}

	var vid *string
	if srcVersionID != "" {
		vid = aws.String(srcVersionID)
	}

	copySourceSSE, sseErr := extractCopySourceSSECInfo(r)
	if sseErr != nil {
		return nil, sseErr
	}
	ctx = context.WithValue(ctx, sseKey, copySourceSSE)

	srcVer, err := h.Backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(srcBucket),
		Key:       aws.String(srcKey),
		VersionId: vid,
	})
	if err != nil {
		return nil, err
	}

	// The backend always echoes the source's stored SSE-C algorithm/key-MD5
	// on srcVer regardless of whether it could decrypt (see
	// decryptVersionForGet), so this check runs after the fetch. Reject
	// rather than silently propagate ciphertext when the source is SSE-C
	// protected and the copy-source key was missing/wrong.
	if validateErr := validateCopySourceSSECOnRead(
		r, aws.ToString(srcVer.SSECustomerAlgorithm), aws.ToString(srcVer.SSECustomerKeyMD5),
	); validateErr != nil {
		_ = srcVer.Body.Close()

		return nil, validateErr
	}

	return srcVer, nil
}

func (h *S3Handler) dispatchCopyNotification(
	ctx context.Context,
	bucket, key, etag string,
	size int64,
) {
	if h.notifier == nil {
		return
	}
	notifXML, err := h.Backend.GetBucketNotificationConfiguration(ctx, bucket)
	if err != nil || notifXML == "" {
		return
	}
	go h.notifier.DispatchObjectCopied(
		h.notificationDispatchContext(),
		bucket,
		key,
		etag,
		size,
		notifXML,
	)
}

func (h *S3Handler) copyObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	destBucket, destKey string,
) {
	h.setOperation(ctx, "CopyObject")

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Destination SSE: CopyObject can specify encryption for the NEW object
	// independently of the source's, via the plain (non copy-source-prefixed)
	// X-Amz-Server-Side-Encryption* headers. Stash it on ctx now so
	// Backend.PutObject below encrypts with it; copySourceData derives its own
	// context for the copy-source SSE-C key and doesn't disturb this value.
	destSSE, destSSEErr := extractSSEInfo(r)
	if destSSEErr != nil {
		WriteError(ctx, w, r, destSSEErr)

		return
	}
	ctx = context.WithValue(ctx, sseKey, destSSE)

	// AWS rejects copying an object onto itself unless some attribute changes.
	if srcB, srcK, _, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source")); ok &&
		srcB == destBucket && srcK == destKey && !copyChangesAttributes(r) {
		WriteError(ctx, w, r, ErrCopySelfNoChange)

		return
	}

	// Reject invalid replacement tag sets before copying.
	if tagging, replace := buildCopyTagging(r); replace {
		if err := validateTaggingHeader(tagging); err != nil {
			WriteError(ctx, w, r, err)

			return
		}
	}

	srcVer, err := h.copySourceData(ctx, r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	defer srcVer.Body.Close()

	// Evaluate x-amz-copy-source-if-* conditionals against the source object.
	if status, ok := checkCopySourceConditionals(r, aws.ToString(srcVer.ETag), aws.ToTime(srcVer.LastModified)); !ok {
		w.WriteHeader(status)

		return
	}

	userMeta, contentType := buildCopyMetadata(r, srcVer.Metadata, srcVer.ContentType)
	tagging, taggingReplace := buildCopyTagging(r)

	logger.Load(ctx).DebugContext(ctx, "CopyObject source info",
		"srcContentType", aws.ToString(contentType),
		"metadataDirective", r.Header.Get("X-Amz-Metadata-Directive"),
		"taggingDirective", r.Header.Get("X-Amz-Tagging-Directive"))

	putInput := &s3.PutObjectInput{
		Bucket:            aws.String(destBucket),
		Key:               aws.String(destKey),
		Body:              srcVer.Body,
		Metadata:          userMeta,
		ContentType:       contentType,
		StorageClass:      types.StorageClass(r.Header.Get("X-Amz-Storage-Class")),
		ChecksumAlgorithm: copyChecksumAlgorithm(r, srcVer),
	}
	h.resolveCopyTagging(ctx, r, putInput, tagging, taggingReplace)

	destVer, err := h.Backend.PutObject(ctx, putInput)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	setSSEResponseHeaders(w, destSSE)
	h.writeCopyResponse(ctx, w, destBucket, destKey, srcVer, destVer)
}

// resolveCopyTagging sets the destination tagging on putInput. When the request
// uses the REPLACE directive the supplied tagging is applied; otherwise (COPY
// directive, the default) the source object's tags are preserved.
func (h *S3Handler) resolveCopyTagging(
	ctx context.Context,
	r *http.Request,
	putInput *s3.PutObjectInput,
	tagging string,
	taggingReplace bool,
) {
	if taggingReplace {
		putInput.Tagging = aws.String(tagging)

		return
	}

	srcBucket, srcKey, _, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		return
	}

	tagOut, tagErr := h.Backend.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(srcKey),
	})
	if tagErr == nil && len(tagOut.TagSet) > 0 {
		putInput.Tagging = aws.String(tagSetToQueryString(tagOut.TagSet))
	}
}

// writeCopyResponse emits version headers, dispatches the copy notification, and
// renders the CopyObjectResult body for a successful CopyObject.
func (h *S3Handler) writeCopyResponse(
	ctx context.Context,
	w http.ResponseWriter,
	destBucket, destKey string,
	srcVer *s3.GetObjectOutput,
	destVer *s3.PutObjectOutput,
) {
	if destVer.VersionId != nil && *destVer.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *destVer.VersionId)
	}

	// Echo the source version ID when copying from a versioned object, matching
	// AWS S3 behaviour: x-amz-copy-source-version-id is always returned when the
	// source has a real (non-null) version ID.
	if srcVer.VersionId != nil && *srcVer.VersionId != "" && *srcVer.VersionId != NullVersion {
		w.Header().Set("X-Amz-Copy-Source-Version-Id", *srcVer.VersionId)
	}

	etag := ""
	if destVer.ETag != nil {
		etag = *destVer.ETag
	}

	h.dispatchCopyNotification(ctx, destBucket, destKey, etag, aws.ToInt64(destVer.Size))

	// Read back the destination object's actual stored Last-Modified rather
	// than independently computing "now" a second time: PutObject already
	// stamped its own timestamp when it wrote the version, and a fresh
	// time.Now() here could disagree with what a subsequent HeadObject/
	// GetObject on the same key reports.
	lastModified := time.Now().UTC()
	if head, headErr := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(destKey),
	}); headErr == nil && head.LastModified != nil {
		lastModified = *head.LastModified
	}

	// The new (destination) object is subject to the destination bucket's own
	// lifecycle rules, exactly like a fresh PutObject — set X-Amz-Expiration
	// the same way putObject does.
	h.setExpirationHeader(ctx, w, destBucket, destKey, &lastModified)

	httputils.WriteXML(ctx, w, http.StatusOK, CopyObjectResult{
		ETag:              etag,
		LastModified:      lastModified.UTC().Format(time.RFC3339),
		ChecksumCRC32:     aws.ToString(destVer.ChecksumCRC32),
		ChecksumCRC32C:    aws.ToString(destVer.ChecksumCRC32C),
		ChecksumCRC64NVME: aws.ToString(destVer.ChecksumCRC64NVME),
		ChecksumSHA1:      aws.ToString(destVer.ChecksumSHA1),
		ChecksumSHA256:    aws.ToString(destVer.ChecksumSHA256),
	})
}

// copyChecksumAlgorithm decides which checksum algorithm (if any) the
// destination object's PutObject should compute. Real S3 CopyObject recomputes
// one when the request explicitly names an algorithm via
// x-amz-checksum-algorithm, or -- when none is requested -- carries forward the
// source's own checksum algorithm if it had one. With neither, the destination
// gets no checksum, matching PutObject's opt-in behavior.
func copyChecksumAlgorithm(r *http.Request, srcVer *s3.GetObjectOutput) types.ChecksumAlgorithm {
	if algo := r.Header.Get("X-Amz-Checksum-Algorithm"); algo != "" {
		return types.ChecksumAlgorithm(strings.ToUpper(algo))
	}

	switch {
	case srcVer.ChecksumCRC32 != nil:
		return types.ChecksumAlgorithmCrc32
	case srcVer.ChecksumCRC32C != nil:
		return types.ChecksumAlgorithmCrc32c
	case srcVer.ChecksumSHA1 != nil:
		return types.ChecksumAlgorithmSha1
	case srcVer.ChecksumSHA256 != nil:
		return types.ChecksumAlgorithmSha256
	case srcVer.ChecksumCRC64NVME != nil:
		return types.ChecksumAlgorithmCrc64nvme
	default:
		return ""
	}
}

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

// handleRenameObject handles PUT /{bucket}/{key}?rename.
// AWS S3 sends the rename target via the x-amz-rename-source header (the
// existing source key) and uses the request URL path as the destination key.
// To match common usage we accept both forms: x-amz-rename-source as source,
// or X-Amz-Copy-Source for compatibility.
func (h *S3Handler) handleRenameObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "RenameObject")

	bucket, targetKey, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || targetKey == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	source := r.Header.Get("X-Amz-Rename-Source")
	if source == "" {
		source = r.Header.Get("X-Amz-Copy-Source")
	}

	source = strings.TrimPrefix(source, "/")

	// "bucket/key" or just "key" forms supported.
	srcKey := source
	if rest, hasPrefix := strings.CutPrefix(source, bucket+"/"); hasPrefix {
		srcKey = rest
	}

	if srcKey == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	if err := h.Backend.RenameObject(ctx, bucket, srcKey, targetKey); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

const copySourceMinParts = 2

func parseCopySource(src string) (string, string, string, bool) {
	src = strings.TrimPrefix(src, "/")
	parts := strings.SplitN(src, "/", copySourceMinParts)

	if len(parts) != copySourceMinParts {
		return "", "", "", false
	}

	bucket := parts[0]
	key := parts[1]
	versionID := ""

	if idx := strings.Index(key, "?versionId="); idx != -1 {
		versionID = key[idx+11:]
		key = key[:idx]
	}

	// Unescape the key since it may be URL-encoded from the client.
	unescapedKey, err := url.PathUnescape(key)
	if err == nil {
		key = unescapedKey
	}

	return bucket, key, versionID, true
}
