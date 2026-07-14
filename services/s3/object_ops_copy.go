package s3

import (
	"context"
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

// copySourceData reads source object metadata for CopyObject.
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

	srcVer, err := h.Backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(srcBucket),
		Key:       aws.String(srcKey),
		VersionId: vid,
	})
	if err != nil {
		return nil, err
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
		Bucket:       aws.String(destBucket),
		Key:          aws.String(destKey),
		Body:         srcVer.Body,
		Metadata:     userMeta,
		ContentType:  contentType,
		StorageClass: types.StorageClass(r.Header.Get("X-Amz-Storage-Class")),
	}
	h.resolveCopyTagging(ctx, r, putInput, tagging, taggingReplace)

	destVer, err := h.Backend.PutObject(ctx, putInput)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

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

	httputils.WriteXML(ctx, w, http.StatusOK, CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().UTC().Format(time.RFC3339),
	})
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
