package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type objectCommonDetails struct {
	Metadata          map[string]string
	ETag              *string
	ContentType       *string
	ContentLength     *int64
	LastModified      *time.Time
	VersionID         *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumCRC64NVME *string
	SSEAlgorithm      string
	SSEKMSKeyID       string
	SSECAlgorithm     string
	SSECKeyMD5        string
}

func (h *S3Handler) handleObjectOperation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	switch r.Method {
	case http.MethodPut:
		h.routeObjectPut(ctx, w, r, bucket, key)
	case http.MethodGet:
		h.routeObjectGet(ctx, w, r, bucket, key)
	case http.MethodDelete:
		h.routeObjectDelete(ctx, w, r, bucket, key)
	case http.MethodPost:
		h.routeObjectPost(ctx, w, r, bucket, key)
	case http.MethodHead:
		h.headObject(ctx, w, r, bucket, key)
	case http.MethodOptions:
		h.handleCORSPreflight(ctx, w, r, bucket)
	default:
		WriteError(ctx, w, r, ErrMethodNotAllowed)
	}
}

func (h *S3Handler) routeObjectPut(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.putObjectTagging(ctx, w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		h.putObjectACL(ctx, w, r, bucket, key)
	case r.URL.Query().Has("partNumber") && r.URL.Query().Has("uploadId"):
		h.uploadPart(ctx, w, r, bucket, key)
	case r.URL.Query().Has("retention"):
		h.putObjectRetention(ctx, w, r, bucket, key)
	case r.URL.Query().Has("legal-hold"):
		h.putObjectLegalHold(ctx, w, r, bucket, key)
	case r.URL.Query().Has("rename"):
		h.handleRenameObject(ctx, w, r)
	case r.URL.Query().Has("encryption") && key != "":
		h.handleUpdateObjectEncryption(ctx, w, r)
	case r.Header.Get("X-Amz-Copy-Source") != "":
		h.copyObject(ctx, w, r, bucket, key)
	default:
		h.putObject(ctx, w, r, bucket, key)
	}
}

func (h *S3Handler) routeObjectGet(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.getObjectTagging(ctx, w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		h.getObjectACL(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.listParts(ctx, w, r, bucket, key)
	case r.URL.Query().Has("retention"):
		h.getObjectRetention(ctx, w, r, bucket, key)
	case r.URL.Query().Has("legal-hold"):
		h.getObjectLegalHold(ctx, w, r, bucket, key)
	case r.URL.Query().Has("attributes"):
		h.handleGetObjectAttributes(ctx, w, r)
	case r.URL.Query().Has("torrent"):
		h.handleGetObjectTorrent(ctx, w, r)
	default:
		h.getObject(ctx, w, r, bucket, key)
	}
}

func (h *S3Handler) routeObjectDelete(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.deleteObjectTagging(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.abortMultipartUpload(ctx, w, r, bucket, key)
	default:
		h.deleteObject(ctx, w, r, bucket, key)
	}
}

func (h *S3Handler) routeObjectPost(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	switch {
	case r.URL.Query().Has("uploads"):
		h.createMultipartUpload(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.completeMultipartUpload(ctx, w, r, bucket, key)
	case r.URL.Query().Has("restore"):
		h.handleRestoreObject(ctx, w, r)
	case r.URL.Query().Has("select"):
		h.selectObjectContent(ctx, w, r, bucket, key)
	default:
		WriteError(ctx, w, r, ErrMethodNotAllowed)
	}
}

func (h *S3Handler) headObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "HeadObject")

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	versionID := r.URL.Query().Get("versionId")
	var vid *string

	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	var nsb *types.NoSuchBucket
	var nsk *types.NoSuchKey
	if errors.As(err, &nsb) || errors.As(err, &nsk) ||
		errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrDeleteMarker) {
		w.Header().Set("X-Amz-Delete-Marker", "true")
		WriteError(ctx, w, r, ErrDeleteMarker)

		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	if sseErr := validateSSECOnRead(
		r, aws.ToString(out.SSECustomerAlgorithm), aws.ToString(out.SSECustomerKeyMD5),
	); sseErr != nil {
		WriteError(ctx, w, r, sseErr)

		return
	}

	if status, ok := checkConditionalHeaders(r, aws.ToString(out.ETag), aws.ToTime(out.LastModified)); !ok {
		w.WriteHeader(status)

		return
	}

	details := objectCommonDetails{
		Metadata:          out.Metadata,
		ETag:              out.ETag,
		ContentType:       out.ContentType,
		ContentLength:     out.ContentLength,
		LastModified:      out.LastModified,
		VersionID:         out.VersionId,
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
		SSEAlgorithm:      string(out.ServerSideEncryption),
		SSEKMSKeyID:       aws.ToString(out.SSEKMSKeyId),
		SSECAlgorithm:     aws.ToString(out.SSECustomerAlgorithm),
		SSECKeyMD5:        aws.ToString(out.SSECustomerKeyMD5),
	}

	h.setCommonHeaders(w, details)
	setSSEHeaders(w, details)

	// Set x-amz-expiration header if a lifecycle rule matches this object.
	h.setExpirationHeader(ctx, w, bucketName, key, out.LastModified)

	if ce := aws.ToString(out.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}

	if cd := aws.ToString(out.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	w.WriteHeader(http.StatusOK)
}

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
			go h.notifier.DispatchObjectCreated(h.notificationDispatchContext(), bucketName, key, etag, size, notifXML)
		}
	}

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
		ContentEncoding:    nilStringIfEmpty(r.Header.Get("Content-Encoding")),
		ContentDisposition: nilStringIfEmpty(r.Header.Get("Content-Disposition")),
		ChecksumAlgorithm:  types.ChecksumAlgorithm(algo),
		ChecksumCRC32:      crc32p,
		ChecksumCRC32C:     crc32cp,
		ChecksumSHA1:       sha1p,
		ChecksumSHA256:     sha256p,
		ChecksumCRC64NVME:  crc64nvmeP,
		Tagging:            aws.String(r.Header.Get("X-Amz-Tagging")),
	}
}

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
		Bucket:      aws.String(destBucket),
		Key:         aws.String(destKey),
		Body:        srcVer.Body,
		Metadata:    userMeta,
		ContentType: contentType,
	}

	if taggingReplace {
		putInput.Tagging = aws.String(tagging)
	} else {
		// COPY directive (default): preserve source tags on destination.
		srcBucket, srcKey, _, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
		if ok {
			if tagOut, tagErr := h.Backend.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
				Bucket: aws.String(srcBucket),
				Key:    aws.String(srcKey),
			}); tagErr == nil && len(tagOut.TagSet) > 0 {
				putInput.Tagging = aws.String(tagSetToQueryString(tagOut.TagSet))
			}
		}
	}

	destVer, err := h.Backend.PutObject(ctx, putInput)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if destVer.VersionId != nil && *destVer.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *destVer.VersionId)
	}

	etag := ""
	if destVer.ETag != nil {
		etag = *destVer.ETag
	}

	// Dispatch S3 notification if configured.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx,
			destBucket,
		); ncErr == nil && notifXML != "" {
			size := aws.ToInt64(destVer.Size)
			go h.notifier.DispatchObjectCopied(
				h.notificationDispatchContext(),
				destBucket,
				destKey,
				etag,
				size,
				notifXML,
			)
		}
	}

	httputils.WriteXML(ctx, w, http.StatusOK, CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *S3Handler) getObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObject")

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	versionID := r.URL.Query().Get("versionId")
	logger.Load(ctx).DebugContext(
		ctx,
		"S3 getObject input",
		"bucket",
		bucketName,
		"key",
		key,
		"versionId",
		versionID,
	)

	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	ver, err := h.Backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	defer ver.Body.Close()

	if sseErr := validateSSECOnRead(
		r, aws.ToString(ver.SSECustomerAlgorithm), aws.ToString(ver.SSECustomerKeyMD5),
	); sseErr != nil {
		WriteError(ctx, w, r, sseErr)

		return
	}

	if status, ok := checkConditionalHeaders(r, aws.ToString(ver.ETag), aws.ToTime(ver.LastModified)); !ok {
		w.WriteHeader(status)

		return
	}

	details := objectCommonDetails{
		Metadata:          ver.Metadata,
		ETag:              ver.ETag,
		ContentType:       ver.ContentType,
		ContentLength:     ver.ContentLength,
		LastModified:      ver.LastModified,
		VersionID:         ver.VersionId,
		ChecksumCRC32:     ver.ChecksumCRC32,
		ChecksumCRC32C:    ver.ChecksumCRC32C,
		ChecksumSHA1:      ver.ChecksumSHA1,
		ChecksumSHA256:    ver.ChecksumSHA256,
		ChecksumCRC64NVME: ver.ChecksumCRC64NVME,
		SSEAlgorithm:      string(ver.ServerSideEncryption),
		SSEKMSKeyID:       aws.ToString(ver.SSEKMSKeyId),
		SSECAlgorithm:     aws.ToString(ver.SSECustomerAlgorithm),
		SSECKeyMD5:        aws.ToString(ver.SSECustomerKeyMD5),
	}

	h.setGetObjectResponseHeaders(ctx, w, r, bucketName, key, ver, details)

	if served := h.serveObjectBody(ctx, w, r, ver); served {
		return
	}

	logger.Load(ctx).DebugContext(ctx,
		"S3 getObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"contentLength", aws.ToInt64(ver.ContentLength),
	)

	w.WriteHeader(http.StatusOK)

	if _, copyErr := io.Copy(w, ver.Body); copyErr != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to write object data", "error", copyErr)
	}
}

// setGetObjectResponseHeaders writes all response headers for GetObject.
func (h *S3Handler) setGetObjectResponseHeaders(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
	ver *s3.GetObjectOutput,
	details objectCommonDetails,
) {
	h.setCommonHeaders(w, details)
	setSSEHeaders(w, details)
	h.setExpirationHeader(ctx, w, bucketName, key, ver.LastModified)

	if ce := aws.ToString(ver.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}

	if cd := aws.ToString(ver.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	if r.Header.Get("X-Amz-Checksum-Mode") == "ENABLED" {
		h.handleChecksumMode(w, ver, details)
	}
}

// serveObjectBody handles range requests and writes the object body.
// Returns true if the response was fully handled (range served or error written).
func (h *S3Handler) serveObjectBody(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	ver *s3.GetObjectOutput,
) bool {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return false
	}

	data, readErr := io.ReadAll(ver.Body)
	if readErr != nil {
		WriteError(ctx, w, r, readErr)

		return true
	}

	if h.serveRange(ctx, w, data, rangeHeader) {
		return true
	}

	ver.Body = io.NopCloser(bytes.NewReader(data))

	return false
}

func (h *S3Handler) deleteObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObject")
	versionID := r.URL.Query().Get("versionId")
	logger.Load(ctx).DebugContext(
		ctx,
		"S3 deleteObject input",
		"bucket",
		bucketName,
		"key",
		key,
		"versionId",
		versionID,
	)

	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		WriteError(ctx, w, r, err)

		return
	}

	if errors.Is(err, ErrObjectLocked) || errors.Is(err, ErrInvalidObjectState) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if out.VersionId != nil && *out.VersionId != "" && *out.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *out.VersionId)
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		w.Header().Set("X-Amz-Delete-Marker", "true")
	}

	logger.Load(ctx).DebugContext(ctx,
		"S3 deleteObject output",
		"bucket", bucketName, "key", key, "deleteMarker", aws.ToBool(out.DeleteMarker),
	)

	// Dispatch S3 notification if configured.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx,
			bucketName,
		); ncErr == nil &&
			notifXML != "" {
			go h.notifier.DispatchObjectDeleted(h.notificationDispatchContext(), bucketName, key, notifXML)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) deleteObjects(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "DeleteObjects")
	var req DeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	if len(req.Objects) > maxDeleteObjects {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errInvalidArgument,
			Message: "You have attempted to delete more objects than allowed by the service's max-delete limit (1000).",
		}, http.StatusBadRequest)

		return
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: make([]types.ObjectIdentifier, 0, len(req.Objects)),
			Quiet:   aws.Bool(req.Quiet),
		},
	}

	for _, obj := range req.Objects {
		input.Delete.Objects = append(input.Delete.Objects, types.ObjectIdentifier{
			Key:       aws.String(obj.Key),
			VersionId: obj.VersionID,
		})
	}

	out, err := h.Backend.DeleteObjects(ctx, input)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := DeleteResult{
		Deleted: make([]DeletedXML, 0, len(out.Deleted)),
		Errors:  make([]DeleteErrorXML, 0, len(out.Errors)),
	}

	for _, d := range out.Deleted {
		if !req.Quiet {
			resp.Deleted = append(resp.Deleted, DeletedXML{
				Key:                   aws.ToString(d.Key),
				VersionID:             d.VersionId,
				DeleteMarker:          aws.ToBool(d.DeleteMarker),
				DeleteMarkerVersionID: d.DeleteMarkerVersionId,
			})
		}
	}

	for _, e := range out.Errors {
		resp.Errors = append(resp.Errors, DeleteErrorXML{
			Key:       aws.ToString(e.Key),
			Code:      aws.ToString(e.Code),
			Message:   aws.ToString(e.Message),
			VersionID: e.VersionId,
		})
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)

	// Dispatch S3 delete notifications for each successfully deleted object.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx,
			bucketName,
		); ncErr == nil && notifXML != "" {
			for _, d := range out.Deleted {
				key := aws.ToString(d.Key)
				go h.notifier.DispatchObjectDeleted(h.notificationDispatchContext(), bucketName, key, notifXML)
			}
		}
	}
}

func (h *S3Handler) putObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectTagging")
	var tagging Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	var tags []types.Tag
	for _, t := range tagging.TagSet.Tags {
		tags = append(tags, types.Tag{
			Key:   aws.String(t.Key),
			Value: aws.String(t.Value),
		})
	}

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	if _, err := h.Backend.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
		Tagging:   &types.Tagging{TagSet: tags},
	}); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectTagging")
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := Tagging{
		TagSet: TagSet{},
	}

	for _, t := range out.TagSet {
		if t.Key != nil && t.Value != nil {
			resp.TagSet.Tags = append(resp.TagSet.Tags, Tag{Key: *t.Key, Value: *t.Value})
		}
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) deleteObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObjectTagging")
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	if _, err := h.Backend.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	}); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// getObjectACL returns a minimal owner-full-control ACL for the requested object.
// Object ACLs are not enforced in this mock implementation; all objects are owned
// by the mock account and grant full control to the owner only.
func (h *S3Handler) getObjectACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectAcl")

	// Verify the object exists before returning an ACL.
	versionID := r.URL.Query().Get("versionId")

	// If a stored ACL exists for the version, return it verbatim. AWS returns
	// the persisted XML; if a canned ACL was set we still need to synthesise
	// XML below.
	stored, aclErr := h.Backend.GetObjectACL(ctx, bucketName, key, versionID)
	if aclErr != nil {
		WriteError(ctx, w, r, aclErr)

		return
	}

	if strings.HasPrefix(strings.TrimSpace(stored), "<") {
		// Stored value looks like XML — pass through.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(stored))

		return
	}

	const ownerID = "gopherstack-mock-owner"

	acp := AccessControlPolicy{
		Xmlns: xmlNamespaceS3,
		Owner: Owner{ID: ownerID, DisplayName: gopherstackName},
		ACL: AccessControlList{
			Grants: []Grant{
				{
					Grantee: Grantee{
						XmlnsXsi: "http://www.w3.org/2001/XMLSchema-instance",
						XsiType:  "CanonicalUser",
						ID:       ownerID,
					},
					Permission: "FULL_CONTROL",
				},
			},
		},
	}

	httputils.WriteXML(ctx, w, http.StatusOK, acp)
}

// putObjectACL persists the ACL (canned via x-amz-acl header or full XML body)
// against the targeted object version.
func (h *S3Handler) putObjectACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectAcl")

	versionID := r.URL.Query().Get("versionId")

	// Caller can use either a canned ACL header or supply an XML body. Persist
	// whichever we received; on read we'll synthesise default XML when empty.
	canned := r.Header.Get("x-amz-acl")

	body, _ := httputils.ReadBody(r)

	acl := canned
	if len(body) > 0 {
		acl = string(body)
	}

	if err := h.Backend.PutObjectACL(ctx, bucketName, key, versionID, acl); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

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

	// AWS always advertises byte-range support and STANDARD storage class.
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Amz-Storage-Class", storageStandard)

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

const copySourceMinParts = 2

// tagSetToQueryString converts a tag set to the URL query-string format used by
// PutObject's Tagging field (e.g., "key1=val1&key2=val2").
func tagSetToQueryString(tags []types.Tag) string {
	v := url.Values{}
	for _, t := range tags {
		v.Set(aws.ToString(t.Key), aws.ToString(t.Value))
	}

	return v.Encode()
}

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

func (h *S3Handler) serveRange(
	ctx context.Context,
	w http.ResponseWriter,
	data []byte,
	rangeHeader string,
) bool {
	total := int64(len(data))
	start, end, ok := parseRange(rangeHeader, total)

	if !ok {
		if !strings.HasPrefix(rangeHeader, "bytes=") {
			return false
		}

		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)

		return true
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)

	// #nosec G705
	if _, err := w.Write(data[start : end+1]); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to write range data", "error", err)
	}

	return true
}

// parseRange parses a "bytes=X-Y" Range header and returns clamped [start, end] indices.
func parseRange(header string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}

	const rangeSpecMaxParts = 2
	spec := strings.TrimSpace(strings.SplitN(header[len("bytes="):], ",", rangeSpecMaxParts)[0])
	startStr, endStr, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}

	var start, end int64
	switch {
	case startStr == "":
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		start = max(size-n, 0)
		end = size - 1
	case endStr == "":
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end = size - 1
	default:
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
	}

	if start > end || start >= size {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}

	return start, end, true
}

// checkConditionalHeaders evaluates HTTP conditional request headers per AWS/HTTP spec.
// Returns (304, false) or (412, false) if a condition fails, or (0, true) if all pass.
func checkConditionalHeaders(r *http.Request, etag string, lastModified time.Time) (int, bool) {
	// First evaluate 412-returning conditions (If-Match, If-Unmodified-Since).
	if status, ok := evaluatePreconditions(r.Header, standardConditionals, etag, lastModified); !ok {
		return status, false
	}

	// Then evaluate 304-returning conditions (If-None-Match, If-Modified-Since).
	stripQ := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQ(etag)

	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if stripQ(ifNoneMatch) == normalizedETag {
			return http.StatusNotModified, false
		}
	}

	if ifModSince := r.Header.Get("If-Modified-Since"); ifModSince != "" {
		if t, err := http.ParseTime(ifModSince); err == nil && !lastModified.After(t) {
			return http.StatusNotModified, false
		}
	}

	return 0, true
}

func (h *S3Handler) putObjectRetention(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectRetention")

	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var ret ObjectRetention
	if xmlErr := xml.NewDecoder(bytes.NewReader(body)).Decode(&ret); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: "The XML you provided was not well-formed",
		}, http.StatusBadRequest)

		return
	}

	retainUntil, parseErr := time.Parse(time.RFC3339, ret.RetainUntilDate)
	if parseErr != nil {
		// Try alternative format
		retainUntil, parseErr = time.Parse("2006-01-02T15:04:05.999Z", ret.RetainUntilDate)
		if parseErr != nil {
			httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
				Code:    errInvalidArgument,
				Message: "Invalid RetainUntilDate format",
			}, http.StatusBadRequest)

			return
		}
	}

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = &versionID
	}

	if putErr := h.Backend.PutObjectRetention(ctx, bucketName, key, vid, ret.Mode, retainUntil); putErr != nil {
		WriteError(ctx, w, r, putErr)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getObjectRetention(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectRetention")

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = &versionID
	}

	mode, retainUntil, err := h.Backend.GetObjectRetention(ctx, bucketName, key, vid)
	if errors.Is(err, ErrNoSuchKey) || errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}

	if errors.Is(err, ErrNoSuchObjectLockConfig) {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "NoSuchObjectLockConfiguration",
			Message: "The specified object does not have a ObjectLock configuration",
		}, http.StatusNotFound)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	ret := ObjectRetention{
		Xmlns:           xmlNamespaceS3,
		Mode:            mode,
		RetainUntilDate: retainUntil.UTC().Format(time.RFC3339),
	}

	httputils.WriteXML(ctx, w, http.StatusOK, ret)
}

func (h *S3Handler) putObjectLegalHold(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectLegalHold")

	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var lh ObjectLegalHold
	if xmlErr := xml.NewDecoder(bytes.NewReader(body)).Decode(&lh); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: "The XML you provided was not well-formed",
		}, http.StatusBadRequest)

		return
	}

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = &versionID
	}

	if putErr := h.Backend.PutObjectLegalHold(ctx, bucketName, key, vid, lh.Status); putErr != nil {
		WriteError(ctx, w, r, putErr)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getObjectLegalHold(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectLegalHold")

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = &versionID
	}

	status, err := h.Backend.GetObjectLegalHold(ctx, bucketName, key, vid)
	if errors.Is(err, ErrNoSuchKey) || errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	lh := ObjectLegalHold{
		Xmlns:  xmlNamespaceS3,
		Status: status,
	}

	httputils.WriteXML(ctx, w, http.StatusOK, lh)
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
