package s3

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // MD5 required for Content-MD5 header validation per S3 spec
	"encoding/base64"
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
	Metadata       map[string]string
	ETag           *string
	ContentType    *string
	ContentLength  *int64
	LastModified   *time.Time
	VersionID      *string
	ChecksumCRC32  *string
	ChecksumCRC32C *string
	ChecksumSHA1   *string
	ChecksumSHA256 *string
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
		h.setOperation(ctx, "PutObjectAcl")
		w.WriteHeader(http.StatusOK) // ACLs ignored
	case r.URL.Query().Has("partNumber") && r.URL.Query().Has("uploadId"):
		h.uploadPart(ctx, w, r, bucket, key)
	case r.URL.Query().Has("retention"):
		h.putObjectRetention(ctx, w, r, bucket, key)
	case r.URL.Query().Has("legal-hold"):
		h.putObjectLegalHold(ctx, w, r, bucket, key)
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

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	if status, ok := checkConditionalHeaders(r, aws.ToString(out.ETag), aws.ToTime(out.LastModified)); !ok {
		w.WriteHeader(status)

		return
	}

	details := objectCommonDetails{
		Metadata:       out.Metadata,
		ETag:           out.ETag,
		ContentType:    out.ContentType,
		ContentLength:  out.ContentLength,
		LastModified:   out.LastModified,
		VersionID:      out.VersionId,
		ChecksumCRC32:  out.ChecksumCRC32,
		ChecksumCRC32C: out.ChecksumCRC32C,
		ChecksumSHA1:   out.ChecksumSHA1,
		ChecksumSHA256: out.ChecksumSHA256,
	}

	h.setCommonHeaders(w, details)

	if ce := aws.ToString(out.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}

	if cd := aws.ToString(out.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	w.WriteHeader(http.StatusOK)
}

// validateContentMD5 checks the Content-MD5 header against the data. Returns false and writes error if invalid.
func validateContentMD5(ctx context.Context, w http.ResponseWriter, r *http.Request, data []byte) bool {
	contentMD5Header := r.Header.Get("Content-MD5")
	if contentMD5Header == "" {
		return true
	}

	decoded, decErr := base64.StdEncoding.DecodeString(contentMD5Header)
	if decErr != nil || len(decoded) != md5.Size {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "BadDigest",
			Message: "The Content-MD5 you specified did not match what we received.",
		}, http.StatusBadRequest)

		return false
	}

	//nolint:gosec // MD5 required for Content-MD5 header validation per S3 spec
	computed := md5.Sum(data)
	if !bytes.Equal(computed[:], decoded) {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "BadDigest",
			Message: "The Content-MD5 you specified did not match what we received.",
		}, http.StatusBadRequest)

		return false
	}

	return true
}

// setPutObjectResponseHeaders sets ETag, version, and checksum headers on the response.
func (h *S3Handler) setPutObjectResponseHeaders(w http.ResponseWriter, ver *s3.PutObjectOutput) {
	w.Header().Set("ETag", *ver.ETag)
	details := objectCommonDetails{
		ETag:           ver.ETag,
		VersionID:      ver.VersionId,
		ChecksumCRC32:  ver.ChecksumCRC32,
		ChecksumCRC32C: ver.ChecksumCRC32C,
		ChecksumSHA1:   ver.ChecksumSHA1,
		ChecksumSHA256: ver.ChecksumSHA256,
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
	logger.Load(ctx).DebugContext(ctx, "S3 putObject input",
		"bucket", bucketName, "key", key, "contentType", r.Header.Get("Content-Type"))

	data, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if !validateContentMD5(ctx, w, r, data) {
		return
	}

	algo, crc32p, crc32cp, sha1p, sha256p := extractAlgoAndChecksums(r)

	if err = verifyChecksumIfPresent(data, algo, crc32p, crc32cp, sha1p, sha256p); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	ver, err := h.Backend.PutObject(ctx, buildPutObjectInput(r, bucketName, key, data,
		algo, crc32p, crc32cp, sha1p, sha256p, parseUserMetadata(r.Header)))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	h.setPutObjectResponseHeaders(w, ver)

	logger.Load(ctx).DebugContext(ctx, "S3 putObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"versionId", aws.ToString(ver.VersionId))

	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx, bucketName,
		); ncErr == nil && notifXML != "" {
			etag := aws.ToString(ver.ETag)
			size := aws.ToInt64(ver.Size)
			go h.notifier.DispatchObjectCreated(context.WithoutCancel(ctx), bucketName, key, etag, size, notifXML)
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
	data []byte,
	algo string, crc32p, crc32cp, sha1p, sha256p *string,
	userMeta map[string]string,
) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(key),
		Body:               bytes.NewReader(data),
		Metadata:           userMeta,
		ContentType:        aws.String(r.Header.Get("Content-Type")),
		ContentEncoding:    nilStringIfEmpty(r.Header.Get("Content-Encoding")),
		ContentDisposition: nilStringIfEmpty(r.Header.Get("Content-Disposition")),
		ChecksumAlgorithm:  types.ChecksumAlgorithm(algo),
		ChecksumCRC32:      crc32p,
		ChecksumCRC32C:     crc32cp,
		ChecksumSHA1:       sha1p,
		ChecksumSHA256:     sha256p,
		Tagging:            aws.String(r.Header.Get("X-Amz-Tagging")),
	}
}

// copySourceData reads source object data for CopyObject.
func (h *S3Handler) copySourceData(
	ctx context.Context, r *http.Request,
) ([]byte, *s3.GetObjectOutput, error) {
	srcBucket, srcKey, srcVersionID, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		return nil, nil, ErrInvalidArgument
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
		return nil, nil, err
	}
	defer srcVer.Body.Close()

	data, err := io.ReadAll(srcVer.Body)
	if err != nil {
		return nil, nil, err
	}

	return data, srcVer, nil
}

func (h *S3Handler) copyObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	destBucket, destKey string,
) {
	h.setOperation(ctx, "CopyObject")

	data, srcVer, err := h.copySourceData(ctx, r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	userMeta := srcVer.Metadata
	contentType := srcVer.ContentType

	logger.Load(ctx).DebugContext(ctx, "CopyObject source info",
		"srcBucket", aws.ToString(srcVer.ContentType),
		"srcContentType", aws.ToString(contentType))

	if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
		ct := r.Header.Get("Content-Type")
		logger.Load(ctx).DebugContext(ctx, "Metadata directive REPLACE detected", "headerContentType", ct)
		if ct != "" && !strings.Contains(ct, "form-urlencoded") {
			contentType = aws.String(ct)
		}
		userMeta = parseUserMetadata(r.Header)
		logger.Load(ctx).DebugContext(ctx, "Metadata directive REPLACE applied",
			"newContentType", aws.ToString(contentType), "newUserMeta", userMeta)
	}

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(destBucket),
		Key:         aws.String(destKey),
		Body:        bytes.NewReader(data),
		Metadata:    userMeta,
		ContentType: contentType,
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
			go h.notifier.DispatchObjectCopied(context.WithoutCancel(ctx), destBucket, destKey, etag, size, notifXML)
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

	if status, ok := checkConditionalHeaders(r, aws.ToString(ver.ETag), aws.ToTime(ver.LastModified)); !ok {
		w.WriteHeader(status)

		return
	}

	details := objectCommonDetails{
		Metadata:       ver.Metadata,
		ETag:           ver.ETag,
		ContentType:    ver.ContentType,
		ContentLength:  ver.ContentLength,
		LastModified:   ver.LastModified,
		VersionID:      ver.VersionId,
		ChecksumCRC32:  ver.ChecksumCRC32,
		ChecksumCRC32C: ver.ChecksumCRC32C,
		ChecksumSHA1:   ver.ChecksumSHA1,
		ChecksumSHA256: ver.ChecksumSHA256,
	}

	h.setCommonHeaders(w, details)
	w.Header().Set("Accept-Ranges", "bytes")

	if ce := aws.ToString(ver.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}

	if cd := aws.ToString(ver.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	if r.Header.Get("X-Amz-Checksum-Mode") == "ENABLED" {
		h.handleChecksumMode(w, ver, details)
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		data, _ := io.ReadAll(ver.Body)
		if h.serveRange(ctx, w, data, rangeHeader) {
			return
		}
		ver.Body = io.NopCloser(bytes.NewReader(data))
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

	if errors.Is(err, ErrObjectLocked) {
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
			go h.notifier.DispatchObjectDeleted(context.WithoutCancel(ctx), bucketName, key, notifXML)
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
			Code:    "InvalidArgument",
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
				go h.notifier.DispatchObjectDeleted(context.WithoutCancel(ctx), bucketName, key, notifXML)
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
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	_, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
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

	const ownerID = "gopherstack-mock-owner"

	acp := AccessControlPolicy{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: Owner{ID: ownerID, DisplayName: "gopherstack"},
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

	h.setChecksumHeaders(w, out)
}

func (h *S3Handler) setChecksumHeaders(w http.ResponseWriter, out objectCommonDetails) {
	var algo, val string

	switch {
	case out.ChecksumCRC32 != nil:
		algo, val = checksumCRC32, *out.ChecksumCRC32
	case out.ChecksumCRC32C != nil:
		algo, val = checksumCRC32C, *out.ChecksumCRC32C
	case out.ChecksumSHA1 != nil:
		algo, val = checksumSHA1, *out.ChecksumSHA1
	case out.ChecksumSHA256 != nil:
		algo, val = checksumSHA256, *out.ChecksumSHA256
	}

	if algo != "" {
		w.Header().Set("X-Amz-Checksum-"+algo, val)
		w.Header().Set("X-Amz-Checksum-Algorithm", algo)
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
	case checksumCRC32:
		return aws.String(checksum), nil, nil, nil
	case checksumCRC32C:
		return nil, aws.String(checksum), nil, nil
	case checksumSHA1:
		return nil, nil, aws.String(checksum), nil
	case checksumSHA256:
		return nil, nil, nil, aws.String(checksum)
	default:
		return nil, nil, nil, nil
	}
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

func (h *S3Handler) handleChecksumMode(
	w http.ResponseWriter,
	ver *s3.GetObjectOutput,
	details objectCommonDetails,
) {
	algo, val := h.getStoredChecksum(details)
	if algo == "" {
		data, _ := io.ReadAll(ver.Body)
		ver.Body = io.NopCloser(bytes.NewReader(data))

		algo = checksumCRC32
		val = CalculateChecksum(data, algo)
	}

	w.Header().Set("X-Amz-Checksum-Algorithm", algo)
	w.Header().Set("X-Amz-Checksum-"+algo, val)
}

func (h *S3Handler) getStoredChecksum(out objectCommonDetails) (string, string) {
	switch {
	case out.ChecksumCRC32 != nil:
		return checksumCRC32, *out.ChecksumCRC32
	case out.ChecksumCRC32C != nil:
		return checksumCRC32C, *out.ChecksumCRC32C
	case out.ChecksumSHA1 != nil:
		return checksumSHA1, *out.ChecksumSHA1
	case out.ChecksumSHA256 != nil:
		return checksumSHA256, *out.ChecksumSHA256
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
	stripQuotes := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQuotes(etag)

	// 1. If-Match and If-Unmodified-Since return 412 Precondition Failed
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if stripQuotes(ifMatch) != normalizedETag {
			return http.StatusPreconditionFailed, false
		}
	}

	if ifUnmodSince := r.Header.Get("If-Unmodified-Since"); ifUnmodSince != "" {
		if t, err := http.ParseTime(ifUnmodSince); err == nil && lastModified.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}

	// 2. If-None-Match and If-Modified-Since return 304 Not Modified
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if stripQuotes(ifNoneMatch) == normalizedETag {
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
			Code:    "MalformedXML",
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
				Code:    "InvalidArgument",
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
		Xmlns:           "http://s3.amazonaws.com/doc/2006-03-01/",
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
			Code:    "MalformedXML",
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
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Status: status,
	}

	httputils.WriteXML(ctx, w, http.StatusOK, lh)
}
