package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"Gopherstack/pkgs/httputils"
	"Gopherstack/pkgs/logger"
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
	log := logger.Load(ctx)
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
	default:
		httputils.WriteError(log, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
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
		h.setOperation(ctx, "GetObjectAcl")
		w.WriteHeader(http.StatusNotImplemented) // ACLs ignored
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
	log := logger.Load(ctx)
	switch {
	case r.URL.Query().Has("uploads"):
		h.createMultipartUpload(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.completeMultipartUpload(ctx, w, r, bucket, key)
	default:
		httputils.WriteError(log, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
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
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) putObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObject")
	log := logger.Load(ctx)
	log.DebugContext(
		ctx,
		"S3 putObject input",
		"bucket",
		bucketName,
		"key",
		key,
		"contentType",
		r.Header.Get("Content-Type"),
	)

	data, err := httputils.ReadBody(r)
	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	userMeta := parseUserMetadata(r.Header)
	algo := strings.ToUpper(r.Header.Get("X-Amz-Checksum-Algorithm"))

	if algo == "" {
		algo = strings.ToUpper(r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"))
	}

	checksumCRC32, checksumCRC32C, checksumSHA1, checksumSHA256 := extractChecksumPointers(
		r.Header,
		algo,
	)

	contentType := r.Header.Get("Content-Type")

	ver, err := h.Backend.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:            aws.String(bucketName),
			Key:               aws.String(key),
			Body:              bytes.NewReader(data),
			Metadata:          userMeta,
			ContentType:       aws.String(contentType),
			ChecksumAlgorithm: types.ChecksumAlgorithm(algo),
			ChecksumCRC32:     checksumCRC32,
			ChecksumCRC32C:    checksumCRC32C,
			ChecksumSHA1:      checksumSHA1,
			ChecksumSHA256:    checksumSHA256,
			Tagging:           aws.String(r.Header.Get("X-Amz-Tagging")),
		},
	)
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

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

	log.DebugContext(ctx,
		"S3 putObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"versionId", aws.ToString(ver.VersionId),
	)

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) copyObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	destBucket, destKey string,
) {
	h.setOperation(ctx, "CopyObject")
	log := logger.Load(ctx)
	srcBucket, srcKey, srcVersionID, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		httputils.WriteError(log, w, r, ErrInvalidArgument, http.StatusBadRequest)

		return
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
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}
	defer srcVer.Body.Close()

	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	data, err := io.ReadAll(srcVer.Body)
	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	userMeta := srcVer.Metadata
	contentType := srcVer.ContentType

	if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
		if ct := r.Header.Get("Content-Type"); ct != "" {
			contentType = aws.String(ct)
		}
		userMeta = parseUserMetadata(r.Header)
	}

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(destBucket),
		Key:         aws.String(destKey),
		Body:        bytes.NewReader(data),
		Metadata:    userMeta,
		ContentType: contentType,
	}

	destVer, err := h.Backend.PutObject(ctx, putInput)
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	if destVer.VersionId != nil && *destVer.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *destVer.VersionId)
	}

	etag := ""
	if destVer.ETag != nil {
		etag = *destVer.ETag
	}

	httputils.WriteXML(log, w, http.StatusOK, CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().Format(time.RFC3339),
	})
}

func (h *S3Handler) getObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObject")
	log := logger.Load(ctx)
	versionID := r.URL.Query().Get("versionId")
	log.DebugContext(
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
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}
	defer ver.Body.Close()

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

	log.DebugContext(ctx,
		"S3 getObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"contentLength", aws.ToInt64(ver.ContentLength),
	)

	w.WriteHeader(http.StatusOK)

	if _, copyErr := io.Copy(w, ver.Body); copyErr != nil {
		log.ErrorContext(ctx, "failed to write object data", "error", copyErr)
	}
}

func (h *S3Handler) deleteObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObject")
	log := logger.Load(ctx)
	versionID := r.URL.Query().Get("versionId")
	log.DebugContext(
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
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	if out.VersionId != nil && *out.VersionId != "" && *out.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *out.VersionId)
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		w.Header().Set("X-Amz-Delete-Marker", "true")
	}

	log.DebugContext(ctx,
		"S3 deleteObject output",
		"bucket", bucketName, "key", key, "deleteMarker", aws.ToBool(out.DeleteMarker),
	)

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) deleteObjects(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "DeleteObjects")
	log := logger.Load(ctx)
	var req DeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		httputils.WriteError(log, w, r, err, http.StatusBadRequest)

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
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

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

	httputils.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) putObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectTagging")
	log := logger.Load(ctx)
	var tagging Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		httputils.WriteError(log, w, r, err, http.StatusBadRequest)

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
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

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
	log := logger.Load(ctx)
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
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

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

	httputils.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) deleteObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObjectTagging")
	log := logger.Load(ctx)
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
		httputils.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
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

func parseCopySource(src string) (string, string, string, bool) {
	src = strings.TrimPrefix(src, "/")
	parts := strings.SplitN(src, "/", pathSplitParts)

	if len(parts) != pathSplitParts {
		return "", "", "", false
	}

	bucket := parts[0]
	key := parts[1]
	versionID := ""

	if idx := strings.Index(key, "?versionId="); idx != -1 {
		versionID = key[idx+11:]
		key = key[:idx]
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
	log := logger.Load(ctx)
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
		log.ErrorContext(ctx, "failed to write range data", "error", err)
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
