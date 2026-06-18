package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) createMultipartUpload(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "CreateMultipartUpload")

	tagging := r.Header.Get("X-Amz-Tagging")

	// Capture SSE config at session-init time and pin it on the upload via
	// ctx. CompleteMultipartUpload reads it back to apply envelope encryption
	// to the assembled body — same flow real S3 uses (SSE chosen on Create,
	// applied on Complete, parts not individually encrypted).
	sse, sseErr := extractSSEInfo(r)
	if sseErr != nil {
		WriteError(ctx, w, r, sseErr)

		return
	}
	ctx = context.WithValue(ctx, sseKey, sse)

	out, err := h.Backend.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(key),
		Tagging:              aws.String(tagging),
		ServerSideEncryption: types.ServerSideEncryption(sse.Algorithm),
		SSEKMSKeyId:          nilStringIfEmpty(sse.KMSKeyID),
		SSECustomerAlgorithm: nilStringIfEmpty(sse.SSECAlgorithm),
		SSECustomerKeyMD5:    nilStringIfEmpty(sse.SSECKeyMD5),
		SSECustomerKey:       nilStringIfEmpty(sse.SSECKeyB64),
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      key,
		UploadID: *out.UploadId,
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) uploadPart(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	if r.Header.Get("X-Amz-Copy-Source") != "" {
		h.uploadPartCopy(ctx, w, r, bucketName, key, uploadID, partNumber)

		return
	}

	h.setOperation(ctx, "UploadPart")

	if md5Header := r.Header.Get("Content-MD5"); md5Header != "" {
		ctx = context.WithValue(ctx, md5Key, md5Header)
	}

	algo, crc32p, crc32cp, sha1p, sha256p := extractAlgoAndChecksums(r)

	out, err := h.Backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(key),
		UploadId:          aws.String(uploadID),
		PartNumber:        aws.Int32(int32(partNumber)), // #nosec G109 G115
		Body:              r.Body,
		ChecksumAlgorithm: types.ChecksumAlgorithm(algo),
		ChecksumCRC32:     crc32p,
		ChecksumCRC32C:    crc32cp,
		ChecksumSHA1:      sha1p,
		ChecksumSHA256:    sha256p,
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) ||
		errors.Is(err, ErrNoSuchUpload) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.Header().Set("ETag", *out.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) uploadPartCopy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key, uploadID string,
	partNumber int,
) {
	h.setOperation(ctx, "UploadPartCopy")

	srcVer, err := h.copySourceData(ctx, r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	defer srcVer.Body.Close()

	var body io.Reader = srcVer.Body
	if srcRange := r.Header.Get("X-Amz-Copy-Source-Range"); srcRange != "" {
		data, readErr := io.ReadAll(srcVer.Body)
		if readErr != nil {
			WriteError(ctx, w, r, readErr)

			return
		}

		start, end, ok := parseRange(srcRange, int64(len(data)))
		if !ok {
			WriteError(ctx, w, r, ErrInvalidArgument)

			return
		}

		body = bytes.NewReader(data[start : end+1])
	}

	out, err := h.Backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(int32(partNumber)), // #nosec G115
		Body:       body,
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := UploadPartCopyResult{
		LastModified: time.Now().UTC().Format(time.RFC3339),
		ETag:         aws.ToString(out.ETag),
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) completeMultipartUpload(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, opCompleteMultipartUpload)
	uploadID := r.URL.Query().Get("uploadId")

	var partsReq CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&partsReq); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	var sdkParts []types.CompletedPart
	for _, p := range partsReq.Parts {
		if p.PartNumber < 1 || p.PartNumber > 10000 {
			WriteError(ctx, w, r, ErrInvalidPart)

			return
		}
		sdkParts = append(sdkParts, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(int32(p.PartNumber)), // #nosec G115
		})
	}

	out, err := h.Backend.CompleteMultipartUpload(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucketName),
			Key:             aws.String(key),
			UploadId:        aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: sdkParts},
		},
	)
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) ||
		errors.Is(err, ErrNoSuchUpload) {
		WriteError(ctx, w, r, err)

		return
	}

	if errors.Is(err, ErrInvalidPart) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := CompleteMultipartUploadResult{
		Location: "/" + bucketName + "/" + key,
		Bucket:   bucketName,
		Key:      key,
		ETag:     *out.ETag,
	}

	// Dispatch S3 notification if configured.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx,
			bucketName,
		); ncErr == nil && notifXML != "" {
			etag := aws.ToString(out.ETag)
			var size int64
			if headOut, headErr := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			}); headErr == nil {
				size = aws.ToInt64(headOut.ContentLength)
			}
			go h.notifier.DispatchObjectCompleted(
				h.notificationDispatchContext(),
				bucketName,
				key,
				etag,
				size,
				notifXML,
			)
		}
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) abortMultipartUpload(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "AbortMultipartUpload")

	uploadID := r.URL.Query().Get("uploadId")

	if _, err := h.Backend.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}); errors.Is(err, ErrNoSuchUpload) {
		WriteError(ctx, w, r, err)

		return
	} else if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) listMultipartUploads(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListMultipartUploads")

	q := r.URL.Query()

	var maxUploads *int32
	if mu := q.Get("max-uploads"); mu != "" {
		if n, err := strconv.ParseInt(mu, 10, 32); err == nil && n > 0 && n <= math.MaxInt32 {
			v := int32(n)
			maxUploads = &v
		}
	}

	out, err := h.Backend.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucketName),
		Prefix:         aws.String(q.Get("prefix")),
		Delimiter:      aws.String(q.Get("delimiter")),
		KeyMarker:      aws.String(q.Get("key-marker")),
		UploadIdMarker: aws.String(q.Get("upload-id-marker")),
		MaxUploads:     maxUploads,
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	result := ListMultipartUploadsResult{
		Xmlns:              xmlNamespaceS3,
		Bucket:             bucketName,
		Delimiter:          q.Get("delimiter"),
		MaxUploads:         int(aws.ToInt32(out.MaxUploads)),
		IsTruncated:        aws.ToBool(out.IsTruncated),
		NextKeyMarker:      aws.ToString(out.NextKeyMarker),
		NextUploadIDMarker: aws.ToString(out.NextUploadIdMarker),
	}

	for _, u := range out.Uploads {
		result.Uploads = append(result.Uploads, MultipartUpload{
			Key:       aws.ToString(u.Key),
			UploadID:  aws.ToString(u.UploadId),
			Initiated: aws.ToTime(u.Initiated),
		})
	}

	for _, cp := range out.CommonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefixXML{
			Prefix: aws.ToString(cp.Prefix),
		})
	}

	httputils.WriteXML(ctx, w, http.StatusOK, result)
}

const (
	defaultMaxParts = 1000
	maxPartsLimit   = 1000
)

func (h *S3Handler) listParts(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "ListParts")
	q := r.URL.Query()
	uploadID := q.Get("uploadId")

	maxParts := int32(defaultMaxParts)
	if mp := q.Get("max-parts"); mp != "" {
		n, err := strconv.ParseInt(mp, 10, 32)
		if err != nil || n < 1 || n > maxPartsLimit {
			WriteError(ctx, w, r, ErrInvalidArgument)

			return
		}
		maxParts = int32(n)
	}

	partNumberMarkerStr := q.Get("part-number-marker")
	partNumberMarker := int32(0)
	if partNumberMarkerStr != "" {
		if n, err := strconv.Atoi(partNumberMarkerStr); err == nil && n >= 0 && n <= math.MaxInt32 {
			partNumberMarker = int32(n) //nolint:gosec // validated within int32 range
		}
	}

	out, err := h.Backend.ListParts(ctx, &s3.ListPartsInput{
		Bucket:           aws.String(bucketName),
		Key:              aws.String(key),
		UploadId:         aws.String(uploadID),
		MaxParts:         aws.Int32(maxParts),
		PartNumberMarker: aws.String(partNumberMarkerStr),
	})
	if errors.Is(err, ErrNoSuchUpload) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	result := ListPartsResult{
		Xmlns:            xmlNamespaceS3,
		Bucket:           bucketName,
		Key:              key,
		UploadID:         uploadID,
		MaxParts:         int(maxParts),
		PartNumberMarker: int(partNumberMarker),
		IsTruncated:      aws.ToBool(out.IsTruncated),
	}

	if out.NextPartNumberMarker != nil && *out.NextPartNumberMarker != "" {
		result.NextPartNumberMarker = *out.NextPartNumberMarker
	}

	for _, p := range out.Parts {
		result.Parts = append(result.Parts, PartXML{
			PartNumber: int(aws.ToInt32(p.PartNumber)),
			ETag:       aws.ToString(p.ETag),
			Size:       aws.ToInt64(p.Size),
		})
	}

	httputils.WriteXML(ctx, w, http.StatusOK, result)
}
