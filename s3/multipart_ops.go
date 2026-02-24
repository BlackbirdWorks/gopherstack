package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *S3Handler) createMultipartUpload(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "CreateMultipartUpload")
	log := logger.Load(ctx)
	out, err := h.Backend.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	resp := InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      key,
		UploadID: *out.UploadId,
	}

	httputil.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) uploadPart(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "UploadPart")
	log := logger.Load(ctx)
	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		WriteError(log, w, r, ErrInvalidArgument)

		return
	}

	data, err := httputil.ReadBody(r)
	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	out, err := h.Backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(int32(partNumber)), // #nosec G109 G115
		Body:       bytes.NewReader(data),
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) ||
		errors.Is(err, ErrNoSuchUpload) {
		WriteError(log, w, r, err)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	w.Header().Set("ETag", *out.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) completeMultipartUpload(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "CompleteMultipartUpload")
	log := logger.Load(ctx)
	uploadID := r.URL.Query().Get("uploadId")

	var partsReq CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&partsReq); err != nil {
		WriteError(log, w, r, ErrInvalidArgument)

		return
	}

	var sdkParts []types.CompletedPart
	for _, p := range partsReq.Parts {
		if p.PartNumber < 1 || p.PartNumber > 10000 {
			WriteError(log, w, r, ErrInvalidPart)

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
		WriteError(log, w, r, err)

		return
	}

	if errors.Is(err, ErrInvalidPart) {
		WriteError(log, w, r, err)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	resp := CompleteMultipartUploadResult{
		Location: "/" + bucketName + "/" + key,
		Bucket:   bucketName,
		Key:      key,
		ETag:     *out.ETag,
	}

	httputil.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) abortMultipartUpload(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	log := logger.Load(ctx)
	uploadID := r.URL.Query().Get("uploadId")

	if _, err := h.Backend.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}); errors.Is(err, ErrNoSuchUpload) {
		WriteError(log, w, r, err)

		return
	} else if err != nil {
		WriteError(log, w, r, err)

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
	log := logger.Load(ctx)

	out, err := h.Backend.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(r.URL.Query().Get("prefix")),
	})
	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	result := ListMultipartUploadsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucketName,
		MaxUploads:  1000, //nolint:mnd // S3 default max uploads per page
		IsTruncated: false,
	}

	for _, u := range out.Uploads {
		result.Uploads = append(result.Uploads, MultipartUpload{
			Key:       aws.ToString(u.Key),
			UploadID:  aws.ToString(u.UploadId),
			Initiated: aws.ToTime(u.Initiated),
		})
	}

	httputil.WriteXML(log, w, http.StatusOK, result)
}

func (h *S3Handler) listParts(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "ListParts")
	log := logger.Load(ctx)
	uploadID := r.URL.Query().Get("uploadId")

	out, err := h.Backend.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if errors.Is(err, ErrNoSuchUpload) {
		WriteError(log, w, r, err)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	result := ListPartsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucketName,
		Key:         key,
		UploadID:    uploadID,
		MaxParts:    1000, //nolint:mnd // S3 default max parts per page
		IsTruncated: false,
	}

	for _, p := range out.Parts {
		result.Parts = append(result.Parts, PartXML{
			PartNumber: int(aws.ToInt32(p.PartNumber)),
			ETag:       aws.ToString(p.ETag),
			Size:       aws.ToInt64(p.Size),
		})
	}

	httputil.WriteXML(log, w, http.StatusOK, result)
}
