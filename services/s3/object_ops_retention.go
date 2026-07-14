package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

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
