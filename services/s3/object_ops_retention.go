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

// restoreRequest is the XML body for POST ?restore.
type restoreRequest struct {
	XMLName xml.Name `xml:"RestoreRequest"`
	Days    int      `xml:"Days"`
}

// handleRestoreObject handles POST /{bucket}/{key}?restore.
// Reads the XML RestoreRequest body to extract Days, then marks the latest
// version as restored for that period.
func (h *S3Handler) handleRestoreObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "RestoreObject")

	bucket, key, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || key == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	var req restoreRequest

	body, _ := httputils.ReadBody(r)
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if err := h.Backend.RestoreObject(ctx, bucket, key, req.Days); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// AWS fires s3:ObjectRestore:Post when a restore is initiated; downstream
	// SQS/SNS/Lambda consumers depend on it for archival workflows.
	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx, bucket,
		); ncErr == nil && notifXML != "" {
			go h.notifier.DispatchObjectRestorePost(
				h.notificationDispatchContext(), bucket, key, notifXML,
			)
		}
	}

	w.WriteHeader(http.StatusAccepted)
}
