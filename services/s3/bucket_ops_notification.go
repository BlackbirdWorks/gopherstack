package s3

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketNotificationConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketNotificationConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	err = h.Backend.PutBucketNotificationConfiguration(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketNotificationConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketNotificationConfiguration")
	notifXML, err := h.Backend.GetBucketNotificationConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if notifXML == "" {
		// Return empty notification config
		httputils.WriteXML(ctx, w, http.StatusOK, s3NotificationConfiguration{})

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(notifXML))
}
