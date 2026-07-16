package s3

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketLifecycleConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	err = h.Backend.PutBucketLifecycleConfiguration(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketLifecycleConfiguration")
	lifecycleXML, err := h.Backend.GetBucketLifecycleConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if !strings.Contains(lifecycleXML, `xmlns="`) {
		lifecycleXML = strings.Replace(
			lifecycleXML,
			"<LifecycleConfiguration>",
			`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
			1,
		)
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(lifecycleXML))
}

func (h *S3Handler) deleteBucketLifecycle(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketLifecycle")
	if err := h.Backend.DeleteBucketLifecycle(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}
