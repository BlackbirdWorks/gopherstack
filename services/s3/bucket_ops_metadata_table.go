package s3

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) createBucketMetadataConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "CreateBucketMetadataConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if err = h.Backend.CreateBucketMetadataConfiguration(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketMetadataConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketMetadataConfiguration")
	configXML, err := h.Backend.GetBucketMetadataConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

func (h *S3Handler) deleteBucketMetadataConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketMetadataConfiguration")
	if err := h.Backend.DeleteBucketMetadataConfiguration(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) createBucketMetadataTableConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "CreateBucketMetadataTableConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if err = h.Backend.CreateBucketMetadataTableConfiguration(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketMetadataTableConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketMetadataTableConfiguration")
	configXML, err := h.Backend.GetBucketMetadataTableConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

func (h *S3Handler) deleteBucketMetadataTableConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketMetadataTableConfiguration")
	if err := h.Backend.DeleteBucketMetadataTableConfiguration(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleUpdateBucketMetadataInventoryTableConfig handles PUT /{bucket}?metadataInventoryTableConfiguration.
// Persists the inventory table configuration so it survives round-trips, matching real S3 behaviour.
func (h *S3Handler) handleUpdateBucketMetadataInventoryTableConfig(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "UpdateBucketMetadataInventoryTableConfiguration")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	body, _ := httputils.ReadBody(r)

	if err := h.Backend.UpdateBucketMetadataInventoryTableConfig(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleUpdateBucketMetadataJournalTableConfig handles PUT /{bucket}?metadataJournalTableConfiguration.
// Persists the journal table configuration so it survives round-trips, matching real S3 behaviour.
func (h *S3Handler) handleUpdateBucketMetadataJournalTableConfig(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "UpdateBucketMetadataJournalTableConfiguration")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	body, _ := httputils.ReadBody(r)

	if err := h.Backend.UpdateBucketMetadataJournalTableConfig(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}
