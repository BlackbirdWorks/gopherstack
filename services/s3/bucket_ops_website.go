package s3 //nolint:dupl // put/get/delete HTTP handlers are structurally identical
// thin wrappers around distinct XML sub-resources (website here, encryption in the
// sibling file); each family lives in its own file per project convention, which
// makes the whole-file clone visible to dupl even though the code always looked
// like this.

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketWebsite(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketWebsite")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Validate the website XML is well-formed before storing it.
	var cfg WebsiteConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketWebsite(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketWebsite(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketWebsite")
	websiteXML, err := h.Backend.GetBucketWebsite(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(websiteXML))
}

func (h *S3Handler) deleteBucketWebsite(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketWebsite")
	if err := h.Backend.DeleteBucketWebsite(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}
