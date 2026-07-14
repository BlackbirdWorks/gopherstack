package s3

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketReplication(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketReplication")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg ReplicationConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	if cfg.Role == "" || len(cfg.Rules) == 0 {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	// Real S3 requires the source bucket to have versioning enabled before a
	// replication configuration can be attached, returning InvalidRequest 400
	// otherwise. GetBucketVersioning surfaces NoSuchBucket for a missing bucket.
	verOut, verErr := h.Backend.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if verErr != nil {
		WriteError(ctx, w, r, verErr)

		return
	}
	if verOut.Status != types.BucketVersioningStatusEnabled {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errInvalidRequest,
			Message: "Versioning must be 'Enabled' on the bucket to apply a replication configuration.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketReplication(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketReplication(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketReplication")
	replicationXML, err := h.Backend.GetBucketReplication(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(replicationXML))
}

func (h *S3Handler) deleteBucketReplication(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketReplication")
	if err := h.Backend.DeleteBucketReplication(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}
