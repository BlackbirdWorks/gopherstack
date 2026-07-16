package s3

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketTagging")

	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var tagging Tagging
	if xmlErr := xml.Unmarshal(body, &tagging); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	tags := make([]types.Tag, 0, len(tagging.TagSet.Tags))
	for _, t := range tagging.TagSet.Tags {
		tags = append(tags, types.Tag{
			Key:   aws.String(t.Key),
			Value: aws.String(t.Value),
		})
	}

	if putErr := h.Backend.PutBucketTagging(ctx, bucket, tags); putErr != nil {
		WriteError(ctx, w, r, putErr)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketTagging")

	tags, err := h.Backend.GetBucketTagging(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	tagging := Tagging{}
	for _, t := range tags {
		tagging.TagSet.Tags = append(tagging.TagSet.Tags, Tag{
			Key:   aws.ToString(t.Key),
			Value: aws.ToString(t.Value),
		})
	}

	httputils.WriteXML(ctx, w, http.StatusOK, tagging)
}

func (h *S3Handler) deleteBucketTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketTagging")

	if err := h.Backend.DeleteBucketTagging(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
