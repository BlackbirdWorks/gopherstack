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

func (h *S3Handler) putObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectTagging")
	var tagging Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	var tags []types.Tag
	for _, t := range tagging.TagSet.Tags {
		tags = append(tags, types.Tag{
			Key:   aws.String(t.Key),
			Value: aws.String(t.Value),
		})
	}

	if err := validateObjectTags(tags); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	if _, err := h.Backend.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
		Tagging:   &types.Tagging{TagSet: tags},
	}); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectTagging")
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := Tagging{
		TagSet: TagSet{},
	}

	for _, t := range out.TagSet {
		if t.Key != nil && t.Value != nil {
			resp.TagSet.Tags = append(resp.TagSet.Tags, Tag{Key: *t.Key, Value: *t.Value})
		}
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) deleteObjectTagging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObjectTagging")
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	if _, err := h.Backend.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	}); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
