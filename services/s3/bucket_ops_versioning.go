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

func (h *S3Handler) putBucketVersioning(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "PutBucketVersioning")
	var conf VersioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&conf); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	_, err := h.Backend.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status:    types.BucketVersioningStatus(conf.Status),
			MFADelete: types.MFADelete(conf.MfaDelete),
		},
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketVersioning(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "GetBucketVersioning")
	out, err := h.Backend.GetBucketVersioning(
		ctx,
		&s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)},
	)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	status := ""
	if out.Status != "" {
		status = string(out.Status)
	}

	httputils.WriteXML(ctx, w, http.StatusOK, VersioningConfiguration{
		Status:    status,
		MfaDelete: string(out.MFADelete),
	})
}
