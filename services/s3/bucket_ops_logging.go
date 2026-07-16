package s3

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketLogging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketLogging")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg BucketLoggingStatus
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketLogging(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketLogging(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketLogging")
	loggingXML, err := h.Backend.GetBucketLogging(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if loggingXML == "" {
		httputils.WriteXML(
			ctx,
			w,
			http.StatusOK,
			s3BucketLoggingStatus{Xmlns: xmlNamespaceS3},
		)

		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(loggingXML))
}
