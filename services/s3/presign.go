package s3

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
)

// presignedDateFormat is the AWS SigV4 date-time format used in X-Amz-Date.
const presignedDateFormat = "20060102T150405Z"

// isPresignedRequest returns true when the request carries AWS presigned URL
// query parameters (i.e. X-Amz-Signature is present in the query string).
func isPresignedRequest(r *http.Request) bool {
	return r.URL.Query().Has("X-Amz-Signature")
}

// validatePresignedRequest checks whether a presigned URL request is still
// valid (not expired). It writes a 403 AccessDenied error and returns false
// if the request is invalid or expired. Returns true when the request may
// proceed normally.
func (h *S3Handler) validatePresignedRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) bool {
	q := r.URL.Query()

	dateStr := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")

	if dateStr == "" || expiresStr == "" {
		httputil.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AccessDenied",
			Message: "Request has expired.",
		}, http.StatusForbidden)

		return false
	}

	signedAt, err := time.Parse(presignedDateFormat, dateStr)
	if err != nil {
		httputil.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Date must be in the ISO 8601 basic format.",
		}, http.StatusBadRequest)

		return false
	}

	expires, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil || expires <= 0 {
		httputil.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Expires must be a positive integer.",
		}, http.StatusBadRequest)

		return false
	}

	expiresAt := signedAt.Add(time.Duration(expires) * time.Second)

	if time.Now().UTC().After(expiresAt) {
		httputil.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AccessDenied",
			Message: "Request has expired.",
		}, http.StatusForbidden)

		return false
	}

	return true
}
