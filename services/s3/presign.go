package s3

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// presignedDateFormat is the AWS SigV4 date-time format used in X-Amz-Date.
const presignedDateFormat = "20060102T150405Z"

// presignedAlgorithm is the only supported pre-signed URL signing algorithm.
const presignedAlgorithm = "AWS4-HMAC-SHA256"

// minPresignCredentialParts is the minimum number of slash-delimited parts in
// a valid X-Amz-Credential value: AKID/date/region/service/aws4_request.
const minPresignCredentialParts = 5

// isPresignedRequest returns true when the request carries AWS presigned URL
// query parameters (i.e. X-Amz-Signature is present in the query string).
func isPresignedRequest(r *http.Request) bool {
	return r.URL.Query().Has("X-Amz-Signature")
}

// validatePresignedRequest checks whether a presigned URL request is
// structurally well-formed and has not yet expired.
// It writes the appropriate S3 error response and returns false if validation
// fails. Returns true when the request may proceed normally.
func (h *S3Handler) validatePresignedRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) bool {
	q := r.URL.Query()

	// Verify all required query parameters are present and non-empty.
	algorithm := q.Get("X-Amz-Algorithm")
	credential := q.Get("X-Amz-Credential")
	dateStr := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")

	if algorithm == "" || credential == "" || dateStr == "" || expiresStr == "" || signedHeaders == "" ||
		signature == "" {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code: "AuthorizationQueryParametersError",
			Message: "Query-string authentication requires the X-Amz-Algorithm, X-Amz-Credential, " +
				"X-Amz-Date, X-Amz-Expires, X-Amz-SignedHeaders, and X-Amz-Signature parameters.",
		}, http.StatusBadRequest)

		return false
	}

	// Validate the signing algorithm.
	if algorithm != presignedAlgorithm {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Algorithm must be AWS4-HMAC-SHA256.",
		}, http.StatusBadRequest)

		return false
	}

	// Validate credential format: AKID/date/region/service/aws4_request
	credParts := strings.Split(credential, "/")
	if len(credParts) < minPresignCredentialParts {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Credential is not well-formed.",
		}, http.StatusBadRequest)

		return false
	}

	signedAt, err := time.Parse(presignedDateFormat, dateStr)
	if err != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Date must be in the ISO 8601 basic format.",
		}, http.StatusBadRequest)

		return false
	}

	expires, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil || expires <= 0 {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Expires must be a positive integer.",
		}, http.StatusBadRequest)

		return false
	}

	expiresAt := signedAt.Add(time.Duration(expires) * time.Second)

	if time.Now().UTC().After(expiresAt) {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "AccessDenied",
			Message: "Request has expired.",
		}, http.StatusForbidden)

		return false
	}

	return true
}
