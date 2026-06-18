package s3

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// headerRequestPayer is the request header a requester sets to acknowledge that
// it will pay transfer/request charges on a Requester-Pays bucket.
const headerRequestPayer = "X-Amz-Request-Payer"

// requestPayerRequester is the only value AWS accepts for x-amz-request-payer.
const requestPayerRequester = "requester"

// enforceRequesterPays implements AWS Requester-Pays semantics: when a bucket's
// request-payment configuration is "Requester", every object request must carry
// the header `x-amz-request-payer: requester`. A request that omits it is
// rejected with 403 AccessDenied, exactly as S3 does for a non-owner requester.
//
// It returns true when the request may proceed. When enforcement fails it writes
// the AWS-accurate error response and returns false. Anonymous/owner-vs-requester
// distinction is not modeled (gopherstack is single-tenant), so the presence of
// the acknowledgement header is the gate — which matches the observable contract
// SDK callers must satisfy against real S3.
func (h *S3Handler) enforceRequesterPays(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) bool {
	payer, err := h.Backend.GetBucketRequestPayment(ctx, bucketName)
	if err != nil {
		// Bucket-level errors are handled by the downstream operation; don't
		// short-circuit here.
		return true
	}

	if payer != requestPaymentRequester {
		return true
	}

	if strings.EqualFold(r.Header.Get(headerRequestPayer), requestPayerRequester) {
		// Requester acknowledged charges; echo the confirmation header as S3 does.
		w.Header().Set("X-Amz-Request-Charged", requestPayerRequester)

		return true
	}

	httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
		Code: errAccessDenied,
		Message: "Access Denied. This bucket is configured with Requester Pays; " +
			"requests must include the x-amz-request-payer header.",
	}, http.StatusForbidden)

	return false
}
