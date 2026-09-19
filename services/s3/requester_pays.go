package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// headerRequestPayer is the request header a requester sets to acknowledge that
// it will pay transfer/request charges on a Requester-Pays bucket.
const headerRequestPayer = "X-Amz-Request-Payer"

// requestPayerRequester is the only value AWS accepts for x-amz-request-payer.
const requestPayerRequester = "requester"

// requestPaymentBucketOwner is the default request-payment payer value.
const requestPaymentBucketOwner = "BucketOwner"

// requestPaymentRequester is the request-payment payer value that enables
// Requester-Pays enforcement.
const requestPaymentRequester = "Requester"

// enforceRequesterPays implements AWS Requester-Pays semantics: when a bucket's
// request-payment configuration is "Requester", every non-owner object request
// must carry `x-amz-request-payer: requester` or be rejected with 403
// AccessDenied. The bucket owner account is exempt (S3 docs, "Requester Pays
// buckets": bucket owners are never charged and never need the header).
// Returns true when the request may proceed; on failure it writes the
// AWS-accurate error response and returns false.
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

	if h.isBucketOwnerCaller(ctx, r, bucketName) {
		return true
	}

	httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
		Code: errAccessDenied,
		Message: "Access Denied. This bucket is configured with Requester Pays; " +
			"requests must include the x-amz-request-payer header.",
	}, http.StatusForbidden)

	return false
}

// isBucketOwnerCaller reports whether r's resolved caller is the bucket's
// owner account. Anonymous requests are never the owner, matching the
// caller model used for bucket-policy/ACL enforcement (see authz.go's
// callerIdentity) -- only a signed request whose account matches the bucket's
// creator qualifies for the Requester-Pays owner exemption.
func (h *S3Handler) isBucketOwnerCaller(ctx context.Context, r *http.Request, bucketName string) bool {
	if callerIdentity(r).anonymous {
		return false
	}

	ownerAccount, err := h.Backend.GetBucketOwnerAccount(ctx, bucketName)
	if err != nil {
		return false
	}

	return awsmeta.Account(ctx) == ownerAccount
}

// GetBucketOwnerAccount returns the account ID that created bucketName.
// Buckets persisted before this field existed default to awsmeta.DefaultAccount.
func (b *InMemoryBackend) GetBucketOwnerAccount(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketOwnerAccount")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketOwnerAccount")
	defer bucket.mu.RUnlock()

	if bucket.OwnerAccountID == "" {
		return awsmeta.DefaultAccount, nil
	}

	return bucket.OwnerAccountID, nil
}

// PutBucketRequestPayment stores the request-payment payer ("BucketOwner" or "Requester").
func (b *InMemoryBackend) PutBucketRequestPayment(
	_ context.Context,
	bucketName, payer string,
) error {
	b.mu.RLock("PutBucketRequestPayment")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketRequestPayment")
	defer bucket.mu.Unlock()

	bucket.RequestPaymentPayer = payer

	return nil
}

// GetBucketRequestPayment returns the request-payment payer; defaults to "BucketOwner".
func (b *InMemoryBackend) GetBucketRequestPayment(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketRequestPayment")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketRequestPayment")
	defer bucket.mu.RUnlock()

	if bucket.RequestPaymentPayer == "" {
		return requestPaymentBucketOwner, nil
	}

	return bucket.RequestPaymentPayer, nil
}

// requestPaymentConfiguration is the XML body for PUT/GET ?requestPayment.
type requestPaymentConfiguration struct {
	XMLName xml.Name `xml:"RequestPaymentConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Payer   string   `xml:"Payer,omitempty"`
}

// handlePutBucketRequestPayment handles PUT /{bucket}?requestPayment.
func (h *S3Handler) handlePutBucketRequestPayment(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "PutBucketRequestPayment")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	var cfg requestPaymentConfiguration

	body, _ := httputils.ReadBody(r)
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
			httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
				Code:    errMalformedXML,
				Message: errMalformedXMLMsg,
			}, http.StatusBadRequest)

			return
		}
	}

	if cfg.Payer != "Requester" && cfg.Payer != "BucketOwner" {
		cfg.Payer = "BucketOwner"
	}

	if err := h.Backend.PutBucketRequestPayment(ctx, bucket, cfg.Payer); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleGetBucketRequestPayment handles GET /{bucket}?requestPayment.
func (h *S3Handler) handleGetBucketRequestPayment(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "GetBucketRequestPayment")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	payer, err := h.Backend.GetBucketRequestPayment(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	httputils.WriteXML(ctx, w, http.StatusOK,
		requestPaymentConfiguration{Xmlns: xmlNamespaceS3, Payer: payer})
}
