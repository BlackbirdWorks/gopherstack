package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"

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
// request-payment configuration is "Requester", every object request must carry
// `x-amz-request-payer: requester` or be rejected with 403 AccessDenied. Returns
// true when the request may proceed; on failure it writes the AWS-accurate error
// response and returns false. Owner-vs-requester isn't modeled (single-tenant),
// so header presence alone is the gate, matching the observable SDK contract.
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
		_ = xml.Unmarshal(body, &cfg)
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
