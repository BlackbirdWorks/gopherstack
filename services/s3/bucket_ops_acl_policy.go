package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// validCannedACLs is the complete set of canned ACL strings that AWS S3 accepts
// for PutBucketAcl. This mirrors types.BucketCannedACL (private, public-read,
// public-read-write, authenticated-read) plus log-delivery-write, which is
// bucket-only. The bucket-owner-read / bucket-owner-full-control canned ACLs
// belong to types.ObjectCannedACL only — real S3 rejects them on PutBucketAcl
// with 400 InvalidArgument, so they are deliberately excluded here.
var validCannedACLs = map[string]struct{}{ //nolint:gochecknoglobals // package-level lookup table
	aclPrivate:           {},
	aclPublicRead:        {},
	aclPublicReadWrite:   {},
	aclAuthenticatedRead: {},
	aclLogDeliveryWrite:  {},
}

func (h *S3Handler) putBucketACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "PutBucketAcl")

	// A PutBucketAcl request carries the grant set either as an x-amz-acl canned
	// header or as an AccessControlPolicy XML body (mutually exclusive in AWS).
	// Mirror the object-ACL path: persist whichever was supplied, and feed both
	// the canned value and the body into the Public-Access-Block check so a
	// body-only request can't slip a public grant past BlockPublicAcls.
	canned := r.Header.Get("X-Amz-Acl")

	body, _ := httputils.ReadBody(r)

	// Only validate the canned value when no explicit body was supplied; an
	// AccessControlPolicy body is authoritative and needs no canned name.
	if len(body) == 0 {
		if canned == "" {
			canned = "private"
		}

		if _, ok := validCannedACLs[canned]; !ok {
			WriteError(ctx, w, r, ErrInvalidArgument)

			return
		}
	}

	acl := canned
	if len(body) > 0 {
		acl = string(body)
	}

	if err := h.enforceACLPolicy(ctx, bucketName, canned, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err := h.Backend.PutBucketACL(ctx, bucketName, acl); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "GetBucketAcl")

	canned, err := h.Backend.GetBucketACL(ctx, bucketName)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// When PutBucketAcl stored a full AccessControlPolicy XML body, return it
	// verbatim (mirrors getObjectACL); otherwise synthesise XML from the canned
	// ACL name.
	if strings.HasPrefix(strings.TrimSpace(canned), "<") {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(canned))

		return
	}

	resp := AccessControlPolicy{
		Xmlns: xmlNamespaceS3,
		Owner: Owner{
			ID:          gopherstackName,
			DisplayName: gopherstackName,
		},
		ACL: AccessControlList{
			Grants: cannedACLGrants(canned, gopherstackName, gopherstackName),
		},
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) putBucketPolicy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketPolicy")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if pabErr := h.enforceBucketPolicyAgainstPAB(ctx, bucket, string(body)); pabErr != nil {
		WriteError(ctx, w, r, pabErr)

		return
	}

	err = h.Backend.PutBucketPolicy(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketPolicy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketPolicy")
	policy, err := h.Backend.GetBucketPolicy(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(policy))
}

func (h *S3Handler) deleteBucketPolicy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketPolicy")
	if err := h.Backend.DeleteBucketPolicy(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putPublicAccessBlock(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutPublicAccessBlock")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg PublicAccessBlockConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutPublicAccessBlock(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getPublicAccessBlock(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetPublicAccessBlock")
	configXML, err := h.Backend.GetPublicAccessBlock(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

func (h *S3Handler) deletePublicAccessBlock(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeletePublicAccessBlock")
	if err := h.Backend.DeletePublicAccessBlock(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketOwnershipControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketOwnershipControls")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg OwnershipControls
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketOwnershipControls(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketOwnershipControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketOwnershipControls")
	configXML, err := h.Backend.GetBucketOwnershipControls(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

func (h *S3Handler) deleteBucketOwnershipControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketOwnershipControls")
	if err := h.Backend.DeleteBucketOwnershipControls(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}
