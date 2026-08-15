package s3

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"slices"
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

	// Real S3 rejects a policy body that isn't valid JSON with 400
	// MalformedPolicy before ever persisting it.
	if !json.Valid(body) {
		WriteError(ctx, w, r, ErrMalformedPolicy)

		return
	}

	// Beyond JSON syntax, real S3 validates the IAM policy document shape
	// (Version, Statement, Effect, Principal, Action, Resource) and rejects
	// semantically invalid policies with 400 MalformedPolicy before
	// persisting — see validateBucketPolicyDocument for the grammar this
	// checks against.
	if shapeErr := validateBucketPolicyDocument(body); shapeErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, *shapeErr, http.StatusBadRequest)

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

// s3PolicyStatus is the XML response for GetBucketPolicyStatus.
type s3PolicyStatus struct {
	XMLName  xml.Name `xml:"PolicyStatus"`
	Xmlns    string   `xml:"xmlns,attr"`
	IsPublic string   `xml:"IsPublic,omitempty"`
}

// abacStatusXML models AbacStatus (s3@v1.106.5 types/types.go), the real wire shape
// for both PutBucketAbac's request body and the entirety of GetBucketAbac's response
// body. GetBucketAbacOutput.AbacStatus is httpPayload-bound: the real deserializer
// (deserializers.go: (*awsRestxml_deserializeOpGetBucketAbac).HandleDeserialize)
// parses the response's ROOT element directly as AbacStatus via
// awsRestxml_deserializeDocumentAbacStatus -- it does not look for an AbacStatus
// child of some other envelope root, so the response root itself must be
// "AbacStatus", not "AbacConfiguration" wrapping a nested AbacStatus child. (A
// same-named awsRestxml_deserializeOpDocumentGetBucketAbacOutput function exists in
// the SDK source but is dead code -- HandleDeserialize never calls it -- and reading
// it as authoritative here would reproduce exactly the class of bug this fixes.) The
// request-side root was also previously "AbacConfiguration"
// (awsRestxml_serializeOpPutBucketAbac's payloadRoot.Local is "AbacStatus"), so
// re-parsing the stored PUT body on Get always errored and GetBucketAbac silently
// returned an empty status for every bucket with ABAC configured.
type abacStatusXML struct {
	XMLName xml.Name `xml:"AbacStatus"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status,omitempty"`
}

// routeBucketGetStubsExtra handles additional bucket GET sub-resource stubs.
// Returns true if handled.
func (h *S3Handler) routeBucketGetStubsExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) bool {
	q := r.URL.Query()

	switch {
	case q.Has("accelerate"):
		h.setOperation(ctx, "GetBucketAccelerateConfiguration")

		status, err := h.Backend.GetBucketAccelerateConfiguration(ctx, bucket)
		if err != nil {
			WriteError(ctx, w, r, err)

			return true
		}

		httputils.WriteXML(ctx, w, http.StatusOK,
			s3AccelerateConfiguration{Xmlns: xmlNamespaceS3, Status: status})

		return true

	case q.Has("policyStatus"):
		h.handleGetBucketPolicyStatus(ctx, w, r, bucket)

		return true

	case q.Has("abac"):
		h.setOperation(ctx, "GetBucketAbac")

		configXML, err := h.Backend.GetBucketAbac(ctx, bucket)
		if err != nil {
			WriteError(ctx, w, r, err)

			return true
		}

		var stored abacStatusXML
		if configXML != "" {
			if xmlErr := xml.Unmarshal([]byte(configXML), &stored); xmlErr != nil {
				httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
					Code:    errMalformedXML,
					Message: errMalformedXMLMsg,
				}, http.StatusBadRequest)

				return true
			}
		}

		stored.Xmlns = xmlNamespaceS3
		httputils.WriteXML(ctx, w, http.StatusOK, stored)

		return true
	}

	return false
}

// policyStatement is a single statement in a bucket policy JSON document.
type policyStatement struct {
	Principal any    `json:"Principal"`
	Action    any    `json:"Action"`
	Effect    string `json:"Effect"`
}

// policyDoc is the minimal structure needed to evaluate public access.
type policyDoc struct {
	Statement []policyStatement `json:"Statement"`
}

// handleGetBucketPolicyStatus implements GetBucketPolicyStatus.
// It returns IsPublic=true when the bucket policy grants s3:GetObject to "*".
func (h *S3Handler) handleGetBucketPolicyStatus(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketPolicyStatus")

	isPublic := sqlValFalse

	policy, err := h.Backend.GetBucketPolicy(ctx, bucket)
	if err != nil && !errors.Is(err, ErrNoBucketPolicy) {
		WriteError(ctx, w, r, err)

		return
	}

	// A public policy only makes the bucket public when the Public Access Block
	// isn't restricting public bucket policies. RestrictPublicBuckets causes S3
	// to report IsPublic=false regardless of the policy content.
	pab, _ := loadPublicAccessBlock(ctx, h.Backend, bucket)
	if policy != "" && policyGrantsPublicGetObject(policy) && !pab.RestrictPublicBuckets {
		isPublic = sqlValTrue
	}

	httputils.WriteXML(ctx, w, http.StatusOK,
		s3PolicyStatus{Xmlns: xmlNamespaceS3, IsPublic: isPublic})
}

// policyGrantsPublicGetObject returns true if the JSON policy document
// contains an Allow statement that grants s3:GetObject to the wildcard principal "*".
func policyGrantsPublicGetObject(policyJSON string) bool {
	var doc policyDoc
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return false
	}

	for _, stmt := range doc.Statement {
		if stmt.Effect != "Allow" {
			continue
		}

		if !principalIsWildcard(stmt.Principal) {
			continue
		}

		if actionIncludesGetObject(stmt.Action) {
			return true
		}
	}

	return false
}

// principalIsWildcard returns true when the policy principal is "*" (any principal).
func principalIsWildcard(principal any) bool {
	switch v := principal.(type) {
	case string:
		return v == "*"
	case map[string]any:
		if aws, ok := v["AWS"]; ok {
			return principalIsWildcard(aws)
		}
	case []any:
		return slices.ContainsFunc(v, principalIsWildcard)
	}

	return false
}

// actionIncludesGetObject returns true when the action list includes s3:GetObject or s3:*.
func actionIncludesGetObject(action any) bool {
	check := func(s string) bool {
		s = strings.ToLower(s)

		return s == actionGetObjectLower || s == "s3:*" || s == "*"
	}

	switch v := action.(type) {
	case string:
		return check(v)
	case []any:
		return slices.ContainsFunc(v, func(item any) bool {
			s, ok := item.(string)

			return ok && check(s)
		})
	}

	return false
}

// handlePutBucketAbac handles PUT /{bucket}?abac.
// Parses and stores the AbacConfiguration XML so that GetBucketAbac returns
// the persisted config, matching real S3 Tables behaviour.
func (h *S3Handler) handlePutBucketAbac(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "PutBucketAbac")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	body, _ := httputils.ReadBody(r)

	if err := h.Backend.PutBucketAbac(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}
