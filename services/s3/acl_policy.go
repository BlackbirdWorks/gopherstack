package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// publicCannedACLs is the set of canned ACL strings that grant access to
// anonymous or all-authenticated users. PutBucketAcl / PutObjectAcl with one
// of these names is rejected when BlockPublicAcls is set on the bucket's
// Public Access Block configuration.
var publicCannedACLs = map[string]struct{}{ //nolint:gochecknoglobals // lookup table
	"public-read":        {},
	"public-read-write":  {},
	"authenticated-read": {},
}

// enforceACLPolicy checks the bucket's PublicAccessBlock and OwnershipControls
// before allowing a canned-ACL Put*Acl request. It returns ErrAccessDenied
// when the request would violate either policy.
//
// Rules (matching real S3):
//   - OwnershipControls=BucketOwnerEnforced disables ACLs entirely; any
//     Put*Acl is rejected.
//   - BlockPublicAcls=true rejects canned ACLs that grant public access
//     (public-read, public-read-write, authenticated-read). Private and
//     bucket-owner-* canned values still pass.
//
// xmlBodyACL (the XML form of a PutObjectAcl body) is treated as a public
// grant if it references the AllUsers / AuthenticatedUsers groups.
func (h *S3Handler) enforceACLPolicy(
	ctx context.Context,
	bucketName, cannedACL, xmlBodyACL string,
) error {
	ownership, err := h.Backend.GetBucketOwnershipControls(ctx, bucketName)
	if err == nil && ownershipIsBucketOwnerEnforced(ownership) {
		return ErrAccessDenied
	}

	pabXML, err := h.Backend.GetPublicAccessBlock(ctx, bucketName)
	if err != nil || pabXML == "" {
		// No PAB set → only the BucketOwnerEnforced check above applies.
		return nil
	}

	var pab PublicAccessBlockConfiguration
	if unmarshalErr := xml.Unmarshal([]byte(pabXML), &pab); unmarshalErr != nil {
		return nil
	}

	if !pab.BlockPublicAcls {
		return nil
	}

	if cannedACL != "" {
		if _, public := publicCannedACLs[cannedACL]; public {
			return ErrAccessDenied
		}
	}

	if xmlBodyACL != "" && aclXMLGrantsPublic(xmlBodyACL) {
		return ErrAccessDenied
	}

	return nil
}

// ownershipIsBucketOwnerEnforced reports whether the ObjectOwnership rule on
// the bucket disables ACLs (i.e. is set to BucketOwnerEnforced).
func ownershipIsBucketOwnerEnforced(ownershipXML string) bool {
	var oc OwnershipControls
	if err := xml.Unmarshal([]byte(ownershipXML), &oc); err != nil {
		return false
	}

	for _, rule := range oc.Rules {
		if rule.ObjectOwnership == "BucketOwnerEnforced" {
			return true
		}
	}

	return false
}

// aclXMLGrantsPublic reports whether the AccessControlPolicy XML body grants
// access to the AllUsers or AuthenticatedUsers groups, which is what
// BlockPublicAcls is designed to prevent.
func aclXMLGrantsPublic(aclXML string) bool {
	return strings.Contains(aclXML, "AllUsers") ||
		strings.Contains(aclXML, "AuthenticatedUsers")
}

// enforceBucketPolicyAgainstPAB rejects PutBucketPolicy when the bucket's
// PublicAccessBlock has BlockPublicPolicy=true AND the policy grants access
// to anonymous principals. Real S3 evaluates the full IAM policy; we do a
// conservative substring check on Principal — sufficient to block the most
// common public-policy patterns (`"Principal":"*"`, `"AWS":"*"`).
func (h *S3Handler) enforceBucketPolicyAgainstPAB(
	ctx context.Context, bucketName, policyJSON string,
) error {
	pabXML, err := h.Backend.GetPublicAccessBlock(ctx, bucketName)
	if err != nil || pabXML == "" {
		return nil
	}

	var pab PublicAccessBlockConfiguration
	if uErr := xml.Unmarshal([]byte(pabXML), &pab); uErr != nil {
		return nil
	}

	if !pab.BlockPublicPolicy {
		return nil
	}

	if policyAllowsPublic(policyJSON) {
		return ErrAccessDenied
	}

	return nil
}

// policyAllowsPublic returns true when the bucket policy JSON contains a
// wildcard Principal grant. Matches `"Principal":"*"`, `"Principal":{"AWS":"*"}`,
// and similar forms after whitespace normalisation.
func policyAllowsPublic(policyJSON string) bool {
	normalised := strings.ReplaceAll(policyJSON, " ", "")
	normalised = strings.ReplaceAll(normalised, "\n", "")
	normalised = strings.ReplaceAll(normalised, "\t", "")

	return strings.Contains(normalised, `"Principal":"*"`) ||
		strings.Contains(normalised, `"AWS":"*"`) ||
		strings.Contains(normalised, `"AWS":["*"]`)
}

// errACLPolicyBlocked is returned as a sentinel by callers wanting to
// distinguish PAB blocks from other AccessDenied causes. Currently the
// handler responds the same either way; this is kept for future audit logs.
var errACLPolicyBlocked = errors.New("ACL policy blocks request")

var _ = errACLPolicyBlocked

// enforcePutObjectPreconditions checks If-Match / If-None-Match on a PutObject
// request. Returns ErrPreconditionFailed when the condition isn't satisfied.
//
//   - If-None-Match: "*"      → fail if the object already exists ("create only").
//   - If-Match: "<etag>"      → fail if the current object's ETag != <etag>.
//   - If-None-Match: "<etag>" → fail if the current object's ETag == <etag>.
//   - No object exists        → any If-Match (other than absent) fails;
//     If-None-Match always passes.
func (h *S3Handler) enforcePutObjectPreconditions(
	ctx context.Context, r *http.Request, bucketName, key string,
) error {
	ifMatch := strings.Trim(r.Header.Get("If-Match"), `"`)
	ifNoneMatch := strings.Trim(r.Header.Get("If-None-Match"), `"`)
	if ifMatch == "" && ifNoneMatch == "" {
		return nil
	}

	out, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	objectExists := err == nil

	if ifNoneMatch == "*" {
		if objectExists {
			return ErrPreconditionFailed
		}

		return nil
	}

	if ifMatch != "" {
		if !objectExists {
			return ErrPreconditionFailed
		}

		currentETag := ""
		if out != nil && out.ETag != nil {
			currentETag = strings.Trim(*out.ETag, `"`)
		}
		if currentETag != ifMatch {
			return ErrPreconditionFailed
		}
	}

	if ifNoneMatch != "" && ifNoneMatch != "*" && objectExists {
		currentETag := ""
		if out != nil && out.ETag != nil {
			currentETag = strings.Trim(*out.ETag, `"`)
		}
		if currentETag == ifNoneMatch {
			return ErrPreconditionFailed
		}
	}

	return nil
}

// enforceDeleteObjectPreconditions checks If-Match on DeleteObject. Matches
// real S3 semantics: a missing object with If-Match set returns 412
// PreconditionFailed (not 404). If-None-Match isn't documented on DELETE so
// we ignore it.
func (h *S3Handler) enforceDeleteObjectPreconditions(
	ctx context.Context, r *http.Request, bucketName, key string,
) error {
	ifMatch := strings.Trim(r.Header.Get("If-Match"), `"`)
	if ifMatch == "" {
		return nil
	}

	out, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return ErrPreconditionFailed
	}

	currentETag := ""
	if out != nil && out.ETag != nil {
		currentETag = strings.Trim(*out.ETag, `"`)
	}
	if currentETag != ifMatch {
		return ErrPreconditionFailed
	}

	return nil
}

