package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// publicCannedACLs is the set of canned ACL strings that grant access to
// anonymous or all-authenticated users. PutBucketAcl / PutObjectAcl with one
// of these names is rejected when BlockPublicAcls is set on the bucket's
// Public Access Block configuration.
var publicCannedACLs = map[string]struct{}{ //nolint:gochecknoglobals // lookup table
	aclPublicRead:        {},
	aclPublicReadWrite:   {},
	aclAuthenticatedRead: {},
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
	ownership, ownershipErr := h.Backend.GetBucketOwnershipControls(ctx, bucketName)
	if ownershipErr == nil && ownershipIsBucketOwnerEnforced(ownership) {
		return ErrAccessDenied
	}

	pab, ok := loadPublicAccessBlock(ctx, h.Backend, bucketName)
	if !ok || !pab.BlockPublicAcls {
		// No PAB set / unparseable / not blocking ACLs → defer to the
		// BucketOwnerEnforced check above only.
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

// loadPublicAccessBlock fetches the bucket's PublicAccessBlock configuration
// and decodes it. The bool return is true only when both the fetch and the
// XML decode succeeded — a `false` indicates the caller should treat the
// bucket as having no PAB applied. Centralising the "missing PAB falls open"
// rule here means the call sites stay free of nilerr-pattern early returns
// that the linter (rightly) flags as suspicious.
func loadPublicAccessBlock(
	ctx context.Context, backend StorageBackend, bucketName string,
) (PublicAccessBlockConfiguration, bool) {
	pabXML, err := backend.GetPublicAccessBlock(ctx, bucketName)
	if err != nil || pabXML == "" {
		return PublicAccessBlockConfiguration{}, false
	}

	var pab PublicAccessBlockConfiguration
	if uErr := xml.Unmarshal([]byte(pabXML), &pab); uErr != nil {
		return PublicAccessBlockConfiguration{}, false
	}

	return pab, true
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
	pab, ok := loadPublicAccessBlock(ctx, h.Backend, bucketName)
	if !ok || !pab.BlockPublicPolicy {
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
	currentETag := currentETagOf(out)

	return evaluatePutPreconditions(ifMatch, ifNoneMatch, currentETag, objectExists)
}

// evaluatePutPreconditions applies the If-Match / If-None-Match decision
// table without doing any I/O. Extracted from enforcePutObjectPreconditions
// purely to keep cyclomatic complexity below the linter cap.
func evaluatePutPreconditions(ifMatch, ifNoneMatch, currentETag string, objectExists bool) error {
	if ifNoneMatch == "*" {
		if objectExists {
			return ErrPreconditionFailed
		}

		return nil
	}

	if ifMatch != "" {
		if !objectExists || currentETag != ifMatch {
			return ErrPreconditionFailed
		}
	}

	if ifNoneMatch != "" && ifNoneMatch != "*" && objectExists && currentETag == ifNoneMatch {
		return ErrPreconditionFailed
	}

	return nil
}

// currentETagOf strips the surrounding quotes from a HeadObject result so it
// can be compared verbatim against the trimmed If-* header values.
func currentETagOf(out *s3.HeadObjectOutput) string {
	if out == nil || out.ETag == nil {
		return ""
	}

	return strings.Trim(*out.ETag, `"`)
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

	if currentETagOf(out) != ifMatch {
		return ErrPreconditionFailed
	}

	return nil
}

// enforceRenameDestinationPreconditions checks RenameObject's four
// DestinationIf* conditionals against the rename target, which may or may
// not already exist. See evaluateDestinationConditionals for header names
// and per-condition semantics.
func (h *S3Handler) enforceRenameDestinationPreconditions(
	ctx context.Context, r *http.Request, bucketName, key string,
) error {
	if r.Header.Get(standardConditionals.ifMatch) == "" &&
		r.Header.Get(standardConditionals.ifNoneMatch) == "" &&
		r.Header.Get(standardConditionals.ifModifiedSince) == "" &&
		r.Header.Get(standardConditionals.ifUnmodifiedSince) == "" {
		return nil
	}

	out, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	exists := err == nil

	var etag string
	var lastModified time.Time
	if exists {
		etag = aws.ToString(out.ETag)
		lastModified = aws.ToTime(out.LastModified)
	}

	if _, ok := evaluateDestinationConditionals(r.Header, etag, lastModified, exists); !ok {
		return ErrPreconditionFailed
	}

	return nil
}
