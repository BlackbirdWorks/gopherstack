package s3

import (
	"context"
	"io"
	"net/http"
)

// Exported wrappers exposing auth / authorization internals to the external
// s3_test package. These exist only in test builds.

// Streaming / chunked encoding.
const (
	StreamingSignedPayload         = streamingSignedPayload
	StreamingSignedPayloadTrailer  = streamingSignedPayloadTrailer
	StreamingUnsignedTrailer       = streamingUnsignedTrailer
	ContentSHA256HeaderName        = contentSHA256HeaderName
	DecodedContentLengthHeaderName = decodedContentLengthHeaderName
	UnsignedPayloadHash            = unsignedPayloadHash
)

// SigV4 / SigV2 header auth.
const (
	EmptyStringSHA256    = emptyStringSHA256
	PresignedDateFormat  = presignedDateFormat
	ErrSignatureMismatch = errSignatureMismatch
	ActionGetObjectLower = actionGetObjectLower
	ErrAuthQueryParams   = errAuthQueryParams
)

// ACL grant expansion.
const (
	PermFullControl       = permFullControl
	PermRead              = permRead
	PermWrite             = permWrite
	PermReadACP           = permReadACP
	URIAllUsers           = uriAllUsers
	URIAuthenticatedUsers = uriAuthenticatedUsers
	URILogDelivery        = uriLogDelivery
	XsiTypeCanonicalUser  = xsiTypeCanonicalUser
	ACLPublicRead         = aclPublicRead
	ACLPublicReadWrite    = aclPublicReadWrite
	ACLAuthenticatedRead  = aclAuthenticatedRead
	ACLLogDeliveryWrite   = aclLogDeliveryWrite
)

// Policy-evaluation effect codes (mirrors the internal policyEffect enum).
const (
	EffectDefault = int(effectDefault)
	EffectAllow   = int(effectAllow)
	EffectDeny    = int(effectDeny)
)

// NewChunkedReadCloser exposes newChunkedReadCloser for external tests.
func NewChunkedReadCloser(body io.ReadCloser) io.ReadCloser {
	return newChunkedReadCloser(body)
}

// IsAWSChunkedRequest exposes isAWSChunkedRequest for external tests.
func IsAWSChunkedRequest(r *http.Request) bool {
	return isAWSChunkedRequest(r)
}

// MaybeDecodeChunkedBody exposes maybeDecodeChunkedBody for external tests.
func MaybeDecodeChunkedBody(r *http.Request) *http.Request {
	return maybeDecodeChunkedBody(r)
}

// CannedACLGrants exposes cannedACLGrants for external tests.
func CannedACLGrants(canned, ownerID, ownerName string) []Grant {
	return cannedACLGrants(canned, ownerID, ownerName)
}

// WildcardMatch exposes wildcardMatch for external tests.
func WildcardMatch(pattern, s string) bool {
	return wildcardMatch(pattern, s)
}

// IsPresignedRequestForTest exposes isPresignedRequest for external tests.
func IsPresignedRequestForTest(r *http.Request) bool {
	return isPresignedRequest(r)
}

// VerifyRequestDate exposes verifyRequestDate for external tests.
func VerifyRequestDate(r *http.Request) bool {
	return verifyRequestDate(r)
}

// ParsedAuthHeader is the external-test view of a parsed Authorization header.
type ParsedAuthHeader struct {
	AccessKeyID string
	Region      string
	OK          bool
	IsSigV2     bool
}

// ParseAuthorizationHeaderForTest parses an Authorization header for external
// tests, flattening the internal authScope into exported fields.
func ParseAuthorizationHeaderForTest(header string) ParsedAuthHeader {
	scope, ok := parseAuthorizationHeader(header)

	return ParsedAuthHeader{
		OK:          ok,
		IsSigV2:     scope.isSigV2,
		AccessKeyID: scope.accessKeyID,
		Region:      scope.region,
	}
}

// VerifyHeaderAuth exposes verifyHeaderAuth for external tests.
func (h *S3Handler) VerifyHeaderAuth(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) bool {
	return h.verifyHeaderAuth(ctx, w, r)
}

// EvaluateBucketPolicyForTest exposes evaluateBucketPolicy for external tests,
// mapping the action string to the internal s3Action and using an empty PAB.
func (h *S3Handler) EvaluateBucketPolicyForTest(
	ctx context.Context, bucket, key, action string, anonymous bool,
) int {
	return int(h.evaluateBucketPolicy(
		ctx, bucket, key, s3Action(action), caller{anonymous: anonymous},
		PublicAccessBlockConfiguration{},
	))
}

// Action names used by external policy-evaluation tests.
const (
	ActionGetObject = string(actionGetObject)
)
