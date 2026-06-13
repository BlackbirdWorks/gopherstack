package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// presignUnsignedPayload is the payload hash AWS SDKs use when presigning S3
// URLs (the body is not known at signing time).
const presignUnsignedPayload = "UNSIGNED-PAYLOAD"

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
func (h *S3Handler) validatePresignedRequest(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) bool {
	q := r.URL.Query()

	// Verify all required query parameters are present and non-empty.
	algorithm := q.Get("X-Amz-Algorithm")
	credential := q.Get("X-Amz-Credential")
	dateStr := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")

	if algorithm == "" || credential == "" || dateStr == "" || expiresStr == "" ||
		signedHeaders == "" ||
		signature == "" {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code: errAuthQueryParams,
			Message: "Query-string authentication requires the X-Amz-Algorithm, X-Amz-Credential, " +
				"X-Amz-Date, X-Amz-Expires, X-Amz-SignedHeaders, and X-Amz-Signature parameters.",
		}, http.StatusBadRequest)

		return false
	}

	// Validate the signing algorithm.
	if algorithm != presignedAlgorithm {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errAuthQueryParams,
			Message: "X-Amz-Algorithm must be AWS4-HMAC-SHA256.",
		}, http.StatusBadRequest)

		return false
	}

	// Validate credential format: AKID/date/region/service/aws4_request
	credParts := strings.Split(credential, "/")
	if len(credParts) < minPresignCredentialParts {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errAuthQueryParams,
			Message: "X-Amz-Credential is not well-formed.",
		}, http.StatusBadRequest)

		return false
	}

	signedAt, err := time.Parse(presignedDateFormat, dateStr)
	if err != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errAuthQueryParams,
			Message: "X-Amz-Date must be in the ISO 8601 basic format.",
		}, http.StatusBadRequest)

		return false
	}

	expires, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil || expires <= 0 {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errAuthQueryParams,
			Message: "X-Amz-Expires must be a positive integer.",
		}, http.StatusBadRequest)

		return false
	}

	expiresAt := signedAt.Add(time.Duration(expires) * time.Second)

	if time.Now().UTC().After(expiresAt) {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errAccessDenied,
			Message: "Request has expired.",
		}, http.StatusForbidden)

		return false
	}

	// Opt-in cryptographic signature verification. Off by default (empty
	// secret) so presigned URLs are accepted on structure+expiry alone.
	if h.PresignSecret != "" &&
		!h.verifyPresignedSignature(r, credParts, dateStr, signedHeaders, signature) {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code: errAccessDenied,
			Message: "The request signature we calculated does not match the signature you " +
				"provided. Check your key and signing method.",
		}, http.StatusForbidden)

		return false
	}

	return true
}

// verifyPresignedSignature recomputes the SigV4 query-auth signature for r and
// reports whether it matches the X-Amz-Signature the client provided. The
// credential scope (date/region/service) is taken from the supplied X-Amz-
// Credential parts; the signing key is derived from the handler's configured
// secret. Returns true on a match.
func (h *S3Handler) verifyPresignedSignature(
	r *http.Request,
	credParts []string,
	amzDate, signedHeaders, providedSig string,
) bool {
	scopeDate := credParts[1]
	region := credParts[2]
	service := credParts[3]

	headerNames := strings.Split(signedHeaders, ";")
	sort.Strings(headerNames)

	canonicalReq := h.buildPresignCanonicalRequest(r, headerNames)
	credentialScope := strings.Join([]string{scopeDate, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		presignedAlgorithm,
		amzDate,
		credentialScope,
		hexSHA256(canonicalReq),
	}, "\n")

	signingKey := derivePresignSigningKey(h.PresignSecret, scopeDate, region, service)
	expected := hex.EncodeToString(hmacSHA256Bytes(signingKey, stringToSign))

	return hmac.Equal([]byte(expected), []byte(providedSig))
}

// buildPresignCanonicalRequest builds the SigV4 canonical request for a
// presigned (query-auth) S3 request. The X-Amz-Signature parameter is excluded
// from the canonical query string and the payload hash is the literal
// UNSIGNED-PAYLOAD that S3 presigning uses.
func (h *S3Handler) buildPresignCanonicalRequest(r *http.Request, signedHeaders []string) string {
	var b strings.Builder

	b.WriteString(r.Method)
	b.WriteByte('\n')

	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	b.WriteString(path)
	b.WriteByte('\n')

	b.WriteString(presignCanonicalQuery(r.URL))
	b.WriteByte('\n')

	for _, name := range signedHeaders {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(presignHeaderValue(r, name))
		b.WriteByte('\n')
	}

	b.WriteByte('\n')
	b.WriteString(strings.Join(signedHeaders, ";"))
	b.WriteByte('\n')
	b.WriteString(presignUnsignedPayload)

	return b.String()
}

// presignCanonicalQuery returns the sorted, percent-encoded query string with
// the X-Amz-Signature parameter removed (it is not part of what was signed).
func presignCanonicalQuery(u *url.URL) string {
	values := u.Query()
	values.Del("X-Amz-Signature")

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		vals := values[k]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, presignURIEncode(k)+"="+presignURIEncode(v))
		}
	}

	return strings.Join(parts, "&")
}

// presignHeaderValue returns the canonical value for a signed header. The
// synthetic "host" header is taken from r.Host (Go strips it from the map).
func presignHeaderValue(r *http.Request, name string) string {
	if name == "host" {
		return strings.TrimSpace(r.Host)
	}

	values := r.Header.Values(http.CanonicalHeaderKey(name))
	trimmed := make([]string, 0, len(values))
	for _, v := range values {
		trimmed = append(trimmed, strings.Join(strings.Fields(v), " "))
	}

	return strings.Join(trimmed, ",")
}

// presignURIEncode percent-encodes per the RFC 3986 rules SigV4 requires.
func presignURIEncode(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"

	var b strings.Builder
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)

			continue
		}

		const hexDigits = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}

	return b.String()
}

// derivePresignSigningKey derives the SigV4 signing key from the secret.
func derivePresignSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256Bytes([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256Bytes(kDate, region)
	kService := hmacSHA256Bytes(kRegion, service)

	return hmacSHA256Bytes(kService, "aws4_request")
}

// hmacSHA256Bytes returns HMAC-SHA256(key, data).
func hmacSHA256Bytes(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))

	return mac.Sum(nil)
}

// hexSHA256 returns the hex-encoded SHA-256 of s.
func hexSHA256(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])
}
