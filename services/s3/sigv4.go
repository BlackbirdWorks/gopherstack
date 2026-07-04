package s3

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Header-based AWS authentication (SigV4 and legacy SigV2). This mirrors the
// query-string (presigned) verification in presign.go but for the Authorization
// header carried by ordinary signed requests. Like presign verification it is
// opt-in: enforcement only happens when the handler has a PresignSecret set
// (via WithPresignValidation). With no secret configured the Authorization
// header is accepted on structure alone, preserving backwards-compatible
// behaviour for the many callers that sign with throwaway credentials.

const (
	sigV4Algorithm      = "AWS4-HMAC-SHA256"
	sigV2AuthPrefix     = "AWS "
	sigV4AuthPrefix     = sigV4Algorithm + " "
	emptyStringSHA256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayloadHash = "UNSIGNED-PAYLOAD"
	// maxClockSkew bounds how far the request date may drift from now before a
	// signed request is rejected as expired, matching AWS's 15-minute window.
	maxClockSkew = 15 * time.Minute
)

// authScope captures the parsed pieces of an AWS Authorization header needed to
// recompute a signature.
type authScope struct {
	accessKeyID   string
	date          string // yyyymmdd credential-scope date
	region        string
	service       string
	signature     string
	signedHeaders []string
	isSigV2       bool
}

// verifyHeaderAuth validates the Authorization header on r. It writes an S3
// error response and returns false when the signature is present but invalid.
// A request with no Authorization header is treated as anonymous and passes
// this gate (data-plane authorization decides whether anonymous access is
// allowed); a malformed header, an unsupported algorithm, an expired request,
// or a signature mismatch is rejected with SignatureDoesNotMatch / AccessDenied.
func (h *S3Handler) verifyHeaderAuth(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) bool {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return true // anonymous; handled by authorizeObjectAccess
	}

	scope, ok := parseAuthorizationHeader(authHeader)
	if !ok {
		h.writeSignatureError(ctx, w, r, "Authorization header is malformed.")

		return false
	}

	// SigV2 uses an entirely different (SHA1) canonicalisation. We recognise it
	// so real SigV2 clients aren't silently mis-verified, but we only structure-
	// check it (the SDKs in this stack all sign SigV4); an empty/garbled SigV2
	// header is still rejected.
	if scope.isSigV2 {
		if scope.accessKeyID == "" || scope.signature == "" {
			h.writeSignatureError(ctx, w, r, "Authorization header is malformed.")

			return false
		}

		return true
	}

	if scope.service != "s3" && scope.service != "s3-object-lambda" {
		h.writeSignatureError(ctx, w, r, "Credential should be scoped to correct service: s3.")

		return false
	}

	// Off by default: only the credential scope (used for region routing) is
	// required unless a secret is configured. Many callers in this stack sign
	// with a Credential-only header purely to convey the region scope.
	if h.PresignSecret == "" {
		return true
	}

	if len(scope.signedHeaders) == 0 || scope.signature == "" {
		h.writeSignatureError(ctx, w, r, "Authorization header requires SignedHeaders and Signature.")

		return false
	}

	if !verifyRequestDate(r) {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "RequestTimeTooSkewed",
			Message: "The difference between the request time and the current time is too large.",
		}, http.StatusForbidden)

		return false
	}

	if !h.signatureMatches(r, scope) {
		h.writeSignatureError(ctx, w, r,
			"The request signature we calculated does not match the signature you provided. "+
				"Check your key and signing method.")

		return false
	}

	return true
}

// writeSignatureError emits a SignatureDoesNotMatch 403 with the given message.
func (h *S3Handler) writeSignatureError(
	ctx context.Context, w http.ResponseWriter, r *http.Request, msg string,
) {
	httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
		Code:    errSignatureMismatch,
		Message: msg,
	}, http.StatusForbidden)
}

// parseAuthorizationHeader parses either a SigV4 or a SigV2 Authorization
// header into an authScope. The bool return is false when the header does not
// begin with a recognised scheme or is missing required fields.
func parseAuthorizationHeader(header string) (authScope, bool) {
	if tail, ok := strings.CutPrefix(header, sigV4AuthPrefix); ok {
		return parseSigV4Header(tail)
	}

	if tail, ok := strings.CutPrefix(header, sigV2AuthPrefix); ok {
		return parseSigV2Header(tail)
	}

	return authScope{}, false
}

// parseSigV4Header parses the comma-separated
// "Credential=.../SignedHeaders=.../Signature=..." tail of a SigV4 header.
func parseSigV4Header(tail string) (authScope, bool) {
	var (
		scope         authScope
		credential    string
		signedHeaders string
	)

	for part := range strings.SplitSeq(tail, ",") {
		part = strings.TrimSpace(part)
		key, val, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		switch key {
		case "Credential":
			credential = val
		case "SignedHeaders":
			signedHeaders = val
		case "Signature":
			scope.signature = val
		}
	}

	credParts := strings.Split(credential, "/")
	if len(credParts) < minPresignCredentialParts {
		return authScope{}, false
	}

	scope.accessKeyID = credParts[0]
	scope.date = credParts[1]
	scope.region = credParts[2]
	scope.service = credParts[3]
	if signedHeaders != "" {
		scope.signedHeaders = strings.Split(signedHeaders, ";")
		sort.Strings(scope.signedHeaders)
	}

	return scope, true
}

// parseSigV2Header parses the legacy "AWS <AccessKeyId>:<Signature>" form.
func parseSigV2Header(tail string) (authScope, bool) {
	akid, sig, found := strings.Cut(tail, ":")
	if !found {
		return authScope{}, false
	}

	return authScope{accessKeyID: akid, signature: sig, isSigV2: true}, true
}

// verifyRequestDate checks the request timestamp (X-Amz-Date, falling back to
// Date) is within the allowed clock skew. A missing date passes (the SDKs may
// omit it in some flows); a present but far-off date is rejected.
func verifyRequestDate(r *http.Request) bool {
	raw := r.Header.Get("X-Amz-Date")
	if raw == "" {
		raw = r.Header.Get("Date")
	}
	if raw == "" {
		return true
	}

	t, ok := parseAmzTime(raw)
	if !ok {
		return true // unparseable date is not our job to reject here
	}

	skew := time.Since(t)
	if skew < 0 {
		skew = -skew
	}

	return skew <= maxClockSkew
}

// parseAmzTime parses the AWS basic ISO-8601 and RFC1123 date forms.
func parseAmzTime(raw string) (time.Time, bool) {
	for _, layout := range []string{presignedDateFormat, http.TimeFormat, time.RFC1123} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// signatureMatches recomputes the SigV4 header signature for r and compares it
// against the client-provided signature in constant time.
func (h *S3Handler) signatureMatches(r *http.Request, scope authScope) bool {
	canonicalReq := buildHeaderCanonicalRequest(r, scope.signedHeaders)
	amzDate := r.Header.Get("X-Amz-Date")
	credentialScope := strings.Join(
		[]string{scope.date, scope.region, scope.service, "aws4_request"}, "/")

	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		credentialScope,
		hexSHA256(canonicalReq),
	}, "\n")

	signingKey := derivePresignSigningKey(h.PresignSecret, scope.date, scope.region, scope.service)
	expected := hex.EncodeToString(hmacSHA256Bytes(signingKey, stringToSign))

	return hmac.Equal([]byte(expected), []byte(scope.signature))
}

// buildHeaderCanonicalRequest builds the SigV4 canonical request for a
// header-signed S3 request. Unlike the presigned form, the payload hash comes
// from the x-amz-content-sha256 header the client signed.
func buildHeaderCanonicalRequest(r *http.Request, signedHeaders []string) string {
	var b strings.Builder

	b.WriteString(r.Method)
	b.WriteByte('\n')

	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	b.WriteString(path)
	b.WriteByte('\n')

	b.WriteString(headerCanonicalQuery(r.URL))
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
	b.WriteString(headerPayloadHash(r))

	return b.String()
}

// headerCanonicalQuery returns the sorted, percent-encoded canonical query
// string (no parameter is excluded for header auth).
func headerCanonicalQuery(u *url.URL) string {
	values := u.Query()
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

// headerPayloadHash returns the payload hash the client used when signing:
// the literal x-amz-content-sha256 value (which may be UNSIGNED-PAYLOAD or a
// STREAMING-* sentinel), defaulting to the empty-body hash when absent.
func headerPayloadHash(r *http.Request) string {
	if v := r.Header.Get(contentSHA256HeaderName); v != "" {
		return v
	}

	return emptyStringSHA256
}
