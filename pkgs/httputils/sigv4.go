package httputils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// sigV4Algorithm is the only signing algorithm AWS SigV4 supports.
const sigV4Algorithm = "AWS4-HMAC-SHA256"

// unsignedPayload is the literal x-amz-content-sha256 value AWS clients send
// when they choose not to hash the body (streaming / chunked uploads).
const unsignedPayload = "UNSIGNED-PAYLOAD"

// emptyStringSHA256 is the hex SHA-256 of the empty string, used when a request
// has no body and no x-amz-content-sha256 header.
const emptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// SigV4Validator cryptographically verifies AWS Signature Version 4 on incoming
// requests. It is OFF by default; the caller opts in via NewSigV4Validator and
// the EchoMiddleware. When enabled, requests whose recomputed signature does not
// match the Authorization header are rejected with the AWS-accurate error.
//
// Verification re-derives the signing key from a single configured secret key
// (the access-key-id in the request is informational only — gopherstack is a
// single-tenant simulator). This mirrors how AWS validates: only the secret is
// secret; everything else is reconstructed from the request.
type SigV4Validator struct {
	// secretKey is the shared secret used to derive the signing key. Every
	// client must sign with this secret for validation to pass.
	secretKey string
}

// NewSigV4Validator builds a validator that checks signatures against secretKey.
// A blank secretKey is treated as "test" — the common AWS dummy credential — so
// the default localstack-style client (AWS_SECRET_ACCESS_KEY=test) validates.
func NewSigV4Validator(secretKey string) *SigV4Validator {
	if secretKey == "" {
		secretKey = "test"
	}

	return &SigV4Validator{secretKey: secretKey}
}

// SigV4Error is the AWS error returned when validation fails. The Code field
// drives the X-Amzn-Errortype header / error code clients expect.
type SigV4Error struct {
	Code    string
	Message string
	Status  int
}

// parsedAuthHeader holds the components extracted from an Authorization header.
type parsedAuthHeader struct {
	credential    string
	signature     string
	region        string
	service       string
	date          string // yyyymmdd (the credential-scope date)
	signedHeaders []string
}

// EchoMiddleware returns Echo middleware that validates SigV4 on every request.
// Requests without an Authorization header are passed through unchanged (many
// gopherstack internal/health/dashboard calls are unsigned); only requests that
// present a SigV4 Authorization header are verified. This keeps anonymous and
// presigned-URL flows working while still rejecting tampered signed requests.
func (v *SigV4Validator) EchoMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()

			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, sigV4Algorithm) {
				// Unsigned request (health, dashboard, presigned-query, anon) —
				// not in scope for header-based SigV4 validation.
				return next(c)
			}

			if sErr := v.Verify(r); sErr != nil {
				ctx := r.Context()
				logger.Load(ctx).DebugContext(ctx, "SigV4 validation failed",
					"code", sErr.Code, "message", sErr.Message)
				c.Response().Header().Set("X-Amzn-Errortype", sErr.Code)

				return c.JSON(sErr.Status, map[string]string{
					"__type":  sErr.Code,
					"message": sErr.Message,
				})
			}

			return next(c)
		}
	}
}

// Verify recomputes the SigV4 signature for r and compares it to the signature
// in the Authorization header. It returns nil on a match, or a *SigV4Error
// describing the AWS-accurate rejection otherwise. Verify reads and restores the
// request body so downstream handlers still see it.
func (v *SigV4Validator) Verify(r *http.Request) *SigV4Error {
	parsed, err := parseAuthorizationHeader(r.Header.Get("Authorization"))
	if err != nil {
		return err
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	switch payloadHash {
	case "":
		// No explicit content hash: hash the body (REST-JSON/XML clients) so we
		// can still build a correct canonical request.
		payloadHash = hashRequestBody(r)
	case unsignedPayload:
		// Client opted out of hashing the body; the literal is signed verbatim.
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return &SigV4Error{
			Code:    "IncompleteSignatureException",
			Message: "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header.",
			Status:  http.StatusBadRequest,
		}
	}

	canonicalReq := buildCanonicalRequest(r, parsed.signedHeaders, payloadHash)
	credentialScope := strings.Join(
		[]string{parsed.date, parsed.region, parsed.service, "aws4_request"}, "/")
	stringToSign := buildStringToSign(amzDate, credentialScope, canonicalReq)

	signingKey := deriveSigningKey(v.secretKey, parsed.date, parsed.region, parsed.service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	if !hmac.Equal([]byte(expected), []byte(parsed.signature)) {
		return &SigV4Error{
			Code: "InvalidSignatureException",
			Message: "The request signature we calculated does not match the signature you " +
				"provided. Check your AWS Secret Access Key and signing method.",
			Status: http.StatusForbidden,
		}
	}

	return nil
}

// parseAuthorizationHeader parses a SigV4 Authorization header value of the form:
//
//	AWS4-HMAC-SHA256 Credential=AKID/yyyymmdd/region/service/aws4_request, \
//	  SignedHeaders=host;x-amz-date, Signature=hex
func parseAuthorizationHeader(auth string) (parsedAuthHeader, *SigV4Error) {
	var p parsedAuthHeader

	malformed := &SigV4Error{
		Code:    "IncompleteSignatureException",
		Message: "Authorization header requires 'Credential', 'Signature' and 'SignedHeaders' parameters.",
		Status:  http.StatusBadRequest,
	}

	rest := strings.TrimSpace(strings.TrimPrefix(auth, sigV4Algorithm))
	for field := range strings.SplitSeq(rest, ",") {
		field = strings.TrimSpace(field)
		key, val, found := strings.Cut(field, "=")
		if !found {
			continue
		}

		switch strings.TrimSpace(key) {
		case "Credential":
			p.credential = strings.TrimSpace(val)
		case "SignedHeaders":
			for h := range strings.SplitSeq(strings.TrimSpace(val), ";") {
				if h != "" {
					p.signedHeaders = append(p.signedHeaders, strings.ToLower(h))
				}
			}
		case "Signature":
			p.signature = strings.TrimSpace(val)
		}
	}

	if p.credential == "" || p.signature == "" || len(p.signedHeaders) == 0 {
		return p, malformed
	}

	// Credential scope: AKID/date/region/service/aws4_request.
	scope := parseValidSigV4Scope(p.credential)
	if scope == nil {
		return p, malformed
	}

	p.date = scope[sigV4DateIndex]
	p.region = scope[sigV4RegionIndex]
	p.service = scope[sigV4ServiceIndex]
	sort.Strings(p.signedHeaders)

	return p, nil
}

// buildCanonicalRequest constructs the raw SigV4 canonical request string (the
// string that buildStringToSign then hashes — it is not pre-hashed here).
func buildCanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) string {
	var b strings.Builder

	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r.URL))
	b.WriteByte('\n')
	b.WriteString(canonicalQueryString(r.URL))
	b.WriteByte('\n')

	for _, h := range signedHeaders {
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(canonicalHeaderValue(r, h))
		b.WriteByte('\n')
	}

	b.WriteByte('\n')
	b.WriteString(strings.Join(signedHeaders, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)

	return b.String()
}

// canonicalHeaderValue returns the trimmed value AWS uses for a signed header.
// The synthetic "host" header is taken from r.Host (Go strips it from Header).
func canonicalHeaderValue(r *http.Request, h string) string {
	switch h {
	case "host":
		return strings.TrimSpace(r.Host)
	case "content-length":
		// Go keeps Content-Length in r.ContentLength, not the header map.
		if r.ContentLength >= 0 && r.Header.Get("Content-Length") == "" {
			return strconv.FormatInt(r.ContentLength, 10)
		}
	}

	values := r.Header.Values(http.CanonicalHeaderKey(h))
	trimmed := make([]string, 0, len(values))
	for _, v := range values {
		trimmed = append(trimmed, strings.Join(strings.Fields(v), " "))
	}

	return strings.Join(trimmed, ",")
}

// canonicalURI returns the URI-encoded path per the SigV4 spec. AWS double-
// encodes for every service except S3; gopherstack signs against the path as
// the client did, so we encode each segment once which matches the AWS SDKs'
// default canonicalisation for the JSON/query protocols used here.
func canonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}

	return path
}

// canonicalQueryString returns the sorted, encoded query string.
func canonicalQueryString(u *url.URL) string {
	values := u.Query()
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		vals := values[k]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, awsURIEncode(k)+"="+awsURIEncode(v))
		}
	}

	return strings.Join(parts, "&")
}

// awsURIEncode percent-encodes per RFC 3986 the way SigV4 requires (unreserved
// chars left as-is, space as %20, slash kept literal is not applied here since
// query values must encode every reserved char).
func awsURIEncode(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"

	var b strings.Builder
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)

			continue
		}

		b.WriteByte('%')
		const hexDigits = "0123456789ABCDEF"
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}

	return b.String()
}

// buildStringToSign assembles the SigV4 string-to-sign.
func buildStringToSign(amzDate, credentialScope, canonicalRequest string) string {
	hashed := sha256.Sum256([]byte(canonicalRequest))

	return strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		credentialScope,
		hex.EncodeToString(hashed[:]),
	}, "\n")
}

// deriveSigningKey derives the SigV4 signing key from the secret.
func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)

	return hmacSHA256(kService, "aws4_request")
}

// hmacSHA256 returns HMAC-SHA256(key, data).
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))

	return h.Sum(nil)
}

// hashRequestBody reads, hashes, and restores the request body, returning the
// hex SHA-256. An empty body hashes to emptyStringSHA256.
func hashRequestBody(r *http.Request) string {
	body, err := ReadBody(r)
	if err != nil || len(body) == 0 {
		return emptyStringSHA256
	}

	sum := sha256.Sum256(body)

	return hex.EncodeToString(sum[:])
}
