package gcpstorage

import (
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // V2 HMAC signing is defined with HMAC-SHA1
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	algRSA          = "GOOG4-RSA-SHA256"
	algHMAC         = "GOOG4-HMAC-SHA256"
	v4DateLayout    = "20060102T150405Z"
	v4ScopeDate     = "20060102"
	maxSignedExpiry = 604800
	clockSkew       = 15 * time.Minute
	scopeParts      = 4
	defaultSignHost = "storage.googleapis.com"
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// KeyResolver returns the RSA public keys of a service account (by email) for signed-URL verification.
type KeyResolver func(email string) []*rsa.PublicKey

// SetKeyResolver wires the service-account key lookup used to verify GOOG4-RSA-SHA256 and V2 signatures.
func (b *InMemoryBackend) SetKeyResolver(fn KeyResolver) {
	b.mu.Lock("SetKeyResolver")
	defer b.mu.Unlock()

	b.keys = fn
}

func (b *InMemoryBackend) publicKeys(email string) []*rsa.PublicKey {
	b.mu.RLock("publicKeys")
	fn := b.keys
	b.mu.RUnlock()

	if fn == nil {
		return nil
	}

	return fn(email)
}

func signErr(status int, code, msg string) *APIError {
	return &APIError{Status: status, Reason: code, XMLCode: code, Message: msg}
}

func errSignature() *APIError {
	return signErr(http.StatusForbidden, "SignatureDoesNotMatch",
		"The request signature we calculated does not match the signature you provided. "+
			"Check your Google secret key and signing method.")
}

func errInvalidSecurity() *APIError {
	return signErr(http.StatusForbidden, "InvalidSecurity", "The security credentials you provided are invalid.")
}

func errInvalidArg(msg string) *APIError {
	return signErr(http.StatusBadRequest, "InvalidArgument", msg)
}

func errExpired() *APIError {
	return signErr(http.StatusBadRequest, "ExpiredToken", "The signed URL has expired.")
}

// authenticate verifies a signed URL when the request carries one, and rejects anonymous requests when required.
func (h *Handler) authenticate(r *http.Request) *APIError {
	q := r.URL.Query()

	switch {
	case q.Has("X-Goog-Signature"):
		return h.Backend.verifyV4(r, q)
	case q.Has("GoogleAccessId") && q.Has("Signature"):
		return h.Backend.verifyV2(r, q)
	case h.RequireAuth && r.Header.Get("Authorization") == "":
		return &APIError{
			Status: http.StatusForbidden, Reason: "required", XMLCode: codeAccessDenied,
			Message: "Anonymous caller does not have access; sign the request or send credentials.",
		}
	default:
		return nil
	}
}

func v4CanonicalQuery(q url.Values) string {
	c := url.Values{}

	for k, v := range q {
		if k != "X-Goog-Signature" {
			c[k] = v
		}
	}

	return strings.ReplaceAll(c.Encode(), "+", "%20")
}

func v4CanonicalURI(r *http.Request) string {
	segs := strings.Split(r.URL.Path, "/")
	for i, s := range segs {
		segs[i] = strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
	}

	return strings.Join(segs, "/")
}

func hostCandidates(r *http.Request) []string {
	host := r.Host
	name := host

	if i := strings.LastIndex(host, ":"); i > 0 && !strings.Contains(host[i:], "]") {
		name = host[:i]
	}

	return slices.Compact([]string{host, name, defaultSignHost})
}

func v4CanonicalRequest(r *http.Request, q url.Values, signed []string, host string) string {
	var hdrs strings.Builder

	payload := unsignedPayload

	for _, name := range signed {
		val := host
		if name != "host" {
			vals := r.Header.Values(name)
			for i := range vals {
				vals[i] = strings.Join(strings.Fields(vals[i]), " ")
			}

			val = strings.Join(vals, ",")
		}

		hdrs.WriteString(name + ":" + val + "\n")

		if name == "x-goog-content-sha256" {
			payload = val
		}
	}

	return strings.Join([]string{
		r.Method, v4CanonicalURI(r), v4CanonicalQuery(q), hdrs.String(), strings.Join(signed, ";"), payload,
	}, "\n")
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))

	return m.Sum(nil)
}

type v4Params struct {
	when    time.Time
	alg     string
	id      string
	date    string
	loc     string
	signed  []string
	sig     []byte
	expires time.Duration
}

func parseV4(q url.Values) (v4Params, *APIError) {
	p := v4Params{alg: q.Get("X-Goog-Algorithm"), date: q.Get("X-Goog-Date")}

	if p.alg != algRSA && p.alg != algHMAC {
		return p, errInvalidArg("Invalid X-Goog-Algorithm.")
	}

	when, err := time.Parse(v4DateLayout, p.date)
	if err != nil {
		return p, errInvalidArg("Invalid X-Goog-Date.")
	}

	secs, err := strconv.ParseInt(q.Get("X-Goog-Expires"), 10, 64)
	if err != nil || secs < 1 || secs > maxSignedExpiry {
		return p, errInvalidArg("X-Goog-Expires must be between 1 and 604800 seconds.")
	}

	id, scope, _ := strings.Cut(q.Get("X-Goog-Credential"), "/")
	parts := strings.Split(scope, "/")

	if id == "" || len(parts) != scopeParts || parts[0] != when.Format(v4ScopeDate) ||
		parts[2] != "storage" || parts[3] != "goog4_request" {
		return p, errInvalidArg("Invalid X-Goog-Credential.")
	}

	if p.sig, err = hex.DecodeString(q.Get("X-Goog-Signature")); err != nil {
		return p, errSignature()
	}

	p.signed = strings.Split(q.Get("X-Goog-SignedHeaders"), ";")
	if !slices.Contains(p.signed, "host") || !slices.IsSorted(p.signed) {
		return p, errInvalidArg("X-Goog-SignedHeaders must be sorted and include host.")
	}

	p.id, p.loc, p.when, p.expires = id, parts[1], when, time.Duration(secs)*time.Second

	return p, nil
}

func (b *InMemoryBackend) verifyV4(r *http.Request, q url.Values) *APIError {
	p, apiErr := parseV4(q)
	if apiErr != nil {
		return apiErr
	}

	now := b.now()

	switch {
	case now.Before(p.when.Add(-clockSkew)):
		return signErr(http.StatusForbidden, "RequestTimeTooSkewed", "The signed URL is not valid yet.")
	case now.After(p.when.Add(p.expires)):
		return errExpired()
	}

	scope := strings.Join([]string{p.when.Format(v4ScopeDate), p.loc, "storage", "goog4_request"}, "/")

	for _, host := range hostCandidates(r) {
		canon := v4CanonicalRequest(r, q, p.signed, host)
		sum := sha256.Sum256([]byte(canon))
		toSign := p.alg + "\n" + p.date + "\n" + scope + "\n" + hex.EncodeToString(sum[:])

		ok, known := b.checkV4Signature(p, toSign)
		if !known {
			return errInvalidSecurity()
		}

		if ok {
			return nil
		}
	}

	return errSignature()
}

func (b *InMemoryBackend) checkV4Signature(p v4Params, toSign string) (bool, bool) {
	if p.alg == algHMAC {
		secret, ok := b.activeHMACSecret(p.id)
		if !ok {
			return false, false
		}

		k := hmacSHA256([]byte("GOOG4"+secret), p.when.Format(v4ScopeDate))
		k = hmacSHA256(k, p.loc)
		k = hmacSHA256(k, "storage")
		k = hmacSHA256(k, "goog4_request")

		return hmac.Equal(hmacSHA256(k, toSign), p.sig), true
	}

	keys := b.publicKeys(p.id)
	sum := sha256.Sum256([]byte(toSign))

	for _, pub := range keys {
		if rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], p.sig) == nil {
			return true, true
		}
	}

	return false, len(keys) > 0
}

func v2ExtensionHeaders(r *http.Request) string {
	var names []string

	for k := range r.Header {
		l := strings.ToLower(k)
		if strings.HasPrefix(l, "x-goog-") && l != "x-goog-encryption-key" && l != "x-goog-encryption-key-sha256" {
			names = append(names, l)
		}
	}

	slices.Sort(names)

	var sb strings.Builder

	for _, n := range names {
		vals := r.Header.Values(n)
		for i := range vals {
			vals[i] = strings.TrimSpace(vals[i])
		}

		sb.WriteString(n + ":" + strings.Join(vals, ",") + "\n")
	}

	return sb.String()
}

func (b *InMemoryBackend) verifyV2(r *http.Request, q url.Values) *APIError {
	exp, err := strconv.ParseInt(q.Get("Expires"), 10, 64)
	if err != nil {
		return errInvalidArg("Invalid Expires.")
	}

	if b.now().Unix() > exp {
		return errExpired()
	}

	sig, err := base64.StdEncoding.DecodeString(q.Get("Signature"))
	if err != nil {
		return errSignature()
	}

	id := q.Get("GoogleAccessId")
	toSign := strings.Join([]string{
		r.Method, r.Header.Get("Content-MD5"), r.Header.Get("Content-Type"), strconv.FormatInt(exp, 10),
	}, "\n") + "\n" + v2ExtensionHeaders(r) + r.URL.EscapedPath()

	if secret, ok := b.activeHMACSecret(id); ok {
		m := hmac.New(sha1.New, []byte(secret))
		m.Write([]byte(toSign))

		if hmac.Equal(m.Sum(nil), sig) {
			return nil
		}

		return errSignature()
	}

	keys := b.publicKeys(id)
	if len(keys) == 0 {
		return errInvalidSecurity()
	}

	sum := sha256.Sum256([]byte(toSign))

	for _, pub := range keys {
		if rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig) == nil {
			return nil
		}
	}

	return errSignature()
}
