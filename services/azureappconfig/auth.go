package azureappconfig

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"slices"
	"strings"
	"time"
)

const (
	hmacScheme   = "HMAC-SHA256 "
	bearerScheme = "Bearer "
	signedDate   = "x-ms-date"
	signedHTTP   = "date"
	signedHost   = "host"
	signedHash   = "x-ms-content-sha256"
	hdrDate      = "X-Ms-Date"
	hdrHTTPDate  = "Date"
	hdrContent   = "X-Ms-Content-Sha256"
	maxClockSkew = 15 * time.Minute
	jwtParts     = 3
)

type hmacHeader struct {
	credential string
	signature  string
	signed     []string
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func parseHMACHeader(auth string) (hmacHeader, bool) {
	if !hasPrefixFold(auth, hmacScheme) {
		return hmacHeader{}, false
	}

	var out hmacHeader

	fields := strings.FieldsFunc(auth[len(hmacScheme):], func(r rune) bool { return r == '&' || r == ',' || r == ' ' })
	for _, f := range fields {
		name, value, _ := strings.Cut(f, "=")

		switch strings.ToLower(name) {
		case "credential":
			out.credential = value
		case "signedheaders":
			out.signed = strings.Split(strings.ToLower(value), ";")
		case "signature":
			out.signature = value
		}
	}

	return out, out.credential != "" && out.signature != "" && len(out.signed) > 0
}

// credentialID returns the HMAC credential id of the request, if it carries one.
func credentialID(r *http.Request) string {
	h, ok := parseHMACHeader(r.Header.Get("Authorization"))
	if !ok {
		return ""
	}

	return h.credential
}

func signedValue(r *http.Request, name string) string {
	if name == signedHost {
		return r.Host
	}

	return r.Header.Get(name)
}

func stringToSign(r *http.Request, signed []string) string {
	vals := make([]string, len(signed))
	for i, n := range signed {
		vals[i] = signedValue(r, n)
	}

	return strings.ToUpper(r.Method) + "\n" + r.RequestURI + "\n" + strings.Join(vals, ";")
}

// SignRequest computes the HMAC-SHA256 signature of the request for the given signed headers and base64 secret.
func SignRequest(r *http.Request, signed []string, secret string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(stringToSign(r, signed)))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

func hasAll(signed []string, want ...string) bool {
	for _, w := range want {
		found := slices.Contains(signed, w)

		if !found {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) checkDate(r *http.Request) error {
	raw := r.Header.Get(hdrDate)
	if raw == "" {
		raw = r.Header.Get(hdrHTTPDate)
	}

	t, err := http.ParseTime(raw)
	if err != nil {
		return ErrUnauthorized
	}

	if skew := b.now().Sub(t); skew > maxClockSkew || skew < -maxClockSkew {
		return ErrUnauthorized
	}

	return nil
}

func checkContentHash(r *http.Request, body []byte) error {
	sum := sha256.Sum256(body)
	want := base64.StdEncoding.EncodeToString(sum[:])

	if subtle.ConstantTimeCompare([]byte(r.Header.Get(hdrContent)), []byte(want)) != 1 {
		return ErrUnauthorized
	}

	return nil
}

func readOnlyMethod(m string) bool { return m == http.MethodGet || m == http.MethodHead }

// verifyHMAC validates an HMAC-SHA256 request against the store's access keys and returns the store name.
func (b *InMemoryBackend) verifyHMAC(r *http.Request, body []byte, hdr hmacHeader) (string, error) {
	owner, key, ok := b.KeyOwner(hdr.credential)
	if !ok || !hasAll(hdr.signed, signedHost, signedHash) ||
		(!hasAll(hdr.signed, signedDate) && !hasAll(hdr.signed, signedHTTP)) {
		return "", ErrUnauthorized
	}

	if err := checkContentHash(r, body); err != nil {
		return "", err
	}

	if err := b.checkDate(r); err != nil {
		return "", err
	}

	want, err := SignRequest(r, hdr.signed, key.Secret)
	if err != nil || subtle.ConstantTimeCompare([]byte(want), []byte(hdr.signature)) != 1 {
		return "", ErrUnauthorized
	}

	if key.ReadOnly && !readOnlyMethod(r.Method) {
		return "", ErrForbidden
	}

	return owner, nil
}

// authorize checks the request's credentials and returns the store the credential belongs to
// ("" for bearer tokens, which are accepted when they are JWT-shaped).
func (b *InMemoryBackend) authorize(r *http.Request, body []byte) (string, error) {
	auth := r.Header.Get("Authorization")

	if hdr, ok := parseHMACHeader(auth); ok {
		return b.verifyHMAC(r, body, hdr)
	}

	if hasPrefixFold(auth, bearerScheme) && strings.Count(auth[len(bearerScheme):], ".") == jwtParts-1 {
		return "", nil
	}

	return "", ErrUnauthorized
}
