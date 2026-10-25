// Package ociauth parses and verifies OCI API request signatures (the
// draft-cavage HTTP Signatures scheme used by every OCI SDK).
//
// Like the other cloud auth packages it is permissive by default: a request is
// only cryptographically checked when its keyId names an API key registered in
// the [Registry]. Unknown keys and unsigned requests pass unless the caller
// asks for enforcement.
package ociauth

import (
	"crypto"
	"crypto/md5" //nolint:gosec // OCI API key fingerprints are MD5 by definition
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// MaxClockSkew is the largest date drift OCI accepts on a signed request.
const MaxClockSkew = 5 * time.Minute

const (
	headerRequestTarget = "(request-target)"
	headerContentSHA256 = "x-content-sha256"
	canonContentSHA256  = "X-Content-Sha256"
	canonXDate          = "X-Date"
	keyIDParts          = 3
)

var (
	// ErrNotAuthenticated is returned for missing, malformed or invalid credentials.
	ErrNotAuthenticated = errors.New("ociauth: not authenticated")
	// ErrBadKey is returned when a public key cannot be parsed.
	ErrBadKey = errors.New("ociauth: invalid RSA public key")
)

// KeyID is the decomposed "<tenancy>/<user>/<fingerprint>" signature keyId.
type KeyID struct {
	Tenancy     string
	User        string
	Fingerprint string
}

// String renders the keyId form used on the wire.
func (k KeyID) String() string { return k.Tenancy + "/" + k.User + "/" + k.Fingerprint }

// ParseKeyID splits a keyId; instance/resource principals use other shapes and report false.
func ParseKeyID(s string) (KeyID, bool) {
	parts := strings.Split(s, "/")
	if len(parts) != keyIDParts || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return KeyID{}, false
	}

	return KeyID{Tenancy: parts[0], User: parts[1], Fingerprint: parts[2]}, true
}

// Signature is a parsed Authorization header.
type Signature struct {
	KeyID     string
	Algorithm string
	Version   string
	Headers   []string
	Signature []byte
}

// ParseAuthorization parses `Signature version="1",headers="...",keyId="...",algorithm="rsa-sha256",signature="..."`.
func ParseAuthorization(h string) (Signature, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(h), "Signature ")
	if !ok {
		return Signature{}, fmt.Errorf("%w: not a Signature scheme", ErrNotAuthenticated)
	}

	fields := map[string]string{}

	for _, kv := range splitParams(rest) {
		k, v, found := strings.Cut(kv, "=")
		if !found {
			return Signature{}, fmt.Errorf("%w: malformed parameter", ErrNotAuthenticated)
		}

		fields[strings.ToLower(strings.TrimSpace(k))] = strings.Trim(strings.TrimSpace(v), `"`)
	}

	sig, err := base64.StdEncoding.DecodeString(fields["signature"])
	if err != nil || len(sig) == 0 || fields["keyid"] == "" {
		return Signature{}, fmt.Errorf("%w: missing keyId or signature", ErrNotAuthenticated)
	}

	hdrs := strings.Fields(strings.ToLower(fields["headers"]))
	if len(hdrs) == 0 {
		hdrs = []string{"date"}
	}

	return Signature{
		KeyID: fields["keyid"], Algorithm: fields["algorithm"], Version: fields["version"],
		Headers: hdrs, Signature: sig,
	}, nil
}

func splitParams(s string) []string {
	var (
		out     []string
		cur     strings.Builder
		inQuote bool
	)

	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote

			cur.WriteRune(r)
		case r == ',' && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}

	if cur.Len() > 0 {
		out = append(out, cur.String())
	}

	return out
}

// Fingerprint returns the colon-separated MD5 of the DER-encoded public key,
// the identifier OCI shows for an uploaded API key.
func Fingerprint(pub *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrBadKey, err)
	}

	sum := md5.Sum(der) //nolint:gosec // see import

	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02x", b)
	}

	return strings.Join(parts, ":"), nil
}

// KeyRecord is the persisted form of a registered API key.
type KeyRecord struct {
	Tenancy   string `json:"tenancy"`
	User      string `json:"user"`
	PublicPEM string `json:"publicPem"`
}

type regKey struct {
	pub *rsa.PublicKey
	rec KeyRecord
}

// Registry holds the API keys the emulator verifies signatures against.
type Registry struct {
	mu    *lockmetrics.RWMutex
	keys  map[string]regKey
	links []*Registry
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{mu: lockmetrics.New("ociauth.registry"), keys: map[string]regKey{}}
}

// ParsePublicKeyPEM accepts a PKIX "PUBLIC KEY" or PKCS#1 "RSA PUBLIC KEY" block.
func ParsePublicKeyPEM(data string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(data))
	if block == nil {
		return nil, fmt.Errorf("%w: no PEM block", ErrBadKey)
	}

	if pub, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		if rk, isRSA := pub.(*rsa.PublicKey); isRSA {
			return rk, nil
		}

		return nil, fmt.Errorf("%w: not an RSA key", ErrBadKey)
	}

	pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadKey, err)
	}

	return pub, nil
}

// Register adds an API key and returns its fingerprint.
func (r *Registry) Register(tenancy, user, publicPEM string) (string, error) {
	pub, err := ParsePublicKeyPEM(publicPEM)
	if err != nil {
		return "", err
	}

	fp, err := Fingerprint(pub)
	if err != nil {
		return "", err
	}

	id := KeyID{Tenancy: tenancy, User: user, Fingerprint: fp}.String()

	r.mu.Lock("Register")
	r.keys[id] = regKey{pub: pub, rec: KeyRecord{Tenancy: tenancy, User: user, PublicPEM: publicPEM}}
	r.mu.Unlock()

	return fp, nil
}

// Remove deletes the key registered under keyID and reports whether it existed.
func (r *Registry) Remove(keyID string) bool {
	r.mu.Lock("Remove")
	defer r.mu.Unlock()

	_, ok := r.keys[keyID]
	delete(r.keys, keyID)

	return ok
}

// Records lists registered keys in a stable order.
func (r *Registry) Records() []KeyRecord {
	r.mu.RLock("Records")
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.keys))
	for id := range r.keys {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	out := make([]KeyRecord, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.keys[id].rec)
	}

	return out
}

// Reset drops every key.
func (r *Registry) Reset() {
	r.mu.Lock("Reset")
	r.keys = map[string]regKey{}
	r.mu.Unlock()
}

// Load replaces the registry contents with recs, skipping unparsable keys.
func (r *Registry) Load(recs []KeyRecord) {
	r.Reset()

	for _, rec := range recs {
		_, _ = r.Register(rec.Tenancy, rec.User, rec.PublicPEM)
	}
}

// Link makes keys registered in other verifiable through r as well, so a
// registry owned by OCI Identity can serve every OCI service. Links must not form a cycle.
func (r *Registry) Link(other *Registry) {
	if other == nil || other == r {
		return
	}

	r.mu.Lock("Link")
	defer r.mu.Unlock()

	if !slices.Contains(r.links, other) {
		r.links = append(r.links, other)
	}
}

func (r *Registry) lookup(keyID string) (*rsa.PublicKey, bool) {
	if pub, ok := r.lookupLocal(keyID); ok {
		return pub, true
	}

	r.mu.RLock("lookup")
	links := slices.Clone(r.links)
	r.mu.RUnlock()

	for _, l := range links {
		if pub, ok := l.lookup(keyID); ok {
			return pub, true
		}
	}

	return nil, false
}

func (r *Registry) lookupLocal(keyID string) (*rsa.PublicKey, bool) {
	r.mu.RLock("lookupLocal")
	defer r.mu.RUnlock()

	k, ok := r.keys[keyID]

	return k.pub, ok
}

// Principal identifies the caller of a request.
type Principal struct {
	KeyID     KeyID
	Verified  bool
	Anonymous bool
}

// Options tunes [Verify].
type Options struct {
	Registry *Registry
	Now      func() time.Time
	// Enforce rejects unsigned requests and signatures from keys not in the Registry.
	Enforce bool
}

// Verify authenticates r. body must be the full request body (used to check a signed x-content-sha256).
func Verify(r *http.Request, body []byte, opt Options) (Principal, error) {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		if opt.Enforce {
			return Principal{}, fmt.Errorf("%w: missing Authorization header", ErrNotAuthenticated)
		}

		return Principal{Anonymous: true}, nil
	}

	sig, err := ParseAuthorization(authz)
	if err != nil {
		if opt.Enforce {
			return Principal{}, err
		}

		return Principal{Anonymous: true}, nil
	}

	kid, _ := ParseKeyID(sig.KeyID)

	var pub *rsa.PublicKey

	known := false
	if opt.Registry != nil {
		pub, known = opt.Registry.lookup(sig.KeyID)
	}

	if !known {
		if opt.Enforce {
			return Principal{}, fmt.Errorf("%w: unknown API key", ErrNotAuthenticated)
		}

		return Principal{KeyID: kid}, nil
	}

	if err = verifyKnown(r, body, sig, pub, opt); err != nil {
		return Principal{}, err
	}

	return Principal{KeyID: kid, Verified: true}, nil
}

func verifyKnown(r *http.Request, body []byte, sig Signature, pub *rsa.PublicKey, opt Options) error {
	if !strings.EqualFold(sig.Algorithm, "rsa-sha256") && sig.Algorithm != "" {
		return fmt.Errorf("%w: unsupported algorithm %q", ErrNotAuthenticated, sig.Algorithm)
	}

	signed := map[string]bool{}
	for _, h := range sig.Headers {
		signed[h] = true
	}

	if !signed[headerRequestTarget] || !signed["host"] || (!signed["date"] && !signed["x-date"]) {
		return fmt.Errorf("%w: (request-target), host and date must be signed", ErrNotAuthenticated)
	}

	if opt.Now != nil {
		if err := checkSkew(r, opt.Now()); err != nil {
			return err
		}
	}

	if signed[headerContentSHA256] {
		sum := sha256.Sum256(body)
		want := base64.StdEncoding.EncodeToString(sum[:])

		if subtle.ConstantTimeCompare([]byte(want), []byte(r.Header.Get(canonContentSHA256))) != 1 {
			return fmt.Errorf("%w: x-content-sha256 does not match body", ErrNotAuthenticated)
		}
	}

	digest := sha256.Sum256([]byte(signingString(r, sig.Headers)))

	if rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig.Signature) == nil {
		return nil
	}

	if rsa.VerifyPSS(pub, crypto.SHA256, digest[:], sig.Signature, nil) == nil {
		return nil
	}

	return fmt.Errorf("%w: signature mismatch", ErrNotAuthenticated)
}

func checkSkew(r *http.Request, now time.Time) error {
	raw := r.Header.Get(canonXDate)
	if raw == "" {
		raw = r.Header.Get("Date")
	}

	t, err := http.ParseTime(raw)
	if err != nil {
		return fmt.Errorf("%w: unparsable date", ErrNotAuthenticated)
	}

	if d := now.Sub(t); d > MaxClockSkew || d < -MaxClockSkew {
		return fmt.Errorf("%w: clock skew exceeds %s", ErrNotAuthenticated, MaxClockSkew)
	}

	return nil
}

func signingString(r *http.Request, headers []string) string {
	lines := make([]string, len(headers))

	for i, h := range headers {
		var v string

		switch h {
		case headerRequestTarget:
			uri := r.RequestURI
			if uri == "" {
				uri = r.URL.RequestURI()
			}

			v = strings.ToLower(r.Method) + " " + uri
		case "host":
			v = r.Host
		default:
			v = r.Header.Get(h)
		}

		lines[i] = h + ": " + v
	}

	return strings.Join(lines, "\n")
}
