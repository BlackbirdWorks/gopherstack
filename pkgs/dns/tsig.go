package dns

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strings"

	"github.com/miekg/dns"
)

// ErrWeakTSIGAlgorithm is returned for hmac-sha1 and hmac-md5 keys: only the SHA-2 family is signed or verified.
var ErrWeakTSIGAlgorithm = errors.New(
	"dns: TSIG algorithm is not supported (only hmac-sha224, -sha256, -sha384 and -sha512)",
)

// ErrUnknownTSIGAlgorithm is returned for an algorithm name that is not a TSIG HMAC algorithm.
var ErrUnknownTSIGAlgorithm = errors.New("dns: unknown TSIG algorithm")

// TSIGKey is a shared secret that signs DNS messages between two servers (RFC 8945).
type TSIGKey struct {
	// Name is the key name, an absolute domain name once canonicalised.
	Name string
	// Algorithm is the algorithm name, with or without the trailing dot (hmac-sha256).
	Algorithm string
	// Secret is the base64 encoded shared secret.
	Secret string
}

// CanonicalKeyName lower-cases a key name and makes it absolute.
func CanonicalKeyName(name string) string { return dns.CanonicalName(name) }

// CanonicalTSIGAlgorithm returns the absolute lower-case algorithm name miekg/dns expects.
func CanonicalTSIGAlgorithm(alg string) string { return dns.CanonicalName(alg) }

// CheckTSIGAlgorithm reports whether alg is a TSIG algorithm this package can sign and verify.
func CheckTSIGAlgorithm(alg string) error {
	_, err := hashFor(CanonicalTSIGAlgorithm(alg))

	return err
}

func hashFor(canonical string) (func() hash.Hash, error) {
	switch canonical {
	case dns.HmacSHA224:
		return sha256.New224, nil
	case dns.HmacSHA256:
		return sha256.New, nil
	case dns.HmacSHA384:
		return sha512.New384, nil
	case dns.HmacSHA512:
		return sha512.New, nil
	case dns.HmacSHA1, dns.HmacMD5:
		return nil, fmt.Errorf("%w: %s", ErrWeakTSIGAlgorithm, strings.TrimSuffix(canonical, "."))
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownTSIGAlgorithm, canonical)
	}
}

// KeyLookup finds a key by its canonical name.
type KeyLookup func(canonicalName string) (TSIGKey, bool)

type provider struct{ lookup KeyLookup }

// NewTSIGProvider returns a dns.TsigProvider that resolves keys through lookup on every message, so keys created
// or deleted after the server started take effect at once.
func NewTSIGProvider(lookup KeyLookup) dns.TsigProvider { return provider{lookup: lookup} }

// SingleKeyProvider returns a dns.TsigProvider that knows only key.
func SingleKeyProvider(key TSIGKey) dns.TsigProvider {
	canonical := CanonicalKeyName(key.Name)

	return provider{lookup: func(name string) (TSIGKey, bool) { return key, name == canonical }}
}

func (p provider) mac(msg []byte, t *dns.TSIG) ([]byte, error) {
	key, ok := p.lookup(CanonicalKeyName(t.Hdr.Name))
	if !ok {
		return nil, dns.ErrSecret
	}

	alg := CanonicalTSIGAlgorithm(t.Algorithm)
	if alg != CanonicalTSIGAlgorithm(key.Algorithm) {
		return nil, dns.ErrKeyAlg
	}

	newHash, err := hashFor(alg)
	if err != nil {
		return nil, err
	}

	secret, err := base64.StdEncoding.DecodeString(key.Secret)
	if err != nil {
		return nil, fmt.Errorf("dns: TSIG secret is not base64: %w", err)
	}

	h := hmac.New(newHash, secret)
	h.Write(msg)

	return h.Sum(nil), nil
}

// Generate implements dns.TsigProvider.
func (p provider) Generate(msg []byte, t *dns.TSIG) ([]byte, error) { return p.mac(msg, t) }

// Verify implements dns.TsigProvider.
func (p provider) Verify(msg []byte, t *dns.TSIG) error {
	want, err := p.mac(msg, t)
	if err != nil {
		return err
	}

	got, err := hex.DecodeString(t.MAC)
	if err != nil {
		return err
	}

	if !hmac.Equal(want, got) {
		return dns.ErrSig
	}

	return nil
}
