package gcpiam

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	jwtDefaultTTL = time.Hour
	jwtMaxTTL     = 12 * time.Hour
)

// ensureSystemKey returns the account's system-managed signing key, creating it on first use.
func (b *InMemoryBackend) ensureSystemKey(name string) (*keyRec, error) {
	b.mu.RLock("ensureSystemKey")
	rec, err := b.findAccountLocked(name)

	var sys *keyRec

	var acct *adminpb.ServiceAccount

	if err == nil {
		sys = rec.system
		acct = cloneAccount(rec.pb)
	}
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	if sys != nil {
		return sys, nil
	}

	now := b.now()

	mat, err := generateMaterial(acct.GetEmail(), now, farFuture())
	if err != nil {
		return nil, err
	}

	id, err := newKeyID()
	if err != nil {
		return nil, err
	}

	der, err := x509.MarshalPKCS8PrivateKey(mat.priv)
	if err != nil {
		return nil, errInternal(err)
	}

	fresh := &keyRec{
		pb: &adminpb.ServiceAccountKey{
			Name:            keyName(acct, id),
			KeyAlgorithm:    adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048,
			KeyOrigin:       adminpb.ServiceAccountKeyOrigin_GOOGLE_PROVIDED,
			KeyType:         adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED,
			ValidAfterTime:  timestamppb.New(now),
			ValidBeforeTime: timestamppb.New(farFuture()),
		},
		cert:    mat.certPEM,
		public:  mat.pubDER,
		private: der,
	}

	b.mu.Lock("ensureSystemKey")
	defer b.mu.Unlock()

	cur, ok := b.accounts[acct.GetUniqueId()]
	if !ok || !cur.active() {
		return nil, errNotFound("Service account %s does not exist.", name)
	}

	if cur.system == nil {
		cur.system = fresh
	}

	return cur.system, nil
}

// signer returns the system-managed private key of an enabled account and its key id.
func (b *InMemoryBackend) signer(name string) (*rsa.PrivateKey, string, error) {
	b.mu.RLock("signer")
	rec, err := b.findAccountLocked(name)
	disabled := err == nil && rec.pb.GetDisabled()
	b.mu.RUnlock()

	if err != nil {
		return nil, "", err
	}

	if disabled {
		return nil, "", errPrecondition("Service account %s is disabled.", name)
	}

	sys, err := b.ensureSystemKey(name)
	if err != nil {
		return nil, "", err
	}

	parsed, err := x509.ParsePKCS8PrivateKey(sys.private)
	if err != nil {
		return nil, "", errInternal(err)
	}

	priv, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, "", errInternal(nil)
	}

	return priv, sys.pb.GetName()[strings.LastIndex(sys.pb.GetName(), "/")+1:], nil
}

func signSHA256(priv *rsa.PrivateKey, data []byte) ([]byte, error) {
	sum := sha256.Sum256(data)

	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, sum[:])
	if err != nil {
		return nil, errInternal(err)
	}

	return sig, nil
}

// SignBlob signs bytes with RSASSA-PKCS1-v1_5 over SHA-256 using the account's system-managed key.
func (b *InMemoryBackend) SignBlob(name string, data []byte) (string, []byte, error) {
	priv, keyID, err := b.signer(name)
	if err != nil {
		return "", nil, err
	}

	sig, err := signSHA256(priv, data)
	if err != nil {
		return "", nil, err
	}

	return keyID, sig, nil
}

// SignJwt signs a JWT claim set as RS256; a missing exp claim defaults to one hour from now.
func (b *InMemoryBackend) SignJwt(name, payload string) (string, string, error) {
	claims, err := b.jwtClaims(payload)
	if err != nil {
		return "", "", err
	}

	priv, keyID, err := b.signer(name)
	if err != nil {
		return "", "", err
	}

	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": keyID})
	if err != nil {
		return "", "", errInternal(err)
	}

	enc := base64.RawURLEncoding
	signing := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)

	sig, err := signSHA256(priv, []byte(signing))
	if err != nil {
		return "", "", err
	}

	return keyID, signing + "." + enc.EncodeToString(sig), nil
}

func (b *InMemoryBackend) jwtClaims(payload string) ([]byte, error) {
	var claims map[string]any

	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()

	if err := dec.Decode(&claims); err != nil || claims == nil {
		return nil, errInvalid("The JWT payload must be a serialized JSON object.")
	}

	now := b.now()

	if raw, ok := claims["exp"]; ok {
		num, isNum := raw.(json.Number)

		exp, err := num.Int64()
		if !isNum || err != nil {
			return nil, errInvalid("The exp claim must be an integer timestamp.")
		}

		if exp < now.Unix() || exp > now.Add(jwtMaxTTL).Unix() {
			return nil, errInvalid("The exp claim must not be in the past and no more than 12 hours in the future.")
		}
	} else {
		claims["exp"] = now.Add(jwtDefaultTTL).Unix()
	}

	out, err := json.Marshal(claims)
	if err != nil {
		return nil, errInternal(err)
	}

	return out, nil
}
