package eks

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	jwtAlgRS256                   = "RS256"
	defaultServiceAccountAudience = "sts.amazonaws.com"
	defaultServiceAccountTTL      = time.Hour
	maxServiceAccountTTL          = 365 * 24 * time.Hour
)

// ServiceAccountTokenRequest asks for a projected service-account token.
type ServiceAccountTokenRequest struct {
	Namespace string
	Name      string
	PodName   string
	Audiences []string
	TTL       time.Duration
}

// ServiceAccountToken is a minted projected service-account token.
type ServiceAccountToken struct {
	ExpiresAt time.Time
	Token     string
	Issuer    string
}

// OIDCDiscoveryDocument is the issuer's openid-configuration document.
type OIDCDiscoveryDocument struct {
	Issuer                           string   `json:"issuer"`
	JWKSURI                          string   `json:"jwks_uri"`
	AuthorizationEndpoint            string   `json:"authorization_endpoint"`
	ResponseTypesSupported           []string `json:"response_types_supported"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	ClaimsSupported                  []string `json:"claims_supported"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
}

// JWK is one RSA signing key in a JWKS.
type JWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// JWKS is a JSON Web Key Set.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

func normalizeIssuer(issuer string) string { return strings.TrimRight(issuer, "/") }

// clusterByIssuerLocked returns the cluster whose identity.oidc.issuer is issuer.
func (b *InMemoryBackend) clusterByIssuerLocked(issuer string) (*Cluster, bool) {
	want := normalizeIssuer(issuer)
	if want == "" {
		return nil, false
	}

	for _, c := range b.clusters.All() {
		if c.OIDCIssuer != "" && normalizeIssuer(c.OIDCIssuer) == want {
			return c, true
		}
	}

	return nil, false
}

// signingKey returns the cluster's deterministic OIDC signing key.
func (b *InMemoryBackend) signingKey(issuer string) (*rsa.PrivateKey, error) {
	issuer = normalizeIssuer(issuer)

	if k, ok := b.oidcKeys.Get(issuer); ok {
		return k, nil
	}

	k, err := deriveIRSAKey(issuer)
	if err != nil {
		return nil, err
	}

	b.oidcKeys.Set(issuer, k)

	return k, nil
}

// OIDCIssuerKeys returns the public signing keys for a cluster issuer served by this backend.
func (b *InMemoryBackend) OIDCIssuerKeys(issuer string) ([]*rsa.PublicKey, bool) {
	b.mu.RLock("OIDCIssuerKeys")
	_, ok := b.clusterByIssuerLocked(issuer)
	b.mu.RUnlock()

	if !ok {
		return nil, false
	}

	k, err := b.signingKey(issuer)
	if err != nil {
		return nil, false
	}

	return []*rsa.PublicKey{&k.PublicKey}, true
}

// OIDCIssuerKeys resolves issuer across every region's backend.
func (h *Handler) OIDCIssuerKeys(issuer string) ([]*rsa.PublicKey, bool) {
	for _, b := range h.RegionBackends() {
		if keys, ok := b.OIDCIssuerKeys(issuer); ok {
			return keys, true
		}
	}

	return nil, false
}

// IsOIDCIssuer reports whether issuer has the EKS cluster issuer shape.
func IsOIDCIssuer(issuer string) bool {
	host, path, ok := strings.Cut(strings.TrimPrefix(issuer, "https://"), "/")

	return ok && strings.HasPrefix(host, "oidc.eks.") && strings.HasSuffix(host, ".amazonaws.com") &&
		strings.HasPrefix(path, "id/")
}

func (b *InMemoryBackend) issuerForID(id string) (string, bool) {
	b.mu.RLock("issuerForID")
	defer b.mu.RUnlock()

	for _, c := range b.clusters.All() {
		if strings.HasSuffix(c.OIDCIssuer, "/id/"+id) {
			return c.OIDCIssuer, true
		}
	}

	return "", false
}

// OIDCDiscovery returns the openid-configuration document for the issuer id.
func (b *InMemoryBackend) OIDCDiscovery(id string) (*OIDCDiscoveryDocument, error) {
	issuer, ok := b.issuerForID(id)
	if !ok {
		return nil, errMsgf(ErrNotFound, "No OIDC issuer found for id: %s.", id)
	}

	return &OIDCDiscoveryDocument{
		Issuer:                           issuer,
		JWKSURI:                          issuer + "/keys",
		AuthorizationEndpoint:            "urn:kubernetes:programmatic_authorization",
		ResponseTypesSupported:           []string{"id_token"},
		SubjectTypesSupported:            []string{"public"},
		ClaimsSupported:                  []string{"sub", "iss"},
		IDTokenSigningAlgValuesSupported: []string{jwtAlgRS256},
	}, nil
}

// OIDCJWKS returns the issuer's public key set.
func (b *InMemoryBackend) OIDCJWKS(id string) (*JWKS, error) {
	issuer, ok := b.issuerForID(id)
	if !ok {
		return nil, errMsgf(ErrNotFound, "No OIDC issuer found for id: %s.", id)
	}

	k, err := b.signingKey(issuer)
	if err != nil {
		return nil, err
	}

	return &JWKS{Keys: []JWK{{
		Kty: "RSA",
		Kid: irsaKeyID(&k.PublicKey),
		Use: "sig",
		Alg: jwtAlgRS256,
		N:   base64.RawURLEncoding.EncodeToString(k.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(k.E)).Bytes()),
	}}}, nil
}

func validateTokenRequest(req *ServiceAccountTokenRequest) error {
	if req.Namespace == "" {
		return errMsgf(ErrValidation, "namespace is required")
	}

	if req.Name == "" {
		return errMsgf(ErrValidation, "name is required")
	}

	if req.TTL <= 0 || req.TTL > maxServiceAccountTTL {
		return errMsgf(ErrValidation, "ttlSeconds must be between 1 and %d", int64(maxServiceAccountTTL.Seconds()))
	}

	if slices.Contains(req.Audiences, "") {
		return errMsgf(ErrValidation, "audience must not be empty")
	}

	return nil
}

// MintServiceAccountToken signs a projected service-account token for the cluster.
func (b *InMemoryBackend) MintServiceAccountToken(
	clusterName string, req ServiceAccountTokenRequest,
) (*ServiceAccountToken, error) {
	if len(req.Audiences) == 0 {
		req.Audiences = []string{defaultServiceAccountAudience}
	}

	if req.TTL == 0 {
		req.TTL = defaultServiceAccountTTL
	}

	if err := validateTokenRequest(&req); err != nil {
		return nil, err
	}

	b.mu.RLock("MintServiceAccountToken")
	c, ok := b.clusters.Get(clusterName)
	issuer := ""
	if ok {
		issuer = c.OIDCIssuer
	}
	b.mu.RUnlock()

	if !ok || issuer == "" {
		return nil, errMsgf(ErrNotFound, "No cluster found for name: %s.", clusterName)
	}

	key, err := b.signingKey(issuer)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	exp := now.Add(req.TTL)

	token, err := signServiceAccountJWT(key, serviceAccountClaims(issuer, req, now, exp))
	if err != nil {
		return nil, err
	}

	return &ServiceAccountToken{Token: token, Issuer: issuer, ExpiresAt: exp}, nil
}

func serviceAccountClaims(issuer string, req ServiceAccountTokenRequest, now, exp time.Time) map[string]any {
	ns := uuid.NewSHA1(uuid.NameSpaceURL, []byte(issuer))
	k8s := map[string]any{
		"namespace": req.Namespace,
		"serviceaccount": map[string]any{
			keyName: req.Name,
			"uid":   uuid.NewSHA1(ns, []byte(req.Namespace+"/"+req.Name)).String(),
		},
	}

	if req.PodName != "" {
		k8s["pod"] = map[string]any{
			"name": req.PodName,
			"uid":  uuid.NewSHA1(ns, []byte(req.Namespace+"/pod/"+req.PodName)).String(),
		}
	}

	return map[string]any{
		"iss":           issuer,
		"sub":           fmt.Sprintf("system:serviceaccount:%s:%s", req.Namespace, req.Name),
		"aud":           req.Audiences,
		"exp":           exp.Unix(),
		"iat":           now.Unix(),
		"nbf":           now.Unix(),
		"jti":           uuid.NewString(),
		"kubernetes.io": k8s,
	}
}

func signServiceAccountJWT(key *rsa.PrivateKey, claims map[string]any) (string, error) {
	hdr, err := json.Marshal(map[string]string{"alg": jwtAlgRS256, "kid": irsaKeyID(&key.PublicKey)})
	if err != nil {
		return "", err
	}

	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	signing := base64.RawURLEncoding.EncodeToString(hdr) + "." + base64.RawURLEncoding.EncodeToString(body)
	digest := sha256.Sum256([]byte(signing))

	sig, err := rsa.SignPKCS1v15(nil, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}

	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
