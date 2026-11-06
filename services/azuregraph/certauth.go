package azuregraph

import (
	"crypto"
	"crypto/sha1" //nolint:gosec // x5t is the SHA-1 certificate thumbprint defined by RFC 7515 and used by Entra
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	typeCert         = "AsymmetricX509Cert"
	typeCertPFX      = "X509CertAndPassword"
	usageVerify      = "Verify"
	usageSign        = "Sign"
	proofAudience    = "00000002-0000-0000-c000-000000000000"
	proofMaxLifetime = 10 * time.Minute
	clockSkew        = 5 * time.Minute

	aadsInvalidAssertion = 50027
	aadsNoFederated      = 70021
	aadsAssertionAud     = 700023
	aadsAssertionTime    = 700024
	aadsAssertionSig     = 700027
)

// KeyResolver returns the public key of an emulator-run OIDC issuer for a JWT key ID.
type KeyResolver func(kid string) (crypto.PublicKey, bool)

// SetIssuerKeys lets federated identity credential assertions signed by an emulator-run OIDC issuer be verified
// in-process. Assertions whose kid the resolver does not know are matched by claims only.
func (b *Backend) SetIssuerKeys(r KeyResolver) {
	b.mu.Lock("SetIssuerKeys")
	defer b.mu.Unlock()

	b.issuerKeys = r
}

func signingMethods() []string {
	return []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"}
}

type certEntry struct {
	cert *x509.Certificate
	cred KeyCredential
}

func (b *Backend) certEntries(keys []KeyCredential) []certEntry {
	now := b.now()

	var out []certEntry

	for _, k := range keys {
		if now.Before(k.StartDateTime) || !now.Before(k.EndDateTime) {
			continue
		}

		if cert, err := parseCert(b.keyMaterial[k.KeyID]); err == nil {
			out = append(out, certEntry{cred: k, cert: cert})
		}
	}

	return out
}

func parseCert(b64 string) (*x509.Certificate, error) {
	der, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("azuregraph: certificate is not base64: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("azuregraph: certificate is not DER X.509: %w", err)
	}

	return cert, nil
}

func (b *Backend) keyErr(prop string) *Error {
	return badRequest("Invalid value specified for property '%s' of resource 'KeyCredential'.", prop)
}

// AddKey registers a key credential and retains the certificate. When the holder already has a valid certificate
// the proof JWT must be signed by one of them. Graph documents addKey as unavailable until a first certificate
// exists; the emulator accepts that first key without proof so the call can bootstrap.
func (b *Backend) AddKey(id string, in KeyCredential, proof string) (KeyCredential, error) {
	if in.Key == nil {
		return KeyCredential{}, badRequest("A value is required for property 'key' of resource 'KeyCredential'.")
	}

	if _, err := base64.StdEncoding.DecodeString(*in.Key); err != nil {
		return KeyCredential{}, b.keyErr("key")
	}

	if in.Type != typeCert && in.Type != typeCertPFX {
		return KeyCredential{}, b.keyErr("type")
	}

	if in.Usage != usageVerify && in.Usage != usageSign {
		return KeyCredential{}, b.keyErr("usage")
	}

	var cert *x509.Certificate

	if in.Type == typeCert {
		c, err := parseCert(*in.Key)
		if err != nil {
			return KeyCredential{}, b.keyErr("key")
		}

		cert = c
		in.StartDateTime, in.EndDateTime = c.NotBefore, c.NotAfter
	}

	b.mu.Lock("AddKey")
	defer b.mu.Unlock()

	start, end, err := b.credWindow(in.StartDateTime, in.EndDateTime)
	if err != nil {
		return KeyCredential{}, err
	}

	_, keys, err := b.credHolder(id)
	if err != nil {
		return KeyCredential{}, err
	}

	if existing := b.certEntries(*keys); len(existing) > 0 {
		if err = b.verifyProof(id, proof, existing); err != nil {
			return KeyCredential{}, err
		}
	}

	cred := KeyCredential{
		KeyID: newID(), Type: in.Type, Usage: in.Usage, DisplayName: in.DisplayName,
		CustomKeyIdentifier: in.CustomKeyIdentifier, StartDateTime: start, EndDateTime: end,
	}

	if cert != nil {
		b.keyMaterial[cred.KeyID] = *in.Key

		if cred.CustomKeyIdentifier == nil {
			sum := sha1.Sum(cert.Raw) //nolint:gosec // certificate thumbprint, not a security hash
			thumb := base64.StdEncoding.EncodeToString(sum[:])
			cred.CustomKeyIdentifier = &thumb
		}
	}

	*keys = append(slices.Clone(*keys), cred)

	return cred, nil
}

func proofErr(reason string) *Error {
	return badRequest("Key credential proof validation failed: %s", reason)
}

func (b *Backend) holderAppID(id string) string {
	if a, ok := b.apps.get(lc(id)); ok {
		return a.AppID
	}

	if s, ok := b.sps.get(lc(id)); ok {
		return s.AppID
	}

	return ""
}

func (b *Backend) verifyProof(holderID, proof string, existing []certEntry) error {
	if proof == "" {
		return proofErr("a proof of possession token signed with an existing certificate is required.")
	}

	claims, ok := verifyWithCerts(proof, existing, nil)
	if !ok {
		return proofErr("the proof is not signed by a valid existing certificate of this object.")
	}

	if aud, _ := claims.GetAudience(); !slices.Contains(aud, proofAudience) {
		return proofErr(fmt.Sprintf("the audience must be %s.", proofAudience))
	}

	iss, _ := claims.GetIssuer()
	if !strings.EqualFold(iss, b.holderAppID(holderID)) && !strings.EqualFold(iss, holderID) {
		return proofErr("the issuer must be the id of the application making the call.")
	}

	exp, nbf, ok := b.timeWindow(claims)
	if !ok || exp.Sub(nbf) > proofMaxLifetime {
		return proofErr("exp and nbf are required, expiring at most 10 minutes after nbf, and must be current.")
	}

	return nil
}

// timeWindow reads exp and nbf and reports whether now is inside them (with Entra's clock skew).
func (b *Backend) timeWindow(claims jwt.MapClaims) (time.Time, time.Time, bool) {
	exp, errE := claims.GetExpirationTime()
	nbf, errN := claims.GetNotBefore()

	if errE != nil || exp == nil {
		return time.Time{}, time.Time{}, false
	}

	start := exp.Add(-proofMaxLifetime)
	if errN == nil && nbf != nil {
		start = nbf.Time
	}

	now := b.now()

	return exp.Time, start, !now.After(exp.Add(clockSkew)) && !now.Before(start.Add(-clockSkew))
}

// verifyWithCerts verifies token against each candidate certificate (those whose thumbprint matches the header, when
// it carries one) and returns the claims and the matching entry.
func verifyWithCerts(token string, certs []certEntry, header map[string]any) (jwt.MapClaims, bool) {
	x5t, _ := header["x5t"].(string)
	x5t256, _ := header["x5t#S256"].(string)
	parser := jwt.NewParser(jwt.WithValidMethods(signingMethods()), jwt.WithoutClaimsValidation())

	for _, c := range certs {
		sha1Sum := sha1.Sum(c.cert.Raw) //nolint:gosec // thumbprint match only
		sha256Sum := sha256.Sum256(c.cert.Raw)

		if !thumbprintMatches(x5t, sha1Sum[:]) || !thumbprintMatches(x5t256, sha256Sum[:]) {
			continue
		}

		claims := jwt.MapClaims{}

		if _, err := parser.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
			return c.cert.PublicKey, nil
		}); err == nil {
			return claims, true
		}
	}

	return nil, false
}

// thumbprintMatches compares a header thumbprint (empty matches anything) with a digest. RFC 7515 uses unpadded
// base64url; msal-go sends x5t#S256 as padded standard base64, so every encoding is accepted.
func thumbprintMatches(got string, sum []byte) bool {
	if got == "" {
		return true
	}

	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding, base64.URLEncoding, base64.StdEncoding, base64.RawStdEncoding,
	} {
		if got == enc.EncodeToString(sum) {
			return true
		}
	}

	return false
}

func assertionErr(status int, name string, code int, format string, a ...any) *ClientAuthError {
	return &ClientAuthError{
		Status: status, Name: name, Code: code,
		Description: fmt.Sprintf("AADSTS%d: %s", code, fmt.Sprintf(format, a...)),
	}
}

// AuthenticateAssertion verifies a JWT client assertion for clientID. audiences are the aud values accepted for a
// certificate assertion (the token endpoint and issuer URLs). known is false for a client the directory does not
// know. An assertion whose iss and sub are the client ID is a certificate assertion, checked against the stored
// keyCredentials; any other is matched against the application's federated identity credentials.
func (b *Backend) AuthenticateAssertion(clientID, assertion string, audiences []string) (string, bool, error) {
	b.mu.RLock("AuthenticateAssertion")
	defer b.mu.RUnlock()

	sp, app, cerr := b.clientObjects(clientID)
	if cerr != nil {
		return "", true, cerr
	}

	if sp == nil {
		return "", false, nil
	}

	claims := jwt.MapClaims{}

	tok, _, err := jwt.NewParser().ParseUnverified(assertion, claims)
	if err != nil {
		return "", true, assertionErr(http.StatusBadRequest, "invalid_request", aadsInvalidAssertion,
			"JWT token is invalid or malformed.")
	}

	if _, _, ok := b.timeWindow(claims); !ok {
		return "", true, assertionErr(http.StatusUnauthorized, "invalid_client", aadsAssertionTime,
			"Client assertion is not within its valid time range. Current time: %s, assertion valid from %v, "+
				"expiry time of assertion %v.", b.now().Format(time.RFC3339), claims["nbf"], claims["exp"])
	}

	iss, _ := claims.GetIssuer()
	sub, _ := claims.GetSubject()

	if strings.EqualFold(iss, clientID) && strings.EqualFold(sub, clientID) {
		return b.checkCertAssertion(sp, app, assertion, tok.Header, claims, audiences)
	}

	return b.checkFederatedAssertion(sp, app, assertion, tok.Header, claims)
}

func (b *Backend) checkCertAssertion(
	sp *ServicePrincipal, app *Application, assertion string, header map[string]any, claims jwt.MapClaims,
	audiences []string,
) (string, bool, error) {
	aud, _ := claims.GetAudience()
	if !slices.ContainsFunc(
		aud,
		func(a string) bool { return slices.Contains(audiences, strings.TrimSuffix(a, "/")) },
	) {
		return "", true, assertionErr(http.StatusUnauthorized, "invalid_client", aadsAssertionAud,
			"Client assertion audience claim does not match Realm issuer.")
	}

	keys := slices.Clone(sp.KeyCredentials)
	if app != nil {
		keys = append(keys, app.KeyCredentials...)
	}

	keys = slices.DeleteFunc(keys, func(k KeyCredential) bool { return k.Usage != usageVerify })

	if _, ok := verifyWithCerts(assertion, b.certEntries(keys), header); !ok {
		return "", true, assertionErr(http.StatusUnauthorized, "invalid_client", aadsAssertionSig,
			"Client assertion contains an invalid signature. [Reason - The key was not found.]")
	}

	return sp.ID, true, nil
}

func (b *Backend) checkFederatedAssertion(
	sp *ServicePrincipal, app *Application, assertion string, header map[string]any, claims jwt.MapClaims,
) (string, bool, error) {
	iss, _ := claims.GetIssuer()
	sub, _ := claims.GetSubject()
	aud, _ := claims.GetAudience()

	var creds []FederatedCredential
	if app != nil {
		creds = b.fics[lc(app.ID)]
	}

	if !slices.ContainsFunc(creds, func(f FederatedCredential) bool {
		return f.Issuer == iss && f.Subject == sub && slices.ContainsFunc(f.Audiences, func(a string) bool {
			return slices.Contains(aud, a)
		})
	}) {
		return "", true, assertionErr(http.StatusBadRequest, "invalid_request", aadsNoFederated,
			"No matching federated identity record found for presented assertion. Assertion Issuer: '%s'. "+
				"Assertion Subject: '%s'. Assertion Audience: '%s'.", iss, sub, strings.Join(aud, ","))
	}

	if kid, _ := header["kid"].(string); kid != "" && b.issuerKeys != nil {
		if key, ok := b.issuerKeys(kid); ok {
			parser := jwt.NewParser(jwt.WithValidMethods(signingMethods()), jwt.WithoutClaimsValidation())
			if _, err := parser.Parse(assertion, func(*jwt.Token) (any, error) { return key, nil }); err != nil {
				return "", true, assertionErr(http.StatusUnauthorized, "invalid_client", aadsAssertionSig,
					"Client assertion contains an invalid signature. [Reason - The key was not found.]")
			}
		}
	}

	return sp.ID, true, nil
}
