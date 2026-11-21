package azurearm

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // x5t is the SHA-1 certificate thumbprint defined by RFC 7515 and used by Entra
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const (
	logicAssertionTTL   = 10 * time.Minute
	logicCodeBadRequest = "BadRequest"
)

var errLogicClientCert = errors.New("client certificate failed")

type logicPFX struct {
	leaf *x509.Certificate
	key  crypto.PrivateKey
	tls  tls.Certificate
}

// loadLogicPFX decodes the base64 PFX and password of a ClientCertificate or ActiveDirectoryOAuth block. The error
// never carries the password or the key material; "Could not load private key" is the service's BadRequest text
// (Learn: set-up-security-permissions, client certificate note).
func loadLogicPFX(auth map[string]any) (*logicPFX, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(stringOf(auth["pfx"])), ""))
	if err != nil || len(raw) == 0 {
		return nil, logicClientCertErr("the pfx is not base64 encoded PFX content")
	}

	key, leaf, chain, err := pkcs12.DecodeChain(raw, stringOf(auth["password"]))
	if err != nil {
		return nil, logicClientCertErr("the pfx content or its password is not valid")
	}

	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, logicClientCertErr("the pfx private key cannot sign")
	}

	der := [][]byte{leaf.Raw}
	for _, c := range chain {
		der = append(der, c.Raw)
	}

	return &logicPFX{leaf: leaf, key: key, tls: tls.Certificate{Certificate: der, PrivateKey: signer, Leaf: leaf}}, nil
}

func logicClientCertErr(msg string) error {
	return &logicClientCertError{msg: "Could not load private key: " + msg}
}

type logicClientCertError struct{ msg string }

func (e *logicClientCertError) Error() string { return e.msg }
func (e *logicClientCertError) Unwrap() error { return errLogicClientCert }

// clientAssertion signs the RS256 client-assertion JWT of the certificate credential type: iss and sub are the
// client id, aud the token endpoint, x5t and x5t#S256 the certificate thumbprints.
func (c *logicPFX) clientAssertion(clientID, audience string, now time.Time) (string, error) {
	priv, ok := c.key.(*rsa.PrivateKey)
	if !ok {
		return "", logicClientCertErr("only RSA keys can sign a client assertion")
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"aud": audience, "iss": clientID, "sub": clientID, "jti": uuid.NewString(),
		"nbf": now.Unix(), "iat": now.Unix(), "exp": now.Add(logicAssertionTTL).Unix(),
	})

	s1 := sha1.Sum(c.leaf.Raw) //nolint:gosec // thumbprint only
	s256 := sha256.Sum256(c.leaf.Raw)
	tok.Header["x5t"] = base64.RawURLEncoding.EncodeToString(s1[:])
	tok.Header["x5t#S256"] = base64.RawURLEncoding.EncodeToString(s256[:])

	return tok.SignedString(priv)
}

func (p *LogicProvider) tlsConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: p.roots.Load()}
}

// certClient is a one-request client presenting cert, on a transport with the same loopback-gated dialer.
func (p *LogicProvider) certClient(cert *tls.Certificate) *http.Client {
	tr := p.newTransport()
	tr.TLSClientConfig.Certificates = []tls.Certificate{*cert}

	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// SetOutboundRootCAs sets the roots outbound HTTPS calls verify against (nil: the system roots). Call it before
// the provider is in use.
func (p *LogicProvider) SetOutboundRootCAs(pool *x509.CertPool) {
	p.roots.Store(pool)

	p.mu.Lock("SetOutboundRootCAs")
	p.http = p.newHTTPClient()
	p.mu.Unlock()
}

func sameOrigin(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)

	return errA == nil && errB == nil && ua.Scheme == ub.Scheme && ua.Host == ub.Host
}
