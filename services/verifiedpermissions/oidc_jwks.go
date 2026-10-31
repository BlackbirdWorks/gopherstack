package verifiedpermissions

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

var (
	errKeyNotFound     = errors.New("signing key not found in JWKS")
	errBadIssuerURL    = errors.New("issuer must be an https URL")
	errBadDiscoveryDoc = errors.New("bad discovery document")
)

type jwksEntry struct {
	fetched time.Time
	keys    map[string]*rsa.PublicKey
}

// cognitoPoolID extracts the user pool ID from a user pool ARN.
func cognitoPoolID(userPoolArn string) string {
	parsed, err := awsarn.Parse(userPoolArn)
	if err != nil {
		return ""
	}

	return strings.TrimPrefix(parsed.Resource, "userpool/")
}

// oidcKey returns the issuer's signing key for kid, fetching discovery + JWKS
// when the cache is cold, older than jwksCacheTTL, or lacks kid (rate limited).
func (v *TokenVerifier) oidcKey(issuer, kid string) (*rsa.PublicKey, error) {
	now := v.now()

	if e, ok := v.cache.Get(issuer); ok {
		age := now.Sub(e.fetched)
		if key, hit := e.keys[kid]; hit && age < jwksCacheTTL {
			return key, nil
		}

		if age < jwksMinRefresh {
			return nil, fmt.Errorf("%w: %q", errKeyNotFound, kid)
		}
	}

	keys, err := v.fetchJWKS(issuer)
	if err != nil {
		return nil, err
	}

	v.cache.Set(issuer, jwksEntry{fetched: now, keys: keys})

	key, ok := keys[kid]
	if !ok {
		return nil, fmt.Errorf("%w: %q", errKeyNotFound, kid)
	}

	return key, nil
}

func (v *TokenVerifier) getJSON(rawURL string, out any) error {
	u, err := url.Parse(rawURL)
	if err != nil || !allowedIssuerURL(u) {
		return fmt.Errorf("%w: %q", errBadIssuerURL, rawURL)
	}

	ctx, cancel := context.WithTimeout(context.Background(), oidcHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := v.client.Load().Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: fetch %s: status %d", errBadDiscoveryDoc, rawURL, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOIDCBody))
	if err != nil {
		return fmt.Errorf("read %s: %w", rawURL, err)
	}

	if err = json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s: %w", rawURL, err)
	}

	return nil
}

func isLoopbackHost(h string) bool {
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

func (v *TokenVerifier) fetchJWKS(issuer string) (map[string]*rsa.PublicKey, error) {
	var disc struct {
		JWKSURI string `json:"jwks_uri"`
	}

	if err := v.getJSON(strings.TrimSuffix(issuer, "/")+oidcDiscoveryTL, &disc); err != nil {
		return nil, err
	}

	if disc.JWKSURI == "" {
		return nil, fmt.Errorf("%w: missing jwks_uri", errBadDiscoveryDoc)
	}

	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Use string `json:"use"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}

	if err := v.getJSON(disc.JWKSURI, &set); err != nil {
		return nil, err
	}

	keys := make(map[string]*rsa.PublicKey, len(set.Keys))

	for _, k := range set.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") {
			continue
		}

		nb, nerr := base64.RawURLEncoding.DecodeString(k.N)
		eb, eerr := base64.RawURLEncoding.DecodeString(k.E)

		if nerr != nil || eerr != nil {
			continue
		}

		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(new(big.Int).SetBytes(eb).Int64())}
	}

	return keys, nil
}

// oidcHTTPClient builds the OIDC fetch client, trusting caFile's PEM CAs in addition to the system roots.
func oidcHTTPClient(caFile string) (*http.Client, error) {
	c := httpclient.New(oidcHTTPTimeout)
	if caFile == "" {
		return c, nil
	}

	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read OIDC CA file: %w", err)
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}

	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%w: no certificates in %s", errBadDiscoveryDoc, caFile)
	}

	tr := httpclient.NewTransport()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	c.Transport = tr

	return c, nil
}

func allowedIssuerURL(u *url.URL) bool {
	return u.Scheme == "https" || (u.Scheme == "http" && isLoopbackHost(u.Hostname()))
}
