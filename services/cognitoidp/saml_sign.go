package cognitoidp

import (
	"bytes"
	"compress/flate"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

const (
	detailRequestSigning = "RequestSigningAlgorithm"
	detailIDPSignout     = "IDPSignout"
	detailIDPInit        = "IDPInit"
	signAlgRSASHA256     = "rsa-sha256"
	paramSAMLRequest     = "SAMLRequest"
	paramSAMLResponse    = "SAMLResponse"
	paramRelayState      = "RelayState"
	paramSigAlg          = "SigAlg"
	paramSignature       = "Signature"
)

func detailTrue(details map[string]string, key string) bool {
	return strings.EqualFold(details[key], "true")
}

// spIdentity is the pool's SAML key pair: the JWT signing key doubles as SP signing and encryption key.
type spIdentity struct {
	key     *rsa.PrivateKey
	certDER []byte
}

func (b *InMemoryBackend) spIdentity(poolID string) (*spIdentity, error) {
	b.mu.RLock("SPIdentity")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	certPEM, err := pool.issuer.SigningCertificatePEM()
	if err != nil {
		return nil, fmt.Errorf("pool signing certificate: %w", err)
	}

	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return nil, samlErrorf("pool signing certificate is not PEM")
	}

	if _, err = x509.ParseCertificate(block.Bytes); err != nil {
		return nil, samlErrorf("pool signing certificate: %v", err)
	}

	return &spIdentity{key: pool.issuer.privateKey, certDER: block.Bytes}, nil
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)

	return sum[:]
}

// signEnveloped adds an enveloped exclusive-C14N RSA-SHA256 signature of doc's root element right after marker.
func signEnveloped(doc, marker string, key *rsa.PrivateKey) (string, error) {
	root, err := parseXML([]byte(doc))
	if err != nil {
		return "", samlErrorf("sign: %v", err)
	}

	id := root.attr(attrNameID)
	idx := strings.Index(doc, marker)

	if id == "" || idx < 0 {
		return "", samlErrorf("sign: document has no ID or insertion point")
	}

	info := `<ds:CanonicalizationMethod Algorithm="` + nsExcC14N + `"></ds:CanonicalizationMethod>` +
		`<ds:SignatureMethod Algorithm="` + algRSASHA256 + `"></ds:SignatureMethod>` +
		`<ds:Reference URI="#` + id + `"><ds:Transforms>` +
		`<ds:Transform Algorithm="` + algEnveloped + `"></ds:Transform>` +
		`<ds:Transform Algorithm="` + nsExcC14N + `"></ds:Transform></ds:Transforms>` +
		`<ds:DigestMethod Algorithm="` + algDigest256 + `"></ds:DigestMethod>` +
		`<ds:DigestValue>` + base64.StdEncoding.EncodeToString(sha256Sum(canonicalize(root, nil, nil))) +
		`</ds:DigestValue></ds:Reference>`

	canonInfo := `<ds:SignedInfo xmlns:ds="` + nsDSig + `">` + info + `</ds:SignedInfo>`

	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sha256Sum([]byte(canonInfo)))
	if err != nil {
		return "", samlErrorf("sign: %v", err)
	}

	sigXML := `<ds:Signature xmlns:ds="` + nsDSig + `"><ds:SignedInfo>` + info + `</ds:SignedInfo>` +
		`<ds:SignatureValue>` + base64.StdEncoding.EncodeToString(sig) + `</ds:SignatureValue></ds:Signature>`
	at := idx + len(marker)

	return doc[:at] + sigXML + doc[at:], nil
}

// signedRedirectURL builds an HTTP-Redirect binding URL carrying a detached RSA-SHA256 query signature.
func signedRedirectURL(sso, param, message, relay string, key *rsa.PrivateKey) (string, error) {
	target, err := url.Parse(sso)
	if err != nil {
		return "", samlErrorf("destination: %v", err)
	}

	signed := param + "=" + url.QueryEscape(message)
	if relay != "" {
		signed += "&" + paramRelayState + "=" + url.QueryEscape(relay)
	}

	signed += "&" + paramSigAlg + "=" + url.QueryEscape(algRSASHA256)

	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sha256Sum([]byte(signed)))
	if err != nil {
		return "", samlErrorf("sign: %v", err)
	}

	query := signed + "&" + paramSignature + "=" + url.QueryEscape(base64.StdEncoding.EncodeToString(sig))
	if target.RawQuery != "" {
		query = target.RawQuery + "&" + query
	}

	target.RawQuery = query

	return target.String(), nil
}

// samlInbound is an undecoded-trust SAML protocol message from the IdP; verify before using its content.
type samlInbound struct {
	root     *xnode
	relay    string
	rawQuery string
	redirect bool
}

// readSAMLMessage decodes the message in param from an HTTP-Redirect (GET) or HTTP-POST request.
func readSAMLMessage(r *http.Request, param string) (*samlInbound, error) {
	if r.Method == http.MethodGet {
		raw, err := inflateBase64(r.URL.Query().Get(param))
		if err != nil {
			return nil, err
		}

		root, err := parseXML(raw)
		if err != nil {
			return nil, samlErrorf("%v", err)
		}

		return &samlInbound{
			root: root, relay: r.URL.Query().Get(paramRelayState), rawQuery: r.URL.RawQuery, redirect: true,
		}, nil
	}

	raw, err := base64.StdEncoding.DecodeString(stripSpace(r.PostForm.Get(param)))
	if err != nil || len(raw) == 0 {
		return nil, samlErrorf("%s is missing or not base64", param)
	}

	root, err := parseXML(raw)
	if err != nil {
		return nil, samlErrorf("%v", err)
	}

	return &samlInbound{root: root, relay: r.PostForm.Get(paramRelayState)}, nil
}

func inflateBase64(s string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(stripSpace(s))
	if err != nil || len(raw) == 0 {
		return nil, samlErrorf("message is missing or not base64")
	}

	out, err := io.ReadAll(io.LimitReader(flate.NewReader(bytes.NewReader(raw)), maxSAMLResponse))
	if err != nil {
		return nil, samlErrorf("message is not deflated")
	}

	return out, nil
}

// verify requires a signature from keys: a detached query signature (redirect) or an enveloped
// signature on the message root itself (POST), so no other signed element can stand in for it.
func (m *samlInbound) verify(param string, keys []*rsa.PublicKey) error {
	if m.redirect {
		return verifyRedirectSignature(m.rawQuery, param, keys)
	}

	signed, err := verifiedElements(m.root, keys)
	if err != nil {
		return err
	}

	if slices.Contains(signed, m.root) {
		return nil
	}

	return sigErrorf("message root is not covered by a valid signature")
}

func verifyRedirectSignature(rawQuery, param string, keys []*rsa.PublicKey) error {
	parts := map[string]string{}

	for kv := range strings.SplitSeq(rawQuery, "&") {
		k, v, _ := strings.Cut(kv, "=")
		if _, dup := parts[k]; dup {
			return sigErrorf("duplicate query parameter %q", k)
		}

		parts[k] = v
	}

	algName, err := url.QueryUnescape(parts[paramSigAlg])
	h, ok := hashFor(algName)

	if err != nil || !ok || !strings.Contains(algName, "rsa-sha") {
		return sigErrorf("unsupported or missing SigAlg")
	}

	sigEsc, err := url.QueryUnescape(parts[paramSignature])
	if err != nil {
		return sigErrorf("signature is not url-encoded")
	}

	sig, err := base64.StdEncoding.DecodeString(stripSpace(sigEsc))
	if err != nil || parts[param] == "" {
		return sigErrorf("signature is missing or not base64")
	}

	signed := param + "=" + parts[param]
	if rs, has := parts[paramRelayState]; has {
		signed += "&" + paramRelayState + "=" + rs
	}

	signed += "&" + paramSigAlg + "=" + parts[paramSigAlg]
	sum := digest(h, []byte(signed))

	for _, k := range keys {
		if rsa.VerifyPKCS1v15(k, h, sum, sig) == nil {
			return nil
		}
	}

	return sigErrorf("query signature does not verify against the identity provider certificates")
}
