package azurearm

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"net/http"
	"strings"

	"software.sslmate.com/src/go-pkcs12"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	apimCAuthCertificate = "authentication-certificate"
	apimMaxCertClients   = 64
)

// gwCert is the part of a stored certificate the gateway needs to present it or to read its public key.
type gwCert struct {
	thumbprint string
	data       string
	password   string
	spki       string
	hasKey     bool
}

func (s *apimService) certMap() map[string]gwCert {
	ents := s.Generic[apimCertificatesType]
	out := make(map[string]gwCert, len(ents))

	for _, e := range ents {
		hasKey, _ := e.Props["privateKey"].(bool)
		out[strings.ToLower(e.ID)] = gwCert{
			thumbprint: strOf(
				e.Props,
				apimCThumbprint,
			), data: strOf(e.Props, "data"), password: strOf(e.Props, apimCPasswordAttr),
			spki: strOf(e.Props, "publicKey"), hasKey: hasKey,
		}
	}

	return out
}

func (c gwCert) tlsCert() (tls.Certificate, bool) {
	raw, err := base64.StdEncoding.DecodeString(c.data)
	if err != nil {
		return tls.Certificate{}, false
	}

	return apimTLSFromPFX(raw, c.password)
}

func apimTLSFromPFX(raw []byte, password string) (tls.Certificate, bool) {
	key, cert, chain, err := pkcs12.DecodeChain(raw, password)
	if err != nil || cert == nil || key == nil {
		return tls.Certificate{}, false
	}

	out := tls.Certificate{Certificate: [][]byte{cert.Raw}, PrivateKey: key, Leaf: cert}
	for _, c := range chain {
		out.Certificate = append(out.Certificate, c.Raw)
	}

	return out, true
}

// apimCertClients caches one HTTP client per client certificate, each cloned from the gateway's own transport so
// the loopback dial policy stays in force.
type apimCertClients struct {
	mu *lockmetrics.RWMutex
	m  map[string]*http.Client
}

func newAPIMCertClients() *apimCertClients {
	return &apimCertClients{mu: lockmetrics.New("azurearm.apim.certclients"), m: map[string]*http.Client{}}
}

func (c *apimCertClients) reset() {
	c.mu.Lock("reset")
	c.m = map[string]*http.Client{}
	c.mu.Unlock()
}

// SetBackendRootCAs adds trust anchors for https backends; call it before the gateway serves traffic.
func (p *APIMProvider) SetBackendRootCAs(pool *x509.CertPool) {
	p.rootCAs = pool
	p.client = p.newBackendClient()
	p.certClients.reset()
}

func (p *APIMProvider) clientWithCert(key string, cert tls.Certificate) *http.Client {
	p.certClients.mu.Lock("clientWithCert")
	defer p.certClients.mu.Unlock()

	if c, ok := p.certClients.m[key]; ok {
		return c
	}

	if len(p.certClients.m) >= apimMaxCertClients {
		for k, c := range p.certClients.m {
			c.CloseIdleConnections()
			delete(p.certClients.m, k)

			break
		}
	}

	base, _ := p.client.Transport.(*http.Transport)
	tr := base.Clone()

	cfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: p.rootCAs}
	if tr.TLSClientConfig != nil {
		cfg = tr.TLSClientConfig.Clone()
		cfg.MinVersion = max(cfg.MinVersion, tls.VersionTLS12)
	}

	cfg.Certificates = []tls.Certificate{cert}
	tr.TLSClientConfig = cfg

	c := &http.Client{Transport: tr, CheckRedirect: p.client.CheckRedirect}
	p.certClients.m[key] = c

	return c
}

// authCert resolves the client certificate of an authentication-certificate statement.
func (g *gwState) authCert(n *polNode) {
	var (
		cert tls.Certificate
		ok   bool
		key  string
	)

	switch {
	case n.Attrs[apimCBodyAttr] != "":
		raw, err := base64.StdEncoding.DecodeString(g.sub(n.Attrs[apimCBodyAttr]))
		if err == nil {
			cert, ok = apimTLSFromPFX(raw, g.sub(n.Attrs[apimCPasswordAttr]))
		}

		key = apimKeyOf(g.plan.svcKey, apimCBodyAttr, n.Attrs[apimCBodyAttr])
	case n.Attrs[apimCCertID] != "":
		c, found := g.plan.certs[strings.ToLower(g.sub(n.Attrs[apimCCertID]))]
		if found && c.hasKey {
			cert, ok = c.tlsCert()
		}

		key = apimKeyOf(g.plan.svcKey, "id", c.thumbprint)
	default:
		th := strings.ToUpper(g.sub(n.Attrs[apimCThumbprint]))

		for _, c := range g.plan.certs {
			if c.thumbprint == th && c.hasKey {
				cert, ok = c.tlsCert()
			}
		}

		key = apimKeyOf(g.plan.svcKey, "id", th)
	}

	if !ok {
		g.failR(http.StatusInternalServerError, "ClientCertificateNotFound",
			"The client certificate for authentication-certificate could not be loaded.")

		return
	}

	g.client = g.p.clientWithCert(key, cert)
}

func validateAuthCert(s *apimService, n *polNode) error {
	count := 0

	for _, a := range []string{apimCThumbprint, apimCCertID, apimCBodyAttr} {
		if n.Attrs[a] != "" {
			count++
		}
	}

	if count != 1 {
		return polErr(n, "Exactly one of thumbprint, certificate-id and body is required.")
	}

	if n.Attrs[apimCBodyAttr] != "" && n.Attrs[apimCPasswordAttr] == "" {
		return polErr(n, "The password is required with body.")
	}

	for _, a := range []string{apimCThumbprint, apimCCertID} {
		v := n.Attrs[a]
		if _, expr, _ := apimIsExprText(v); expr || v == "" {
			continue
		}

		if !s.hasClientCert(a, v) {
			return polErr(n, "No uploaded certificate with a private key matches '%s'.", v)
		}
	}

	return nil
}

func (s *apimService) hasClientCert(attr, v string) bool {
	for _, e := range s.Generic[apimCertificatesType] {
		hasKey, _ := e.Props["privateKey"].(bool)
		if !hasKey {
			continue
		}

		if (attr == apimCCertID && strings.EqualFold(e.ID, v)) ||
			(attr == apimCThumbprint && strings.EqualFold(strOf(e.Props, apimCThumbprint), v)) {
			return true
		}
	}

	return false
}

func (g *gwState) httpClient() *http.Client {
	if g.client != nil {
		return g.client
	}

	return g.p.client
}

// sendClient is the client of a send-request, using its own authentication-certificate child when present.
func (g *gwState) sendClient(n *polNode) *http.Client {
	if ac := n.kid(apimCAuthCertificate); ac != nil {
		saved := g.client
		g.authCert(ac)

		c := g.client
		g.client = saved

		if c == nil {
			return g.p.client
		}

		return c
	}

	return g.p.client
}
