package ociapigateway

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxCAChain   = 3
	caBundleBody = 1 << 20
)

var (
	errNoGatewayCert = errors.New("no TLS certificate for this server name")
	errNoCAPEM       = errors.New("pem holds no certificate")
)

func mtlsRequired(rp *requestPolicies) *mtlsDoc {
	if rp != nil && rp.MutualTLS != nil && isTrue(rp.MutualTLS.IsVerifiedCertificateRequired) {
		return rp.MutualTLS
	}

	return nil
}

// RegisterCABundle stores the PEM behind a gateway caBundles entry (CA_BUNDLE or CERTIFICATE_AUTHORITY id);
// the emulator has no OCI Certificates service to resolve those ids from.
func (b *InMemoryBackend) RegisterCABundle(id, pemData string) error {
	rest, n := []byte(pemData), 0

	for {
		var blk *pem.Block

		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}

		if _, err := x509.ParseCertificate(blk.Bytes); err != nil {
			return err
		}

		n++
	}

	if n == 0 {
		return errNoCAPEM
	}

	b.mu.Lock("RegisterCABundle")
	b.caPEMs[id] = pemData
	b.mu.Unlock()

	return nil
}

// RemoveCABundle deletes a registered CA bundle PEM.
func (b *InMemoryBackend) RemoveCABundle(id string) bool {
	b.mu.Lock("RemoveCABundle")
	defer b.mu.Unlock()

	_, ok := b.caPEMs[id]
	delete(b.caPEMs, id)

	return ok
}

type caBundleRef struct {
	CaBundleID             string `json:"caBundleId"`
	CertificateAuthorityID string `json:"certificateAuthorityId"`
}

// clientCAPool builds the trust store of a gateway from its caBundles entries.
func (b *InMemoryBackend) clientCAPool(gwID string) *x509.CertPool {
	b.mu.RLock("clientCAPool")
	defer b.mu.RUnlock()

	g := b.gateways[gwID]
	if g == nil {
		return nil
	}

	var refs []caBundleRef
	if json.Unmarshal(g.CaBundles, &refs) != nil {
		return nil
	}

	pool := x509.NewCertPool()
	added := false

	for _, r := range refs {
		for _, id := range []string{r.CaBundleID, r.CertificateAuthorityID} {
			if pemData, ok := b.caPEMs[id]; ok && id != "" && pool.AppendCertsFromPEM([]byte(pemData)) {
				added = true
			}
		}
	}

	if !added {
		return nil
	}

	return pool
}

// sanMatch matches a value against an allowedSans pattern: case-insensitive, with '*' only at the start and/or end.
func sanMatch(pattern, name string) bool {
	pattern, name = strs.Fold(pattern), strs.Fold(name)
	pre, suf := strings.HasPrefix(pattern, "*"), strings.HasSuffix(pattern, "*") && len(pattern) > 1
	core := strings.TrimSuffix(strings.TrimPrefix(pattern, "*"), "*")

	switch {
	case pre && suf:
		return strings.Contains(name, core)
	case pre:
		return strings.HasSuffix(name, core)
	case suf:
		return strings.HasPrefix(name, core)
	default:
		return name == pattern
	}
}

func certNames(c *x509.Certificate) []string {
	names := slices.Clone(c.DNSNames)
	names = append(names, c.EmailAddresses...)

	for _, u := range c.URIs {
		names = append(names, u.String())
	}

	if c.Subject.CommonName != "" {
		names = append(names, c.Subject.CommonName)
	}

	return names
}

// checkMTLS verifies the client certificate against the gateway trust store and the allowedSans.
// Plain-HTTP requests carry no certificate and are refused when mTLS is required.
func (b *InMemoryBackend) checkMTLS(r *http.Request, tgt target) denial {
	pol := mtlsRequired(tgt.spec.doc.RequestPolicies)
	if pol == nil {
		return denial{}
	}

	refuse := denial{status: http.StatusUnauthorized, msg: "Unauthorized"}

	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return refuse
	}

	pool := b.clientCAPool(tgt.gateway)
	if pool == nil {
		return refuse
	}

	inter := x509.NewCertPool()
	for _, c := range r.TLS.PeerCertificates[1:] {
		inter.AddCert(c)
	}

	leaf := r.TLS.PeerCertificates[0]

	chains, err := leaf.Verify(x509.VerifyOptions{
		Roots: pool, Intermediates: inter, CurrentTime: b.clock(),
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil || !slices.ContainsFunc(chains, func(c []*x509.Certificate) bool { return len(c)-1 <= maxCAChain }) {
		return refuse
	}

	if len(pol.AllowedSans) == 0 {
		return denial{}
	}

	for _, name := range certNames(leaf) {
		if slices.ContainsFunc(pol.AllowedSans, func(p string) bool { return sanMatch(p, name) }) {
			return denial{}
		}
	}

	return refuse
}

// wantsClientCert reports whether any servable deployment of the gateway requires client certificates.
func (b *InMemoryBackend) wantsClientCert(gwID string) bool {
	for id, d := range b.deployments {
		if d.GatewayID == gwID && servable(d.State) && b.specs[id] != nil &&
			mtlsRequired(b.specs[id].doc.RequestPolicies) != nil {
			return true
		}
	}

	return false
}

// tlsConfigFor serves the gateway certificate selected by SNI. Client certificates are requested, not verified
// here: verification against the gateway trust store happens per deployment in checkMTLS.
func (b *InMemoryBackend) tlsConfigFor(hello *tls.ClientHelloInfo) (*tls.Config, error) {
	b.mu.RLock("tlsConfigFor")
	defer b.mu.RUnlock()

	for _, g := range b.gateways {
		if !servable(g.State) || !strs.Equal(g.Hostname, hello.ServerName) {
			continue
		}

		c := b.certificates[g.CertificateID]
		if c == nil || c.State == stateDeleted || c.PrivateKey == "" {
			break
		}

		pair, err := tls.X509KeyPair([]byte(c.CertPEM+"\n"+c.Intermediate), []byte(c.PrivateKey))
		if err != nil {
			return nil, err
		}

		cfg := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
		if b.wantsClientCert(g.ID) {
			cfg.ClientAuth = tls.RequestClientCert
		}

		return cfg, nil
	}

	return nil, errNoGatewayCert
}

type caBundleReq struct {
	CaBundleID string `json:"caBundleId"`
	PEM        string `json:"pem"`
}

// adminCABundles registers the PEM behind a gateway caBundles id (/_gopherstack/oci/apigateway/ca-bundles).
func (h *Handler) adminCABundles(q *req) {
	switch q.r.Method {
	case http.MethodPost:
		var in caBundleReq

		body, _ := io.ReadAll(io.LimitReader(q.r.Body, caBundleBody))
		if err := json.Unmarshal(body, &in); err != nil || in.CaBundleID == "" {
			h.fail(q, errParam("caBundleId and pem are required"))

			return
		}

		if err := h.Backend.RegisterCABundle(in.CaBundleID, in.PEM); err != nil {
			h.fail(q, errParam(err.Error()))

			return
		}

		_ = q.json(http.StatusCreated, map[string]string{"caBundleId": in.CaBundleID}, "")
	case http.MethodDelete:
		if !h.Backend.RemoveCABundle(q.r.URL.Query().Get("caBundleId")) {
			h.fail(q, newErr(http.StatusNotFound, "NotFound", "No such CA bundle"))

			return
		}

		q.w.WriteHeader(http.StatusNoContent)
	default:
		h.fail(q, errNoRoute())
	}
}
