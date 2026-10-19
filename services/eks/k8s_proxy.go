package eks

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	proxyReadHeaderTimeout = 10 * time.Second
	proxyLeafValidity      = 5 * 365 * 24 * time.Hour
	awsAuthCacheTTL        = 2 * time.Second
	awsAuthFetchTimeout    = 5 * time.Second
	impersonatePrefix      = "Impersonate-"
	anonymousUser          = "system:anonymous"
	unauthenticatedGroup   = "system:unauthenticated"
)

var errNoPEM = errors.New("no PEM block")

type awsAuthCache struct {
	at   time.Time
	maps awsAuthMaps
}

// k8sProxy terminates TLS on the cluster endpoint and authenticates `aws eks get-token` bearers
// before forwarding to k3s as the admin user with Kubernetes impersonation headers.
type k8sProxy struct {
	b       *InMemoryBackend
	api     *k8sAPI
	srv     *http.Server
	cache   atomic.Pointer[awsAuthCache]
	cluster string
	token   string
	port    int
}

func issueProxyLeaf(ca *clusterCA, host string) (tls.Certificate, error) {
	certBlk, _ := pem.Decode(ca.certPEM)
	keyBlk, _ := pem.Decode(ca.keyPEM)

	if certBlk == nil || keyBlk == nil {
		return tls.Certificate{}, fmt.Errorf("parse cluster CA: %w", errNoPEM)
	}

	caCert, err := x509.ParseCertificate(certBlk.Bytes)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse cluster CA certificate: %w", err)
	}

	caKey, err := x509.ParseECPrivateKey(keyBlk.Bytes)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse cluster CA key: %w", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate proxy key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), clusterCASerialBits))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate proxy serial: %w", err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "gopherstack-eks-proxy"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(proxyLeafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}

	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	} else {
		tmpl.DNSNames = append(tmpl.DNSNames, host)
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("sign proxy certificate: %w", err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}, nil
}

func (e *clusterEngine) proxyListener(name string) (net.Listener, error) {
	port := 0

	if e.cfg.Ports != nil {
		p, err := e.cfg.Ports.Acquire("eks-" + name + "-proxy")
		if err != nil {
			return nil, fmt.Errorf("acquire proxy port: %w", err)
		}

		port = p
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(port)))
	if err != nil {
		e.releasePort(port)

		return nil, fmt.Errorf("listen for proxy: %w", err)
	}

	return ln, nil
}

// startProxy serves the authenticating endpoint for a ready cluster.
func (b *InMemoryBackend) startProxy(name string, ca *clusterCA, api *k8sAPI) (*k8sProxy, error) {
	e := b.clusterEng

	leaf, err := issueProxyLeaf(ca, e.cfg.Host)
	if err != nil {
		return nil, err
	}

	ln, err := e.proxyListener(name)
	if err != nil {
		return nil, err
	}

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()

		return nil, fmt.Errorf("proxy listener: %w", errNoPEM)
	}

	target, err := url.Parse(api.base)
	if err != nil {
		_ = ln.Close()
		e.releasePort(addr.Port)

		return nil, fmt.Errorf("parse API address: %w", err)
	}

	p := &k8sProxy{b: b, api: api, cluster: name, token: e.cfg.Token, port: addr.Port}
	rp := &httputil.ReverseProxy{
		Rewrite:       func(pr *httputil.ProxyRequest) { pr.SetURL(target) },
		Transport:     api.tr,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeK8sStatus(w, http.StatusBadGateway, "BadGateway")
		},
	}

	p.srv = &http.Server{
		Handler:           p.handler(rp),
		ReadHeaderTimeout: proxyReadHeaderTimeout,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{leaf}},
		TLSNextProto:      map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}

	e.wg.Go(func() { _ = p.srv.ServeTLS(ln, "", "") })

	return p, nil
}

func (p *k8sProxy) close() { _ = p.srv.Close() }

func writeK8sStatus(w http.ResponseWriter, code int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		keyKind: "Status", keyAPIVersion: "v1", "status": "Failure",
		keyMessage: reason, "reason": reason, keyCode: code,
	})
}

func (p *k8sProxy) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		scrubCredentialHeaders(r.Header)

		bearer, isBearer := strings.CutPrefix(authz, "Bearer ")

		switch {
		case authz == "":
			setImpersonation(r.Header, k8sIdentity{Username: anonymousUser, Groups: []string{unauthenticatedGroup}})
			r.Header.Set("Authorization", "Bearer "+p.token)
		case isBearer && subtle.ConstantTimeCompare([]byte(bearer), []byte(p.token)) == 1:
			r.Header.Set("Authorization", "Bearer "+p.token)
		case isBearer && strings.HasPrefix(bearer, IAMTokenPrefix):
			id, ok := p.authenticate(r.Context(), bearer)
			if !ok {
				writeK8sStatus(w, http.StatusUnauthorized, "Unauthorized")

				return
			}

			setImpersonation(r.Header, id)
			r.Header.Set("Authorization", "Bearer "+p.token)
		default:
			writeK8sStatus(w, http.StatusUnauthorized, "Unauthorized")

			return
		}

		next.ServeHTTP(w, r)
	})
}

func scrubCredentialHeaders(h http.Header) {
	h.Del("Authorization")

	for k := range h {
		if strings.HasPrefix(k, impersonatePrefix) {
			h.Del(k)
		}
	}
}

func setImpersonation(h http.Header, id k8sIdentity) {
	h.Set(impersonatePrefix+"User", id.Username)

	for _, g := range id.Groups {
		h.Add(impersonatePrefix+"Group", g)
	}

	for k, vals := range id.Extra {
		for _, v := range vals {
			if v != "" {
				h.Add(impersonatePrefix+"Extra-"+k, v)
			}
		}
	}
}

func (p *k8sProxy) authenticate(ctx context.Context, bearer string) (k8sIdentity, bool) {
	res := p.b.credentialResolver()

	cred, accessKeyID, err := authenticateIAMToken(res, bearer, p.cluster, time.Now())
	if err != nil {
		return k8sIdentity{}, false
	}

	pr, ok := parsePrincipal(cred.PrincipalARN)
	if !ok {
		return k8sIdentity{}, false
	}

	awsAuth := p.awsAuth(ctx)

	p.b.mu.RLock("k8sAuthenticate")
	defer p.b.mu.RUnlock()

	cl, found := p.b.clusters.Get(p.cluster)
	if !found {
		return k8sIdentity{}, false
	}

	return p.b.k8sIdentityForLocked(cl, pr, accessKeyID, awsAuth)
}

func (p *k8sProxy) awsAuth(ctx context.Context) awsAuthMaps {
	if c := p.cache.Load(); c != nil && time.Since(c.at) < awsAuthCacheTTL {
		return c.maps
	}

	ctx, cancel := context.WithTimeout(ctx, awsAuthFetchTimeout)
	defer cancel()

	data, err := p.api.getConfigMapData(ctx, "kube-system", "aws-auth")
	if err != nil {
		return awsAuthMaps{}
	}

	m := parseAwsAuth(data)
	p.cache.Store(&awsAuthCache{at: time.Now(), maps: m})

	return m
}

// SetCredentialResolver sets how EKS verifies `aws eks get-token` bearers; call before serving traffic.
func (b *InMemoryBackend) SetCredentialResolver(r CredentialResolver) {
	b.mu.Lock("SetCredentialResolver")
	defer b.mu.Unlock()

	b.credentials = r

	if b.clusterEng != nil {
		b.clusterEng.cfg.Credentials = r
	}
}

func (b *InMemoryBackend) credentialResolver() CredentialResolver {
	b.mu.RLock("credentialResolver")
	defer b.mu.RUnlock()

	return b.credentials
}

// SetCredentialResolver wires r into every region's backend, including siblings built later.
func (h *Handler) SetCredentialResolver(r CredentialResolver) {
	for _, bk := range h.RegionBackends() {
		bk.SetCredentialResolver(r)
	}
}
