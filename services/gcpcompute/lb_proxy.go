package gcpcompute

import (
	"slices"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maxProxyCerts   = 15
	keepAliveMin    = 5
	keepAliveMax    = 600
	proxyHeaderNone = "NONE"
	proxyHeaderV1   = "PROXY_V1"
	tlsEarlyDataOff = "DISABLED"
	quicNone        = "NONE"
)

func httpProxySpec() lbSpec[computeapi.TargetHttpProxy] {
	return lbSpec[computeapi.TargetHttpProxy]{
		coll: collHTTPProxy, kind: "compute#targetHttpProxy", noun: "target_http_proxy",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetHttpProxy { return b.httpProxies },
		meta: func(v *computeapi.TargetHttpProxy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: func(b *InMemoryBackend, c lbCtx, in *computeapi.TargetHttpProxy, _ map[string]any) *APIError {
			if err := keepAliveOK(in.HttpKeepAliveTimeoutSec); err != nil {
				return err
			}

			return b.resolveProxyMapLocked(c, &in.UrlMap)
		},
		refs: func(v *computeapi.TargetHttpProxy) []string { return []string{v.UrlMap} },
	}
}

func httpsProxySpec() lbSpec[computeapi.TargetHttpsProxy] {
	return lbSpec[computeapi.TargetHttpsProxy]{
		coll: collHTTPSProxy, kind: "compute#targetHttpsProxy", noun: "target_https_proxy",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetHttpsProxy { return b.httpsProxies },
		meta: func(v *computeapi.TargetHttpsProxy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: prepHTTPSProxy,
		refs: func(v *computeapi.TargetHttpsProxy) []string {
			return append(slices.Clone(v.SslCertificates), v.UrlMap, v.SslPolicy)
		},
	}
}

func tcpProxySpec() lbSpec[computeapi.TargetTcpProxy] {
	return lbSpec[computeapi.TargetTcpProxy]{
		coll: collTCPProxy, kind: "compute#targetTcpProxy", noun: "target_tcp_proxy",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetTcpProxy { return b.tcpProxies },
		meta: func(v *computeapi.TargetTcpProxy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: func(b *InMemoryBackend, c lbCtx, in *computeapi.TargetTcpProxy, _ map[string]any) *APIError {
			if err := proxyHeaderOK(&in.ProxyHeader); err != nil {
				return err
			}

			return b.resolveProxyServiceLocked(c, &in.Service)
		},
		refs: func(v *computeapi.TargetTcpProxy) []string { return []string{v.Service} },
	}
}

func sslProxySpec() lbSpec[computeapi.TargetSslProxy] {
	return lbSpec[computeapi.TargetSslProxy]{
		coll: collSSLProxy, kind: "compute#targetSslProxy", noun: "target_ssl_proxy",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetSslProxy { return b.sslProxies },
		meta: func(v *computeapi.TargetSslProxy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink, id: &v.Id,
			}
		},
		prep: prepSSLProxy,
		refs: func(v *computeapi.TargetSslProxy) []string {
			return append(slices.Clone(v.SslCertificates), v.Service, v.SslPolicy)
		},
	}
}

func keepAliveOK(v int64) *APIError {
	if v != 0 && (v < keepAliveMin || v > keepAliveMax) {
		return errInvalid("Invalid value for field 'resource.httpKeepAliveTimeoutSec': '%d'. Must be between 5 and 600.", v)
	}

	return nil
}

func proxyHeaderOK(h *string) *APIError {
	if *h == "" {
		*h = proxyHeaderNone
	}

	if *h != proxyHeaderNone && *h != proxyHeaderV1 {
		return errField("resource.proxyHeader", *h, "Must be NONE or PROXY_V1.")
	}

	return nil
}

func (b *InMemoryBackend) resolveProxyMapLocked(c lbCtx, ref *string) *APIError {
	if *ref == "" {
		return errRequired("resource.urlMap")
	}

	l, err := lbResolve(b, urlMapSpec(), c, "resource.urlMap", *ref)
	if err != nil {
		return err
	}

	*ref = l

	return nil
}

func (b *InMemoryBackend) resolveProxyServiceLocked(c lbCtx, ref *string) *APIError {
	if *ref == "" {
		return errRequired("resource.service")
	}

	l, err := lbResolve(b, bsSpec(), c, "resource.service", *ref)
	if err != nil {
		return err
	}

	*ref = l

	return nil
}

func (b *InMemoryBackend) resolveProxyCertsLocked(c lbCtx, certs *[]string, optional bool) *APIError {
	if len(*certs) == 0 && !optional {
		return errRequired("resource.sslCertificates")
	}

	if len(*certs) > maxProxyCerts {
		return errField("resource.sslCertificates", "", "At most 15 certificates are allowed.")
	}

	out, err := lbResolveAll(b, sslCertSpec(), c, "resource.sslCertificates", *certs)
	if err != nil {
		return err
	}

	*certs = out

	return nil
}

func (b *InMemoryBackend) resolveProxyPolicyLocked(c lbCtx, ref *string) *APIError {
	if *ref == "" {
		return nil
	}

	if c.scope != scopeGlobal {
		return errField("resource.sslPolicy", *ref, "SSL policies are global.")
	}

	l, err := lbResolve(b, sslPolicySpec(), c, "resource.sslPolicy", *ref)
	if err != nil {
		return err
	}

	*ref = l

	return nil
}

func prepHTTPSProxy(b *InMemoryBackend, c lbCtx, in *computeapi.TargetHttpsProxy, _ map[string]any) *APIError {
	if err := keepAliveOK(in.HttpKeepAliveTimeoutSec); err != nil {
		return err
	}

	if in.QuicOverride == "" {
		in.QuicOverride = quicNone
	}

	if !slices.Contains([]string{quicNone, "ENABLE", "DISABLE"}, in.QuicOverride) {
		return errField("resource.quicOverride", in.QuicOverride, "")
	}

	if in.TlsEarlyData != "" && !slices.Contains([]string{tlsEarlyDataOff, "PERMISSIVE", "STRICT", "UNRESTRICTED"}, in.TlsEarlyData) {
		return errField("resource.tlsEarlyData", in.TlsEarlyData, "")
	}

	if err := b.resolveProxyMapLocked(c, &in.UrlMap); err != nil {
		return err
	}

	if err := b.resolveProxyCertsLocked(c, &in.SslCertificates, in.CertificateMap != ""); err != nil {
		return err
	}

	return b.resolveProxyPolicyLocked(c, &in.SslPolicy)
}

func prepSSLProxy(b *InMemoryBackend, c lbCtx, in *computeapi.TargetSslProxy, _ map[string]any) *APIError {
	if err := proxyHeaderOK(&in.ProxyHeader); err != nil {
		return err
	}

	if err := b.resolveProxyServiceLocked(c, &in.Service); err != nil {
		return err
	}

	if err := b.resolveProxyCertsLocked(c, &in.SslCertificates, in.CertificateMap != ""); err != nil {
		return err
	}

	return b.resolveProxyPolicyLocked(c, &in.SslPolicy)
}
