package elbv2

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
)

var (
	errNoCertificate    = errors.New("no usable certificate for listener")
	errNoMatchingServer = errors.New("no compatible certificate for requested server name")
)

const defaultSSLPolicy = "ELBSecurityPolicy-2016-08"

// goCipherSuite maps an OpenSSL cipher name from the security-policy tables to a Go suite.
// Suites Go does not implement (ECDHE CBC-SHA384, RSA-SHA256 AES256) report false.
func goCipherSuite(name string) (uint16, bool) {
	switch name {
	case "ECDHE-ECDSA-AES128-GCM-SHA256":
		return tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, true
	case "ECDHE-RSA-AES128-GCM-SHA256":
		return tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, true
	case "ECDHE-ECDSA-AES256-GCM-SHA384":
		return tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, true
	case "ECDHE-RSA-AES256-GCM-SHA384":
		return tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, true
	case "ECDHE-ECDSA-AES128-SHA256":
		return tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, true
	case "ECDHE-RSA-AES128-SHA256":
		return tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, true
	case "ECDHE-ECDSA-AES128-SHA":
		return tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, true
	case "ECDHE-RSA-AES128-SHA":
		return tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, true
	case "ECDHE-ECDSA-AES256-SHA":
		return tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, true
	case "ECDHE-RSA-AES256-SHA":
		return tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, true
	case "AES128-GCM-SHA256":
		return tls.TLS_RSA_WITH_AES_128_GCM_SHA256, true
	case "AES256-GCM-SHA384":
		return tls.TLS_RSA_WITH_AES_256_GCM_SHA384, true
	case "AES128-SHA256":
		return tls.TLS_RSA_WITH_AES_128_CBC_SHA256, true
	case "AES128-SHA":
		return tls.TLS_RSA_WITH_AES_128_CBC_SHA, true
	case "AES256-SHA":
		return tls.TLS_RSA_WITH_AES_256_CBC_SHA, true
	}

	return 0, false
}

func tlsVersionOf(proto string) uint16 {
	switch proto {
	case "TLSv1.3":
		return tls.VersionTLS13
	case "TLSv1.2":
		return tls.VersionTLS12
	case "TLSv1.1":
		return tls.VersionTLS11
	case "TLSv1":
		return tls.VersionTLS10
	}

	return 0
}

type policyTLS struct {
	ciphers []uint16
	min     uint16
	max     uint16
}

// policyTLSFor derives min/max protocol and TLS 1.2 suites from the catalog entry for name.
func policyTLSFor(name string) policyTLS {
	if name == "" {
		name = defaultSSLPolicy
	}

	groups := sslCipherGroups()

	for _, spec := range sslPolicySpecs() {
		if spec.name != name {
			continue
		}

		p := policyTLS{min: tls.VersionTLS13}

		for _, proto := range spec.protocols {
			v := tlsVersionOf(proto)
			p.min = min(p.min, v)
			p.max = max(p.max, v)
		}

		for _, c := range groups[spec.ciphers] {
			if id, ok := goCipherSuite(c); ok {
				p.ciphers = append(p.ciphers, id)
			}
		}

		return p
	}

	return policyTLS{min: tls.VersionTLS12, max: tls.VersionTLS13}
}

// opensslCipherName reports the policy-table spelling of a negotiated suite, as ALB's X-Amzn-Tls-Cipher-Suite does.
func opensslCipherName(id uint16) string {
	for _, g := range sslCipherGroups() {
		for _, n := range g {
			if gid, ok := goCipherSuite(n); ok && gid == id {
				return n
			}
		}
	}

	return tls.CipherSuiteName(id)
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLSv1.3"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS10:
		return "TLSv1"
	}

	return ""
}

func alpnProtocols(policy []string) []string {
	if len(policy) == 0 {
		return nil
	}

	switch policy[0] {
	case "HTTP1Only":
		return []string{"http/1.1", "http/1.0"}
	case "HTTP2Only":
		return []string{"h2"}
	case "HTTP2Optional":
		return []string{"http/1.1", "http/1.0", "h2"}
	case "HTTP2Preferred":
		return []string{"h2", "http/1.1", "http/1.0"}
	}

	return nil
}

func loadKeyPair(r CertificateResolver, arn string) (*tls.Certificate, error) {
	kp, ok := r.(CertificateKeyPairResolver)
	if !ok {
		return nil, fmt.Errorf("%w: resolver exposes no key material", errNoCertificate)
	}

	certPEM, keyPEM, found := kp.CertificateKeyPair(arn)
	if !found {
		return nil, fmt.Errorf("%w: %s", errNoCertificate, arn)
	}

	c, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", errNoCertificate, arn, err)
	}

	c.Leaf, err = x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", errNoCertificate, arn, err)
	}

	return &c, nil
}

// selectCertificate follows the documented ALB algorithm: a SNI match among the certificate list wins
// (ECDSA over RSA, then latest expiry); no SNI or no match falls back to the default certificate.
func selectCertificate(certs []*tls.Certificate, def *tls.Certificate, hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if hello.ServerName != "" {
		var matched []*tls.Certificate

		for _, c := range certs {
			if c.Leaf.VerifyHostname(hello.ServerName) == nil {
				matched = append(matched, c)
			}
		}

		if len(matched) > 0 {
			matched = slices.DeleteFunc(matched, func(c *tls.Certificate) bool {
				return hello.SupportsCertificate(c) != nil
			})
			if len(matched) == 0 {
				return nil, errNoMatchingServer
			}

			slices.SortStableFunc(matched, func(a, b *tls.Certificate) int {
				if ea, eb := isECDSA(a), isECDSA(b); ea != eb {
					if ea {
						return -1
					}

					return 1
				}

				return b.Leaf.NotAfter.Compare(a.Leaf.NotAfter)
			})

			return matched[0], nil
		}
	}

	if def == nil {
		return nil, errNoCertificate
	}

	return def, nil
}

func isECDSA(c *tls.Certificate) bool { return c.Leaf.PublicKeyAlgorithm == x509.ECDSA }

// clientConfig returns a GetConfigForClient that reads the listener's current policy and certificates
// on every handshake, so ModifyListener/AddListenerCertificates take effect without a rebind.
func (b *InMemoryBackend) clientConfig(listenerArn string, alpn bool) func(*tls.ClientHelloInfo) (*tls.Config, error) {
	return func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		l, _, ok := b.listenerView(listenerArn)
		if !ok {
			return nil, errNoCertificate
		}

		b.mu.RLock("clientConfig")
		resolver := b.certResolver
		b.mu.RUnlock()

		if resolver == nil {
			return nil, errNoCertificate
		}

		var (
			certs []*tls.Certificate
			def   *tls.Certificate
		)

		for i, c := range l.Certificates {
			kp, err := loadKeyPair(resolver, c.CertificateArn)
			if err != nil {
				continue
			}

			certs = append(certs, kp)

			if c.IsDefault || (def == nil && i == 0) {
				def = kp
			}
		}

		pick, err := selectCertificate(certs, def, hello)
		if err != nil {
			return nil, err
		}

		pol := policyTLSFor(l.SSLPolicy)
		cfg := &tls.Config{
			Certificates: []tls.Certificate{*pick},
			MinVersion:   pol.min,
			MaxVersion:   pol.max,
			CipherSuites: pol.ciphers,
		}

		if protos := alpnProtocols(l.AlpnPolicy); alpn && hasOverlap(protos, hello.SupportedProtos) {
			cfg.NextProtos = protos
		}

		return cfg, nil
	}
}

func hasOverlap(a, b []string) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}

	return false
}
