package gcpcompute

import (
	"crypto/tls"
	"crypto/x509"
	"regexp"
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	certSelfManaged = "SELF_MANAGED"
	certManaged     = "MANAGED"
	certProvision   = "PROVISIONING"
	maxManagedSANs  = 100
	maxDomainLen    = 253
	profCompatible  = "COMPATIBLE"
	profModern      = "MODERN"
	profRestricted  = "RESTRICTED"
	profFIPS        = "FIPS_202205"
	profCustom      = "CUSTOM"
	tls10           = "TLS_1_0"
	tls13           = "TLS_1_3"
)

var domainRE = regexp.MustCompile(`^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\.)+[a-z]([-a-z0-9]{0,61}[a-z0-9])?\.?$`)

var sslFeatures = []string{ //nolint:gochecknoglobals // read-only lookup table
	"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
	"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
	"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
	"TLS_RSA_WITH_3DES_EDE_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_GCM_SHA256",
	"TLS_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_GCM_SHA384",
}

func sslCertSpec() lbSpec[computeapi.SslCertificate] {
	return lbSpec[computeapi.SslCertificate]{
		coll: collSSLCerts, kind: "compute#sslCertificate", noun: "ssl_certificate",
		m: func(b *InMemoryBackend) map[string]*computeapi.SslCertificate { return b.sslCerts },
		meta: func(v *computeapi.SslCertificate) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: prepSSLCert,
	}
}

func sslPolicySpec() lbSpec[computeapi.SslPolicy] {
	return lbSpec[computeapi.SslPolicy]{
		coll: collSSLPolicies, kind: "compute#sslPolicy", noun: "ssl_policy",
		m: func(b *InMemoryBackend) map[string]*computeapi.SslPolicy { return b.sslPolicies },
		meta: func(v *computeapi.SslPolicy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: prepSSLPolicy,
	}
}

func prepSSLCert(_ *InMemoryBackend, c lbCtx, in *computeapi.SslCertificate, _ map[string]any) *APIError {
	if in.Type == "" {
		in.Type = certSelfManaged
		if in.Managed != nil {
			in.Type = certManaged
		}
	}

	switch in.Type {
	case certManaged:
		return prepManagedCert(c, in)
	case certSelfManaged:
		return prepSelfManagedCert(in)
	}

	return errField("resource.type", in.Type, "Must be SELF_MANAGED or MANAGED.")
}

func prepManagedCert(c lbCtx, in *computeapi.SslCertificate) *APIError {
	if c.scope != scopeGlobal {
		return errField("resource.type", in.Type, "Managed certificates are global in this emulator.")
	}

	if in.Managed == nil || len(in.Managed.Domains) == 0 {
		return errRequired("resource.managed.domains")
	}

	if in.Certificate != "" || in.PrivateKey != "" || in.SelfManaged != nil {
		return errInvalid(
			"Invalid value for field 'resource': a MANAGED certificate takes no certificate or private key.",
		)
	}

	if len(in.Managed.Domains) > maxManagedSANs {
		return errField("resource.managed.domains", "", "At most 100 domains are allowed.")
	}

	status := map[string]string{}

	for _, d := range in.Managed.Domains {
		if len(d) > maxDomainLen || !domainRE.MatchString(d) {
			return errField("resource.managed.domains", d, "Must be a fully qualified domain name without wildcards.")
		}

		status[d] = certProvision
	}

	in.Managed.Status, in.Managed.DomainStatus = certProvision, status

	return nil
}

func prepSelfManagedCert(in *computeapi.SslCertificate) *APIError {
	cert, keyPEM := in.Certificate, in.PrivateKey
	if sm := in.SelfManaged; sm != nil {
		cert, keyPEM = cmpOr(cert, sm.Certificate), cmpOr(keyPEM, sm.PrivateKey)
	}

	if cert == "" {
		return errRequired("resource.certificate")
	}

	if keyPEM == "" {
		return errRequired("resource.privateKey")
	}

	pair, err := tls.X509KeyPair([]byte(cert), []byte(keyPEM))
	if err != nil {
		return errInvalid("Invalid value for field 'resource.certificate': the certificate or private key "+
			"is not valid PEM or they do not match: %v", err)
	}

	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return errField("resource.certificate", "", "The certificate cannot be parsed.")
	}

	in.ExpireTime = leaf.NotAfter.UTC().Format(timeLayout)
	in.SubjectAlternativeNames = slices.Clone(leaf.DNSNames)

	for _, ip := range leaf.IPAddresses {
		in.SubjectAlternativeNames = append(in.SubjectAlternativeNames, ip.String())
	}

	in.Certificate, in.PrivateKey, in.SelfManaged, in.Managed = cert, "", nil, nil

	return nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func featuresFor(profile string) []string {
	pick := func(keep func(string) bool) []string {
		var out []string

		for _, f := range sslFeatures {
			if keep(f) {
				out = append(out, f)
			}
		}

		return out
	}

	switch profile {
	case profModern:
		return pick(func(f string) bool { return strings.Contains(f, "_ECDHE_") })
	case profRestricted:
		return pick(func(f string) bool {
			return strings.Contains(f, "_ECDHE_") && (strings.Contains(f, "GCM") || strings.Contains(f, "CHACHA"))
		})
	case profFIPS:
		return pick(func(f string) bool { return strings.Contains(f, "_ECDHE_") && strings.Contains(f, "GCM") })
	}

	return slices.Clone(sslFeatures)
}

func prepSSLPolicy(_ *InMemoryBackend, c lbCtx, in *computeapi.SslPolicy, _ map[string]any) *APIError {
	if c.scope != scopeGlobal {
		return errInvalid("SSL policies are global in this emulator.")
	}

	if in.Profile == "" {
		in.Profile = profCompatible
	}

	if !slices.Contains([]string{profCompatible, profModern, profRestricted, profFIPS, profCustom}, in.Profile) {
		return errField("resource.profile", in.Profile, "")
	}

	if in.MinTlsVersion == "" {
		in.MinTlsVersion = tls10
	}

	if !slices.Contains([]string{tls10, "TLS_1_1", "TLS_1_2", tls13}, in.MinTlsVersion) {
		return errField("resource.minTlsVersion", in.MinTlsVersion, "")
	}

	if in.MinTlsVersion == tls13 && in.Profile != profRestricted {
		return errField("resource.minTlsVersion", in.MinTlsVersion, "TLS_1_3 requires the RESTRICTED profile.")
	}

	if in.Profile != profCustom {
		if len(in.CustomFeatures) > 0 {
			return errField("resource.customFeatures", "", "Only valid with the CUSTOM profile.")
		}

		in.EnabledFeatures = featuresFor(in.Profile)

		return nil
	}

	if len(in.CustomFeatures) == 0 {
		return errRequired("resource.customFeatures")
	}

	for _, f := range in.CustomFeatures {
		if !slices.Contains(sslFeatures, f) {
			return errField("resource.customFeatures", f, "Not an available feature.")
		}
	}

	in.EnabledFeatures = slices.Sorted(slices.Values(in.CustomFeatures))

	return nil
}
