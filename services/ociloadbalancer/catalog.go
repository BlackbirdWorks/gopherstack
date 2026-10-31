package ociloadbalancer

import (
	"crypto/tls"
	"slices"
	"strconv"
)

const (
	protoHTTP  = "HTTP"
	protoHTTP2 = "HTTP2"
	protoTCP   = "TCP"

	policyRoundRobin = "ROUND_ROBIN"
	policyLeastConns = "LEAST_CONNECTIONS"
	policyIPHash     = "IP_HASH"

	shapeFlexible = "flexible"
)

func itoa(n int) string { return strconv.Itoa(n) }

//nolint:gochecknoglobals // immutable reference data
var (
	policyNames   = []string{policyRoundRobin, policyLeastConns, policyIPHash}
	protocolNames = []string{protoHTTP, protoHTTP2, protoTCP}
	shapeNames    = []string{"10Mbps", "10Mbps-Micro", "100Mbps", "400Mbps", "8000Mbps", shapeFlexible}

	builtinSuites = []string{
		"oci-default-ssl-cipher-suite-v1", "oci-modern-ssl-cipher-suite-v1", "oci-compatible-ssl-cipher-suite-v1",
		"oci-wider-compatible-ssl-cipher-suite-v1", "oci-customized-ssl-cipher-suite",
		"oci-default-http2-ssl-cipher-suite-v1", "oci-default-http2-tls-13-ssl-cipher-suite-v1",
		"oci-default-http2-tls-12-13-ssl-cipher-suite-v1", "oci-tls-13-recommended-ssl-cipher-suite-v1",
	}

	cipherIDs = map[string]uint16{
		"ECDHE-RSA-AES128-GCM-SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		"ECDHE-RSA-AES256-GCM-SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		"ECDHE-ECDSA-AES128-GCM-SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"ECDHE-ECDSA-AES256-GCM-SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		"ECDHE-RSA-CHACHA20-POLY1305":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
		"ECDHE-ECDSA-CHACHA20-POLY1305": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		"ECDHE-RSA-AES128-SHA":          tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		"ECDHE-RSA-AES256-SHA":          tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		"ECDHE-ECDSA-AES128-SHA":        tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		"ECDHE-ECDSA-AES256-SHA":        tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		"AES128-GCM-SHA256":             tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		"AES256-GCM-SHA384":             tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		"AES128-SHA":                    tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		"AES256-SHA":                    tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		"ECDHE-RSA-AES128-SHA256":       tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
		"ECDHE-ECDSA-AES128-SHA256":     tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	}
)

func isBuiltinSuite(name string) bool {
	return slices.Contains(builtinSuites, name)
}
