package ociloadbalancer

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyCiphersSkipsInsecureSuites(t *testing.T) {
	t.Parallel()

	lb := &LoadBalancer{SSLCipherSuites: map[string]*SSLCipherSuite{
		"s": {Name: "s", Ciphers: []string{
			"ECDHE-RSA-AES128-SHA256", "ECDHE-ECDSA-AES128-SHA256", "ECDHE-RSA-AES128-GCM-SHA256",
		}},
	}}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	applyCiphers(cfg, lb, "s")

	assert.Equal(t, []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}, cfg.CipherSuites)
}
