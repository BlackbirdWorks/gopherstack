package ociloadbalancer

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackendTLSForSkipsOnlyLoopback(t *testing.T) {
	t.Parallel()

	yes, no := true, false

	tests := []struct {
		name     string
		verify   *bool
		host     string
		wantSkip bool
	}{
		{"false loopback v4", &no, "127.0.0.1", true},
		{"false loopback v6", &no, "::1", true},
		{"unset loopback", nil, "127.0.0.1", true},
		{"true loopback", &yes, "127.0.0.1", false},
		{"false private", &no, "10.0.0.5", false},
		{"false public", &no, "198.18.0.7", false},
		{"false hostname", &no, "localhost", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := &tls.Config{MinVersion: tls.VersionTLS12}
			cfg := backendTLSFor(base, &SSLConfig{VerifyPeerCertificate: tt.verify}, tt.host)

			assert.Equal(t, tt.wantSkip, cfg.InsecureSkipVerify)
			assert.Equal(t, tt.host, cfg.ServerName)
			assert.False(t, base.InsecureSkipVerify)
		})
	}
}
