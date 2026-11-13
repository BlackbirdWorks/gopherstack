package ociloadbalancer

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestDialTLSBoundsHandshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit time.Duration
	}{
		{name: "silent backend", limit: 50 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)

			held := make(chan net.Conn, 1)

			go func() {
				c, aerr := ln.Accept()
				if aerr == nil {
					held <- c
				}
			}()

			t.Cleanup(func() {
				_ = ln.Close()

				select {
				case c := <-held:
					_ = c.Close()
				default:
				}
			})

			d := &dataPlane{dialer: &net.Dialer{}, dialLimit: tt.limit}
			base := &tls.Config{MinVersion: tls.VersionTLS12}
			guard, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			conn, err := d.dialTLS(guard, "tcp", ln.Addr().String(), base, &SSLConfig{})

			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Nil(t, conn)
			assert.NoError(t, guard.Err(), "the handshake was cut off by the dial timeout, not the caller context")
		})
	}
}
