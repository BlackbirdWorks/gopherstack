package dns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func TestIsSelfAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		upstream string
		self     string
		want     bool
	}{
		{name: "exact", upstream: ":53", self: ":53", want: true},
		{name: "loopback vs wildcard", upstream: "127.0.0.1:53", self: ":53", want: true},
		{name: "v6 loopback vs wildcard", upstream: "[::1]:53", self: ":53", want: true},
		{name: "unspecified vs loopback", upstream: "0.0.0.0:53", self: "127.0.0.1:53", want: true},
		{name: "same host", upstream: "10.0.0.5:5353", self: "10.0.0.5:5353", want: true},
		{name: "different port", upstream: "127.0.0.1:5353", self: ":53"},
		{name: "external", upstream: "8.8.8.8:53", self: ":53"},
		{name: "external vs specific self", upstream: "8.8.8.8:53", self: "10.0.0.5:53"},
		{name: "loopback vs specific self", upstream: "127.0.0.1:53", self: "10.0.0.5:53"},
		{name: "unparseable", upstream: "bogus", self: ":53"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dns.IsSelfAddr(tt.upstream, tt.self))
		})
	}
}
