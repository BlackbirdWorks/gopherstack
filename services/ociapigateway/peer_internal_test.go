package ociapigateway

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeerAllowed(t *testing.T) {
	t.Parallel()

	verdict := func(v Reachability) func(string, string, int) Reachability {
		return func(string, string, int) Reachability { return v }
	}

	tests := []struct {
		name   string
		remote string
		tgt    target
		want   bool
	}{
		{"public gateway, internet peer", "203.0.113.9:4000", target{}, true},
		{"private gateway, internet peer", "203.0.113.9:4000", target{private: true}, false},
		{"private gateway, loopback", "127.0.0.1:4000", target{private: true}, true},
		{"private gateway, rfc1918 peer", "10.0.4.7:4000", target{private: true}, true},
		{"private gateway, v4-mapped private peer", "[::ffff:10.0.4.7]:4000", target{private: true}, true},
		{"private gateway, unparsable peer", "garbage", target{private: true}, false},
		{"denied beats private range", "10.0.4.7:4000",
			target{private: true, vnic: "v", reach: verdict(ReachDenied)}, false},
		{"allowed beats public range", "203.0.113.9:4000",
			target{private: true, vnic: "v", reach: verdict(ReachAllowed)}, true},
		{"foreign vcn to public gateway", "10.0.4.7:4000", target{vnic: "v", reach: verdict(ReachForeign)}, true},
		{"foreign vcn to private gateway, public peer", "203.0.113.9:4000",
			target{private: true, vnic: "v", reach: verdict(ReachForeign)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, peerAllowed(&http.Request{RemoteAddr: tt.remote}, tt.tgt))
		})
	}
}
