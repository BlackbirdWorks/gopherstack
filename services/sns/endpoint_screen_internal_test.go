package sns

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliveryClient_BlocksLinkLocalTargets(t *testing.T) {
	t.Parallel()

	redirector := func(target string) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target, http.StatusFound)
		}))
		t.Cleanup(srv.Close)

		return srv
	}

	tests := []struct {
		url     func() string
		name    string
		allow   bool
		wantErr bool
	}{
		{name: "direct metadata", url: func() string { return "http://169.254.169.254:81/x" }, wantErr: true},
		{name: "direct v6 link local", url: func() string { return "http://[fe80::1]:81/x" }, wantErr: true},
		{
			name:    "redirect to metadata",
			url:     func() string { return redirector("http://169.254.169.254:81/x").URL },
			wantErr: true,
		},
		{name: "redirect to ftp", url: func() string { return redirector("ftp://127.0.0.1/x").URL }},
		{
			name: "loopback ok",
			url: func() string {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(srv.Close)

				return srv.URL
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newDeliveryClient(tc.allow)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tc.url(), nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			if resp != nil {
				_ = resp.Body.Close()
			}

			if tc.wantErr {
				require.ErrorIs(t, err, errBlockedDeliveryTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestIsBlockedDeliveryAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr string
		want bool
	}{
		{addr: "169.254.169.254", want: true},
		{addr: "169.254.170.2", want: true},
		{addr: "fe80::1", want: true},
		{addr: "fd00:ec2::254", want: true},
		{addr: "::ffff:169.254.169.254", want: true},
		{addr: "127.0.0.1"},
		{addr: "10.0.0.1"},
		{addr: "172.16.0.1"},
		{addr: "::1"},
		{addr: "fd00:ec2::253"},
	}

	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			t.Parallel()

			a, err := netip.ParseAddr(tc.addr)
			require.NoError(t, err)
			assert.Equal(t, tc.want, isBlockedDeliveryAddr(a))
		})
	}
}
