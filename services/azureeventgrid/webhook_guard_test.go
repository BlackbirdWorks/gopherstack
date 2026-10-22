package azureeventgrid_test

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

func TestParseWebhookURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "http", raw: "http://127.0.0.1:8080/hook"},
		{name: "https", raw: "https://example.com/hook"},
		{name: "ftp", raw: "ftp://example.com/hook", wantErr: true},
		{name: "userinfo", raw: "http://user:pw@example.com/", wantErr: true},
		{name: "no host", raw: "http:///hook", wantErr: true},
		{name: "relative", raw: "/hook", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := azureeventgrid.ParseWebhookURL(tt.raw)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestBlockedWebhookAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr string
		want bool
	}{
		{addr: "169.254.169.254", want: true},
		{addr: "::ffff:169.254.169.254", want: true},
		{addr: "fe80::1", want: true},
		{addr: "fd00:ec2::254", want: true},
		{addr: "0.0.0.0", want: true},
		{addr: "224.0.0.1", want: true},
		{addr: "127.0.0.1"},
		{addr: "10.1.2.3"},
		{addr: "93.184.216.34"},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, azureeventgrid.BlockedWebhookAddr(netip.MustParseAddr(tt.addr)))
		})
	}
}

func TestWebhookClientDial(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := azureeventgrid.NewWebhookClient(time.Second)
	t.Cleanup(c.CloseIdleConnections)

	resp, err := c.Get(srv.URL)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	_, err = c.Get("http://169.254.169.254/latest/meta-data")
	require.ErrorIs(t, err, azureeventgrid.WebhookTargetErr())
}
