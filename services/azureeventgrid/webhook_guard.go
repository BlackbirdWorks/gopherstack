package azureeventgrid

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"syscall"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

var (
	errWebhookURL    = errors.New("webhook endpoint must be an http(s) URL without credentials")
	errWebhookTarget = errors.New("webhook endpoint resolves to a blocked address")
)

// parseWebhookURL accepts only http(s) URLs with a host and no userinfo.
func parseWebhookURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return nil, errWebhookURL
	}

	return u, nil
}

// blockedWebhookAddr reports cloud-metadata, link-local, multicast and unspecified addresses; loopback and
// private ranges stay reachable because subscribers are local services.
func blockedWebhookAddr(a netip.Addr) bool {
	a = a.Unmap()
	ec2IPv6Metadata := netip.MustParsePrefix("fd00:ec2::/32")

	return a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() || a.IsMulticast() || a.IsUnspecified() ||
		ec2IPv6Metadata.Contains(a)
}

// guardDial rejects the connection after DNS resolution, so rebinding and redirects cannot reach a blocked address.
func guardDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errWebhookTarget
	}

	a, err := netip.ParseAddr(host)
	if err != nil || blockedWebhookAddr(a) {
		return errWebhookTarget
	}

	return nil
}

// newWebhookClient returns a pooled client whose dialer refuses blocked addresses.
func newWebhookClient(timeout time.Duration) *http.Client {
	tr := httpclient.NewTransport()
	d := &net.Dialer{Timeout: timeout, KeepAlive: timeout, Control: guardDial}
	tr.DialContext = d.DialContext

	return &http.Client{Timeout: timeout, Transport: tr}
}
