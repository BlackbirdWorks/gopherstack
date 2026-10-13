package sns

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"syscall"
)

// AllowLinkLocalDeliveryEnv set to "true" lets HTTP/HTTPS delivery reach
// link-local and instance-metadata addresses, which are blocked by default.
const AllowLinkLocalDeliveryEnv = "GOPHERSTACK_SNS_ALLOW_LINK_LOCAL_DELIVERY"

const (
	maxDeliveryRedirects = 10
	envTrue              = "true"
)

var errBlockedDeliveryTarget = errors.New("sns: delivery target is a link-local or metadata address")

func isBlockedDeliveryAddr(addr netip.Addr) bool {
	addr = addr.Unmap().WithZone("")

	return netip.MustParsePrefix("169.254.0.0/16").Contains(addr) ||
		netip.MustParsePrefix("fe80::/10").Contains(addr) ||
		addr == netip.MustParseAddr("fd00:ec2::254")
}

func allowLinkLocalFromEnv() bool {
	return os.Getenv(AllowLinkLocalDeliveryEnv) == envTrue
}

// screenEndpointHost rejects an endpoint URL whose host is a blocked literal address.
func screenEndpointHost(u *url.URL) error {
	if addr, err := netip.ParseAddr(u.Hostname()); err == nil && isBlockedDeliveryAddr(addr) {
		return errBlockedDeliveryTarget
	}

	return nil
}

func validateDeliveryEndpointAddress(endpoint string, allowLinkLocal bool) error {
	if allowLinkLocal {
		return nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil //nolint:nilerr // syntax is validated separately
	}

	if screenEndpointHost(u) != nil {
		return fmt.Errorf(
			"%w: Invalid parameter: Endpoint must not be a link-local or instance metadata address",
			ErrInvalidParameter,
		)
	}

	return nil
}

// newDeliveryClient builds the default HTTP/HTTPS delivery client. Resolved
// dial addresses are screened, so hostnames and redirect hops are covered too.
func newDeliveryClient(allowLinkLocal bool) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:errcheck,forcetypeassert // stdlib type
	transport.Proxy = nil
	transport.DisableKeepAlives = true

	if !allowLinkLocal {
		dialer := &net.Dialer{
			Timeout: snsHTTPTimeout,
			Control: func(_, address string, _ syscall.RawConn) error {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return err
				}

				if addr, perr := netip.ParseAddr(host); perr == nil && isBlockedDeliveryAddr(addr) {
					return errBlockedDeliveryTarget
				}

				return nil
			},
		}
		transport.DialContext = dialer.DialContext
	}

	return &http.Client{
		Timeout:   snsHTTPTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxDeliveryRedirects {
				return http.ErrUseLastResponse
			}

			if req.URL.Scheme != protocolHTTP && req.URL.Scheme != protocolHTTPS {
				return http.ErrUseLastResponse
			}

			if !allowLinkLocal {
				return screenEndpointHost(req.URL)
			}

			return nil
		},
	}
}

// SetAllowLinkLocalDelivery permits (true) or blocks (false, the default unless
// GOPHERSTACK_SNS_ALLOW_LINK_LOCAL_DELIVERY=true) delivery and Subscribe to
// link-local and instance-metadata addresses. It replaces the delivery client.
func (b *InMemoryBackend) SetAllowLinkLocalDelivery(allow bool) {
	b.mu.Lock("SetAllowLinkLocalDelivery")
	defer b.mu.Unlock()

	b.allowLinkLocal.Store(allow)
	b.httpClient = newDeliveryClient(allow)
}

func (b *InMemoryBackend) screenEndpoint(protocol, endpoint string) error {
	if err := validateSubscribeEndpoint(protocol, endpoint); err != nil {
		return err
	}

	if protocol != protocolHTTP && protocol != protocolHTTPS {
		return nil
	}

	return validateDeliveryEndpointAddress(endpoint, b.allowLinkLocal.Load())
}
