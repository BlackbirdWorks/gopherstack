package elbv2

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	dataPlanePrefix       = "/_gopherstack/elbv2/"
	listenersEndpointPath = dataPlanePrefix + "_listeners"
	dataPlaneHostMarker   = ".elb."
	dataPlaneLocalSuffix  = ".elb.localhost"
	dataPlaneAWSSuffix    = ".elb.amazonaws.com"
	listenerPortHeader    = "X-Gopherstack-Listener-Port"
	defaultIdleTimeout    = 60 * time.Second
	idleTimeoutAttr       = "idle_timeout.timeout_seconds"
	defaultListenerPort   = 80
	operationDataPlane    = "DataPlaneRequest"
	msgNoTargets          = "503 Service Temporarily Unavailable"
	msgBadGateway         = "502 Bad Gateway"
	msgNotImplemented     = "501 Not Implemented"
)

// LambdaInvoker invokes a Lambda function synchronously for lambda target groups.
type LambdaInvoker interface {
	// InvokeFunction returns the response payload and the function error type (empty on success).
	InvokeFunction(ctx context.Context, functionARN string, payload []byte) ([]byte, string, error)
}

// InstanceAddressResolver reports the reachable private IP of a docker-launched EC2 instance.
type InstanceAddressResolver interface {
	InstanceAddress(instanceID string) (string, bool)
}

type dataPlane struct {
	invoker  atomic.Pointer[LambdaInvoker]
	instance atomic.Pointer[InstanceAddressResolver]
	client   *http.Client
	rr       sync.Map
}

func newDataPlane() *dataPlane {
	return &dataPlane{client: newProxyClient()}
}

// SetLambdaInvoker wires the invoker used for lambda target groups.
func (h *Handler) SetLambdaInvoker(inv LambdaInvoker) { h.dp.invoker.Store(&inv) }

// SetInstanceAddressResolver wires instance-target address resolution.
func (h *Handler) SetInstanceAddressResolver(r InstanceAddressResolver) { h.dp.instance.Store(&r) }

func isDataPlanePath(p string) bool { return strings.HasPrefix(p, dataPlanePrefix) }

func hostWithoutPort(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}

	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func canonicalLBHost(host string) string {
	if base, ok := strings.CutSuffix(host, dataPlaneLocalSuffix); ok {
		return base + dataPlaneAWSSuffix
	}

	return host
}

func (h *Handler) isDataPlaneRequest(r *http.Request) bool {
	if isDataPlanePath(r.URL.Path) {
		return true
	}

	host := hostWithoutPort(r.Host)
	if !strings.Contains(host, dataPlaneHostMarker) {
		return false
	}

	lb, _ := h.findLBByDNS(canonicalLBHost(host))

	return lb != nil
}

func (h *Handler) allBackends() []StorageBackend {
	if h.peers == nil {
		return []StorageBackend{h.Backend}
	}

	return h.RegionBackends()
}

func (h *Handler) findLBByDNS(dns string) (*LoadBalancer, StorageBackend) {
	for _, be := range h.allBackends() {
		lbs, err := be.DescribeLoadBalancers(nil, nil)
		if err != nil {
			continue
		}

		for i := range lbs {
			if strings.EqualFold(lbs[i].DNSName, dns) {
				return &lbs[i], be
			}
		}
	}

	return nil, nil
}

func (h *Handler) findLBByName(name string) (*LoadBalancer, StorageBackend) {
	for _, be := range h.allBackends() {
		lbs, err := be.DescribeLoadBalancers(nil, []string{name})
		if err == nil && len(lbs) > 0 {
			return &lbs[0], be
		}
	}

	return nil, nil
}

// dpRequest is one inbound data-plane request after load-balancer and listener resolution.
type dpRequest struct {
	client   netip.Addr
	be       StorageBackend
	r        *http.Request
	lb       *LoadBalancer
	listener *Listener
	path     string
	rawPath  string
	host     string
	// clientPort is the TCP peer port of the client, 0 when unknown.
	clientPort uint16
}

func (h *Handler) serveDataPlane(c *echo.Context) error {
	h.handleDataPlane(c.Response(), c.Request())

	return nil
}

func (h *Handler) handleDataPlane(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == listenersEndpointPath {
		h.serveListenerEndpoints(w)

		return
	}

	lb, be, path, rawPath := h.resolveLB(r)
	if lb == nil {
		writeDPError(w, http.StatusNotFound, "404 Not Found: no such load balancer")

		return
	}

	if lb.Type != lbTypeApplication {
		writeDPError(w, http.StatusServiceUnavailable, "data plane serves application load balancers only")

		return
	}

	listeners, err := be.DescribeListeners(lb.LoadBalancerArn, nil)
	if err != nil {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	l := pickListener(listeners, requestedPort(r))
	if l == nil {
		writeDPError(w, http.StatusServiceUnavailable, "no HTTP listener on this load balancer")

		return
	}

	req := &dpRequest{
		r: r, be: be, lb: lb, listener: l, path: path, rawPath: rawPath,
		host: hostWithoutPort(r.Host),
	}
	req.client, req.clientPort = clientAddrPort(r)
	h.route(w, req)
}

func (h *Handler) resolveLB(r *http.Request) (*LoadBalancer, StorageBackend, string, string) {
	if rest, ok := strings.CutPrefix(r.URL.EscapedPath(), dataPlanePrefix); ok {
		name, tail, _ := strings.Cut(rest, "/")
		raw := "/" + tail
		lb, be := h.findLBByName(name)

		decoded, err := url.PathUnescape(raw)
		if err != nil {
			decoded = raw
		}

		return lb, be, decoded, raw
	}

	lb, be := h.findLBByDNS(canonicalLBHost(hostWithoutPort(r.Host)))

	raw := r.URL.EscapedPath()
	if raw == "" {
		raw = "/"
	}

	return lb, be, r.URL.Path, raw
}

func requestedPort(r *http.Request) int32 {
	if v := r.Header.Get(listenerPortHeader); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(n)
		}
	}

	if _, p, err := net.SplitHostPort(r.Host); err == nil {
		if n, perr := strconv.ParseInt(p, 10, 32); perr == nil {
			return int32(n)
		}
	}

	return 0
}

func pickListener(ls []Listener, want int32) *Listener {
	web := make([]*Listener, 0, len(ls))

	for i := range ls {
		if ls[i].Protocol == protoHTTP || ls[i].Protocol == protoHTTPS {
			web = append(web, &ls[i])
		}
	}

	if len(web) == 0 {
		return nil
	}

	sort.Slice(web, func(i, j int) bool {
		if web[i].Protocol != web[j].Protocol {
			return web[i].Protocol == protoHTTP
		}

		return web[i].Port < web[j].Port
	})

	for _, l := range web {
		if want != 0 && l.Port == want {
			return l
		}
	}

	for _, l := range web {
		if l.Port == defaultListenerPort {
			return l
		}
	}

	return web[0]
}

func clientAddr(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	a, _ := netip.ParseAddr(host)

	return a.Unmap()
}

func writeDPError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(status)
	_, _ = w.Write([]byte("<html><head><title>" + msg + "</title></head><body><center><h1>" + msg +
		"</h1></center></body></html>"))
}

// serveListenerEndpoints lists real-listener host endpoints across regions (the port mapping).
func (h *Handler) serveListenerEndpoints(w http.ResponseWriter) {
	out := []ListenerEndpoint{}

	for _, be := range h.allBackends() {
		if ib, ok := be.(*InMemoryBackend); ok {
			out = append(out, ib.ListenerEndpoints()...)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ListenerArn < out[j].ListenerArn })

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
