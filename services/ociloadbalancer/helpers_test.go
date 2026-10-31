package ociloadbalancer_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ociloadbalancer "github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testSubnet      = "ocid1.subnet.oc1..subnet"
)

type keyPair struct {
	privPEM string
	pubPEM  string
	fprint  string
}

func newKeyPair(t *testing.T) keyPair {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pub, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)

	fp, err := ociauth.Fingerprint(&k.PublicKey)
	require.NoError(t, err)

	return keyPair{
		privPEM: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})),
		pubPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})),
		fprint:  fp,
	}
}

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Now().UTC().Truncate(time.Second).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *ociloadbalancer.InMemoryBackend
	handler *ociloadbalancer.Handler
	clock   *fakeClock
	lb      loadbalancer.LoadBalancerClient
	base    string
	port    int
}

type envOpts struct {
	enforce  bool
	register bool
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer l.Close()

	tcp, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return tcp.Port
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	be := ociloadbalancer.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	h := ociloadbalancer.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	prov := common.NewRawConfigurationProvider(
		testTenancy,
		testUser,
		ociloadbalancer.Region,
		kp.fprint,
		kp.privPEM,
		nil,
	)
	e := &env{backend: be, handler: h, clock: clock, port: h.Port, base: fmt.Sprintf("http://localhost:%d", h.Port)}

	var err error

	e.lb, err = loadbalancer.NewLoadBalancerClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.lb.Host = e.base

	return e
}

// settle completes pending work requests and lifecycle transitions.
func (e *env) settle() {
	e.clock.Advance(5 * time.Second)
	e.backend.Sweep()
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, url string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

// testBackend is a loopback HTTP backend that identifies itself and can be made unhealthy.
type testBackend struct {
	srv     *httptest.Server
	name    string
	healthy atomic.Bool
}

func (b *testBackend) ip() string {
	host, _, _ := net.SplitHostPort(b.srv.Listener.Addr().String())

	return host
}

func (b *testBackend) port() int {
	_, p, _ := net.SplitHostPort(b.srv.Listener.Addr().String())

	var n int

	_, _ = fmt.Sscanf(p, "%d", &n)

	return n
}

func (b *testBackend) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if !b.healthy.Load() {
				w.WriteHeader(http.StatusInternalServerError)
			}

			return
		}

		w.Header().Set("X-Backend", b.name)
		w.Header().Set("Echo-X-Added", r.Header.Get("X-Added"))
		w.Header().Set("Echo-Host", r.Host)
		w.Header().Set("Echo-Xff", r.Header.Get("X-Forwarded-For"))
		w.Header().Set("Set-Cookie", "sid=abc; Path=/")
		_, _ = io.WriteString(w, b.name+" "+r.URL.Path)
	})
}

func startBackend(t *testing.T, name string) *testBackend {
	t.Helper()

	b := &testBackend{name: name}
	b.healthy.Store(true)
	b.srv = httptest.NewServer(b.handler())
	t.Cleanup(b.srv.Close)

	return b
}

func startTLSBackend(t *testing.T, name string, cert tls.Certificate) *testBackend {
	t.Helper()

	b := &testBackend{name: name}
	b.healthy.Store(true)
	b.srv = httptest.NewUnstartedServer(b.handler())
	b.srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	b.srv.StartTLS()
	t.Cleanup(b.srv.Close)

	return b
}

func (b *testBackend) detail() loadbalancer.BackendDetails {
	return loadbalancer.BackendDetails{IpAddress: new(b.ip()), Port: new(b.port())}
}

// hc is a fast health checker: one failed check marks a backend critical.
func hc() *loadbalancer.HealthCheckerDetails {
	return &loadbalancer.HealthCheckerDetails{
		Protocol: new(
			"HTTP",
		),
		UrlPath:          new("/health"),
		Retries:          new(0),
		IntervalInMillis: new(1000),
		TimeoutInMillis:  new(1000),
	}
}

func backendSet(policy string, bs ...*testBackend) loadbalancer.BackendSetDetails {
	d := loadbalancer.BackendSetDetails{Policy: new(policy), HealthChecker: hc()}
	for _, b := range bs {
		d.Backends = append(d.Backends, b.detail())
	}

	return d
}

func baseDetails(name string) loadbalancer.CreateLoadBalancerDetails {
	return loadbalancer.CreateLoadBalancerDetails{
		CompartmentId: new(testCompartment), DisplayName: new(name), ShapeName: new("flexible"),
		ShapeDetails: &loadbalancer.ShapeDetails{MinimumBandwidthInMbps: new(10), MaximumBandwidthInMbps: new(100)},
		SubnetIds:    []string{testSubnet},
	}
}

func (e *env) create(t *testing.T, d loadbalancer.CreateLoadBalancerDetails) (string, string) {
	t.Helper()

	resp, err := e.lb.CreateLoadBalancer(
		t.Context(),
		loadbalancer.CreateLoadBalancerRequest{CreateLoadBalancerDetails: d},
	)
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)

	wr, err := e.lb.GetWorkRequest(
		t.Context(),
		loadbalancer.GetWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	require.NoError(t, err)

	return *wr.LoadBalancerId, *resp.OpcWorkRequestId
}

// mkLB creates a load balancer and settles it to ACTIVE.
func (e *env) mkLB(t *testing.T, d loadbalancer.CreateLoadBalancerDetails) string {
	t.Helper()

	id, _ := e.create(t, d)
	e.settle()

	return id
}

func (e *env) get(t *testing.T, id string) loadbalancer.GetLoadBalancerResponse {
	t.Helper()

	resp, err := e.lb.GetLoadBalancer(t.Context(), loadbalancer.GetLoadBalancerRequest{LoadBalancerId: &id})
	require.NoError(t, err)

	return resp
}

func (e *env) addr(t *testing.T, lbID string, port int) string {
	t.Helper()

	for _, ep := range e.backend.Endpoints() {
		if ep.LoadBalancerID == lbID && ep.Port == port {
			return ep.Address
		}
	}

	require.FailNow(t, "listener not serving", "lb %s port %d", lbID, port)

	return ""
}

type dataResp struct {
	header http.Header
	body   string
	status int
}

// call sends a request to a load balancer listener; host overrides the Host header.
func (e *env) call(t *testing.T, lbID string, port int, host, path string, headers map[string]string) dataResp {
	t.Helper()

	return e.do(t, "http://"+e.addr(t, lbID, port)+path, host, http.MethodGet, headers, nil)
}

func (e *env) do(t *testing.T, url, host, method string, headers map[string]string, client *http.Client) dataResp {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, http.NoBody)
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	if client == nil {
		client = testclient.New(t)
	}

	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	resp, err := client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return dataResp{status: resp.StatusCode, body: string(out), header: resp.Header}
}

func tlsClient(t *testing.T, certPEM []byte) *http.Client {
	t.Helper()

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(certPEM))

	tr := httpclient.NewTransport()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c := &http.Client{Transport: tr}
	t.Cleanup(c.CloseIdleConnections)

	return c
}

func genCert(t *testing.T) ([]byte, []byte) {
	t.Helper()

	c, k, err := devtls.GenerateSelfSignedCertPEM("localhost")
	require.NoError(t, err)

	return c, k
}
