package ociapigateway_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testSubnet      = "ocid1.subnet.oc1..subnet"
	testFunction    = "ocid1.fnfunc.oc1.iad.fn1"
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
	backend *ociapigateway.InMemoryBackend
	handler *ociapigateway.Handler
	clock   *fakeClock
	gw      apigateway.GatewayClient
	dep     apigateway.DeploymentClient
	api     apigateway.ApiGatewayClient
	wr      apigateway.WorkRequestsClient
	base    string
	port    int
}

type envOpts struct {
	enforce  bool
	register bool
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer l.Close()

	tcp, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return tcp.Port
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	be := ociapigateway.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	h := ociapigateway.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, ociapigateway.Region, kp.fprint, kp.privPEM, nil)
	e := &env{backend: be, handler: h, clock: clock, port: h.Port, base: fmt.Sprintf("http://localhost:%d", h.Port)}

	var err error

	e.gw, err = apigateway.NewGatewayClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.dep, err = apigateway.NewDeploymentClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.api, err = apigateway.NewApiGatewayClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.wr, err = apigateway.NewWorkRequestsClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.gw.Host, e.dep.Host, e.api.Host, e.wr.Host = e.base, e.base, e.base, e.base

	return e
}

func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, method, url, body string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out), resp.Header
}

func (e *env) createGateway(t *testing.T, name string) apigateway.CreateGatewayResponse {
	t.Helper()

	resp, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
		CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
		SubnetId: new(testSubnet), DisplayName: new(name),
	})
	require.NoError(t, err)

	return resp
}

// mkGateway creates a gateway and settles it to ACTIVE.
func (e *env) mkGateway(t *testing.T, name string) apigateway.Gateway {
	t.Helper()

	id := *e.createGateway(t, name).Id
	e.settle()

	resp, err := e.gw.GetGateway(t.Context(), apigateway.GetGatewayRequest{GatewayId: &id})
	require.NoError(t, err)
	require.Equal(t, apigateway.GatewayLifecycleStateActive, resp.LifecycleState)

	return resp.Gateway
}

func specFromJSON(t *testing.T, s string) *apigateway.ApiSpecification {
	t.Helper()

	var spec apigateway.ApiSpecification
	require.NoError(t, json.Unmarshal([]byte(s), &spec))

	return &spec
}

func (e *env) createDeployment(
	t *testing.T, gwID, prefix string, spec *apigateway.ApiSpecification,
) (apigateway.CreateDeploymentResponse, error) {
	t.Helper()

	return e.dep.CreateDeployment(t.Context(), apigateway.CreateDeploymentRequest{
		GatewayId: &gwID, CompartmentId: new(testCompartment), PathPrefix: &prefix, Specification: spec,
	})
}

// mkDeployment creates a deployment from specification JSON and settles it to ACTIVE.
func (e *env) mkDeployment(t *testing.T, gwID, prefix, spec string) string {
	t.Helper()

	resp, err := e.createDeployment(t, gwID, prefix, specFromJSON(t, spec))
	require.NoError(t, err)

	e.settle()

	return *resp.Id
}

type dataResp struct {
	header http.Header
	body   string
	status int
}

// call sends a data-plane request through the /_apigw/<gatewayId> path scheme.
func (e *env) call(t *testing.T, gwID, method, path string, headers map[string]string, body string) dataResp {
	t.Helper()

	return e.callURL(t, e.base+"/_apigw/"+gwID+path, "", method, headers, body)
}

func (e *env) callURL(t *testing.T, url, host, method string, headers map[string]string, body string) dataResp {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := testclient.New(t)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	resp, err := client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return dataResp{status: resp.StatusCode, body: string(out), header: resp.Header}
}

func stockSpec(status int, body string) string {
	return fmt.Sprintf(
		`{"routes":[{"path":"/hi","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":%d,"body":%q}}]}`,
		status,
		body,
	)
}

const time1s = time.Second
