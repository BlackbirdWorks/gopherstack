package ocifunctions_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
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
	c.ns.Store(time.Now().UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *oci.InMemoryBackend
	handler *oci.Handler
	clock   *fakeClock
	prov    common.ConfigurationProvider
	kp      keyPair
	mgmt    functions.FunctionsManagementClient
	wrc     functions.WorkRequestManagementClient
	base    string
}

type envOpts struct {
	rt       oci.Runner
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

	be := oci.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	if opts.rt != nil {
		be.SetRuntime(func() (oci.Runner, error) { return opts.rt, nil })
	}

	h := oci.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	e := &env{
		backend: be, handler: h, clock: clock, kp: kp,
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, oci.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.mgmt, err = functions.NewFunctionsManagementClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.mgmt.Host = e.base

	e.wrc, err = functions.NewWorkRequestManagementClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.wrc.Host = e.base

	return e
}

func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func (e *env) mkApp(t *testing.T, name string, mod func(*functions.CreateApplicationDetails)) functions.Application {
	t.Helper()

	d := functions.CreateApplicationDetails{
		CompartmentId: new(testCompartment), DisplayName: new(name), SubnetIds: []string{testSubnet},
	}
	if mod != nil {
		mod(&d)
	}

	resp, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{CreateApplicationDetails: d})
	require.NoError(t, err)
	e.settle()

	return resp.Application
}

func (e *env) mkFn(
	t *testing.T,
	app functions.Application,
	name string,
	mod func(*functions.CreateFunctionDetails),
) functions.Function {
	t.Helper()

	d := functions.CreateFunctionDetails{
		DisplayName: new(name), ApplicationId: app.Id, MemoryInMBs: new(int64(256)),
		SourceDetails: functions.CreateContainerImageFunctionSourceDetails{Image: new("example/fn:1")},
	}
	if mod != nil {
		mod(&d)
	}

	resp, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{CreateFunctionDetails: d})
	require.NoError(t, err)
	e.settle()

	got, err := e.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: resp.Id})
	require.NoError(t, err)

	return got.Function
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, method, url, body string, hdr map[string]string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out), resp.Header
}
