package ocicontainerinstances_test

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	ci "github.com/oracle/oci-go-sdk/v65/containerinstances"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	svc "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testSubnet      = "ocid1.subnet.oc1..subnet"
	testAD          = "Uocm:US-ASHBURN-AD-1"
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
	backend *svc.InMemoryBackend
	handler *svc.Handler
	clock   *fakeClock
	client  ci.ContainerInstanceClient
	kp      keyPair
	base    string
}

type envOpts struct {
	configure func(*svc.InMemoryBackend)
	enforce   bool
	register  bool
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

	be := svc.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	if opts.configure != nil {
		opts.configure(be)
	}

	h := svc.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	e := &env{backend: be, handler: h, clock: clock, kp: kp, base: fmt.Sprintf("http://localhost:%d", h.Port)}
	e.client = e.newClient(t, kp)

	return e
}

func (e *env) newClient(t *testing.T, kp keyPair) ci.ContainerInstanceClient {
	t.Helper()

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, svc.Region, kp.fprint, kp.privPEM, nil)

	c, err := ci.NewContainerInstanceClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = e.base

	return c
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

func rawGet(t *testing.T, url string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func f32(v float32) *float32 { return &v }

type createOpts struct {
	mutate func(*ci.CreateContainerInstanceDetails)
	token  string
}

func baseDetails() ci.CreateContainerInstanceDetails {
	return ci.CreateContainerInstanceDetails{
		CompartmentId:      common.String(testCompartment),
		AvailabilityDomain: common.String(testAD),
		Shape:              common.String("CI.Standard.E4.Flex"),
		DisplayName:        common.String("web"),
		ShapeConfig:        &ci.CreateContainerInstanceShapeConfigDetails{Ocpus: f32(2), MemoryInGBs: f32(8)},
		Containers:         []ci.CreateContainerDetails{{ImageUrl: common.String("docker.io/library/alpine:3.21")}},
		Vnics:              []ci.CreateContainerVnicDetails{{SubnetId: common.String(testSubnet)}},
	}
}

func (e *env) create(t *testing.T, o createOpts) (ci.CreateContainerInstanceResponse, error) {
	t.Helper()

	d := baseDetails()
	if o.mutate != nil {
		o.mutate(&d)
	}

	req := ci.CreateContainerInstanceRequest{CreateContainerInstanceDetails: d}
	if o.token != "" {
		req.OpcRetryToken = common.String(o.token)
	}

	return e.client.CreateContainerInstance(t.Context(), req)
}

func (e *env) mustCreate(t *testing.T, o createOpts) ci.CreateContainerInstanceResponse {
	t.Helper()

	resp, err := e.create(t, o)
	require.NoError(t, err)

	return resp
}

func (e *env) get(t *testing.T, id *string) ci.ContainerInstance {
	t.Helper()

	resp, err := e.client.GetContainerInstance(t.Context(), ci.GetContainerInstanceRequest{ContainerInstanceId: id})
	require.NoError(t, err)

	return resp.ContainerInstance
}

func (e *env) wr(t *testing.T, id *string) ci.WorkRequest {
	t.Helper()

	resp, err := e.client.GetWorkRequest(t.Context(), ci.GetWorkRequestRequest{WorkRequestId: id})
	require.NoError(t, err)

	return resp.WorkRequest
}
