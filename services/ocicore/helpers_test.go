package ocicore_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

const (
	testTenancy = "ocid1.tenancy.oc1..aaaaaaaagopherstackemulatedtenancy000000000000000000000000"
	testComp    = testTenancy
	testRegion  = "us-ashburn-1"
)

type keyPair struct {
	priv    *rsa.PrivateKey
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
		priv:    k,
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
	client  core.VirtualNetworkClient
	compute core.ComputeClient
	block   core.BlockstorageClient
	kp      keyPair
	base    string
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

func newEnv(t *testing.T, enforce bool) *env {
	t.Helper()

	be := oci.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	h := oci.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	e := &env{backend: be, handler: h, clock: clock, kp: kp, base: fmt.Sprintf("http://localhost:%d", h.Port)}
	e.client = e.clientFor(t, "ocid1.user.oc1..bootstrap", kp)
	prov := common.NewRawConfigurationProvider(
		testTenancy,
		"ocid1.user.oc1..bootstrap",
		testRegion,
		kp.fprint,
		kp.privPEM,
		nil,
	)

	cc, err := core.NewComputeClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	cc.Host = e.base
	e.compute = cc

	bc, err := core.NewBlockstorageClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	bc.Host = e.base
	e.block = bc

	return e
}

func (e *env) clientFor(t *testing.T, user string, kp keyPair) core.VirtualNetworkClient {
	t.Helper()

	prov := common.NewRawConfigurationProvider(testTenancy, user, testRegion, kp.fprint, kp.privPEM, nil)
	c, err := core.NewVirtualNetworkClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = e.base

	return c
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func (e *env) mkVcn(t *testing.T, cidr, name string) core.Vcn {
	t.Helper()

	r, err := e.client.CreateVcn(t.Context(), core.CreateVcnRequest{
		CompartmentId: new(testComp), CidrBlocks: []string{cidr}, DisplayName: new(name), DnsLabel: new("lab" + name)})
	require.NoError(t, err)

	return r.Vcn
}

func (e *env) mkSubnet(t *testing.T, vcn core.Vcn, cidr string) core.Subnet {
	t.Helper()

	r, err := e.client.CreateSubnet(t.Context(), core.CreateSubnetRequest{
		CompartmentId: new(testComp), VcnId: vcn.Id, CidrBlock: new(cidr)})
	require.NoError(t, err)

	return r.Subnet
}
