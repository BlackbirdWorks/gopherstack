package ociidentity_test

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
	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const testTenancy = oci.TenancyID

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
	client  identity.IdentityClient
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

// newEnv starts an Identity listener; the client signs with a throwaway key as a made-up user.
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

	return e
}

func (e *env) clientFor(t *testing.T, user string, kp keyPair) identity.IdentityClient {
	t.Helper()

	prov := common.NewRawConfigurationProvider(testTenancy, user, oci.Region, kp.fprint, kp.privPEM, nil)
	c, err := identity.NewIdentityClientWithConfigurationProvider(prov)
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

func (e *env) mkUser(t *testing.T, name string) identity.User {
	t.Helper()

	r, err := e.client.CreateUser(t.Context(), identity.CreateUserRequest{
		CompartmentId: new(testTenancy), Name: new(name), Description: new("test user " + name)})
	require.NoError(t, err)

	return r.User
}

func (e *env) mkGroup(t *testing.T, name string) identity.Group {
	t.Helper()

	r, err := e.client.CreateGroup(
		t.Context(),
		identity.CreateGroupRequest{
			CompartmentId: new(testTenancy), Name: new(name), Description: new("test group " + name)},
	)
	require.NoError(t, err)

	return r.Group
}

func (e *env) mkCompartment(t *testing.T, parent, name string) identity.Compartment {
	t.Helper()

	r, err := e.client.CreateCompartment(t.Context(), identity.CreateCompartmentRequest{
		CompartmentId: new(parent), Name: new(name), Description: new("compartment " + name),
	})
	require.NoError(t, err)

	return r.Compartment
}

func mustDate(t *testing.T, s string) time.Time {
	t.Helper()

	d, err := time.Parse("2006-01-02", s)
	require.NoError(t, err)

	return d
}
