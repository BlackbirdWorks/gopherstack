package ociobjectstorage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
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
	client  objectstorage.ObjectStorageClient
	backend *oci.InMemoryBackend
	handler *oci.Handler
	clock   *fakeClock
	keys    keyPair
	baseURL string
	ns      string
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

	be := oci.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

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
		backend: be,
		handler: h,
		clock:   clock,
		keys:    kp,
		ns:      be.Namespace(),
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", h.Port),
	}
	e.client = e.clientFor(t, kp)

	return e
}

func (e *env) clientFor(t *testing.T, kp keyPair) objectstorage.ObjectStorageClient {
	t.Helper()

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, oci.Region, kp.fprint, kp.privPEM, nil)
	c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = e.baseURL

	return c
}

func (e *env) mkBucket(t *testing.T, name string, mut func(*objectstorage.CreateBucketDetails)) {
	t.Helper()

	d := objectstorage.CreateBucketDetails{Name: new(name), CompartmentId: new(testCompartment)}
	if mut != nil {
		mut(&d)
	}

	_, err := e.client.CreateBucket(t.Context(), objectstorage.CreateBucketRequest{
		NamespaceName: new(e.ns), CreateBucketDetails: d,
	})
	require.NoError(t, err)
}

func (e *env) put(t *testing.T, bucket, name, body string) objectstorage.PutObjectResponse {
	t.Helper()

	resp, err := e.client.PutObject(t.Context(), objectstorage.PutObjectRequest{
		NamespaceName: new(e.ns), BucketName: new(bucket), ObjectName: new(name),
		ContentLength: new(int64(len(body))), PutObjectBody: io.NopCloser(strings.NewReader(body)),
	})
	require.NoError(t, err)

	return resp
}

func (e *env) get(t *testing.T, bucket, name string) string {
	t.Helper()

	resp, err := e.client.GetObject(t.Context(), objectstorage.GetObjectRequest{
		NamespaceName: new(e.ns), BucketName: new(bucket), ObjectName: new(name),
	})
	require.NoError(t, err)

	defer resp.Content.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, resp.Content)
	require.NoError(t, err)

	return buf.String()
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}
