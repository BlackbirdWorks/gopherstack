package ocistreaming_test

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
	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
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
	admin   streaming.StreamAdminClient
	base    string
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
		backend: be, handler: h, clock: clock, kp: kp,
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, oci.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.admin, err = streaming.NewStreamAdminClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.admin.Host = e.base

	return e
}

func (e *env) msgs(t *testing.T, s streaming.Stream) streaming.StreamClient {
	t.Helper()

	c, err := streaming.NewStreamClientWithConfigurationProvider(e.prov, *s.MessagesEndpoint)
	require.NoError(t, err)

	return c
}

// settle advances past the CREATING/DELETING transition.
func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func (e *env) mkStream(t *testing.T, name string, partitions int) streaming.Stream {
	t.Helper()

	resp, err := e.admin.CreateStream(t.Context(), streaming.CreateStreamRequest{
		Name: new(name), Partitions: new(partitions), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.admin.GetStream(t.Context(), streaming.GetStreamRequest{StreamId: resp.Id})
	require.NoError(t, err)

	return got.Stream
}

func (e *env) mkPool(t *testing.T, name string, mod func(*streaming.CreateStreamPoolDetails)) streaming.StreamPool {
	t.Helper()

	d := streaming.CreateStreamPoolDetails{CompartmentId: new(testCompartment), Name: new(name)}
	if mod != nil {
		mod(&d)
	}

	resp, err := e.admin.CreateStreamPool(t.Context(), streaming.CreateStreamPoolRequest{CreateStreamPoolDetails: d})
	require.NoError(t, err)
	e.settle()

	return resp.StreamPool
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, method, url, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func putOne(
	t *testing.T,
	c streaming.StreamClient,
	s streaming.Stream,
	key, value string,
) streaming.PutMessagesResultEntry {
	t.Helper()

	var k []byte
	if key != "" {
		k = []byte(key)
	}

	resp, err := c.PutMessages(t.Context(), streaming.PutMessagesRequest{
		StreamId: s.Id,
		Messages: []streaming.PutMessagesDetailsEntry{{Key: k, Value: []byte(value)}},
	})
	require.NoError(t, err)
	require.Len(t, resp.Entries, 1)

	return resp.Entries[0]
}

func plainCursor(
	t *testing.T, c streaming.StreamClient, s streaming.Stream, part string,
	typ streaming.CreateCursorDetailsTypeEnum, mod func(*streaming.CreateCursorDetails),
) string {
	t.Helper()

	d := streaming.CreateCursorDetails{Partition: new(part), Type: typ}
	if mod != nil {
		mod(&d)
	}

	resp, err := c.CreateCursor(t.Context(), streaming.CreateCursorRequest{StreamId: s.Id, CreateCursorDetails: d})
	require.NoError(t, err)

	return *resp.Value
}
