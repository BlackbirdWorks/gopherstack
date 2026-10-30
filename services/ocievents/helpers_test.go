package ocievents_test

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/events"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ocievents "github.com/blackbirdworks/gopherstack/services/ocievents"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testTopic       = "ocid1.onstopic.oc1.iad.topic1"
	testStream      = "ocid1.stream.oc1.iad.stream1"
	testFunction    = "ocid1.fnfunc.oc1.iad.fn1"
	matchAll        = "{}"
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
	backend *ocievents.InMemoryBackend
	handler *ocievents.Handler
	clock   *fakeClock
	client  events.EventsClient
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

	be := ocievents.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)
	be.SetDispatcher(func(fn func()) { fn() })

	h := ocievents.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, ocievents.Region, kp.fprint, kp.privPEM, nil)
	e := &env{backend: be, handler: h, clock: clock, base: fmt.Sprintf("http://localhost:%d", h.Port)}

	var err error

	e.client, err = events.NewEventsClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	e.client.Host = e.base

	return e
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

func onsAction() events.CreateNotificationServiceActionDetails {
	return events.CreateNotificationServiceActionDetails{IsEnabled: new(true), TopicId: new(testTopic)}
}

func ossAction() events.CreateStreamingServiceActionDetails {
	return events.CreateStreamingServiceActionDetails{IsEnabled: new(true), StreamId: new(testStream)}
}

func faasAction() events.CreateFaaSActionDetails {
	return events.CreateFaaSActionDetails{IsEnabled: new(true), FunctionId: new(testFunction)}
}

func (e *env) createRule(t *testing.T, name, condition string, acts ...events.ActionDetails) events.CreateRuleResponse {
	t.Helper()

	resp, err := e.client.CreateRule(t.Context(), events.CreateRuleRequest{
		DisplayName: new(name), IsEnabled: new(true), Condition: new(condition), CompartmentId: new(testCompartment),
		Actions: &events.ActionDetailsList{Actions: acts}})
	require.NoError(t, err)

	return resp
}

// fakeFunctions records invocations and fails them on demand.
type fakeFunctions struct {
	err    error
	bodies []string
	mu     sync.Mutex
}

func (f *fakeFunctions) FunctionActive(string) bool { return true }

func (f *fakeFunctions) InvokeFunction(_ context.Context, _ string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.bodies = append(f.bodies, string(body))

	return f.err
}

func (f *fakeFunctions) invoked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.bodies...)
}
