package ocinotifications_test

import (
	"net/http/httptest"
	"net/url"

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
	"github.com/oracle/oci-go-sdk/v65/ons"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ocinotifications"
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
	ctl     ons.NotificationControlPlaneClient
	dp      ons.NotificationDataPlaneClient
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

	e.ctl, err = ons.NewNotificationControlPlaneClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.ctl.Host = e.base

	e.dp, err = ons.NewNotificationDataPlaneClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.dp.Host = e.base

	return e
}

// settle advances past the CREATING/DELETING transition.
func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func (e *env) mkTopic(t *testing.T, name string) ons.NotificationTopic {
	t.Helper()

	resp, err := e.ctl.CreateTopic(t.Context(), ons.CreateTopicRequest{
		Name: new(name), CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	e.settle()

	got, err := e.ctl.GetTopic(t.Context(), ons.GetTopicRequest{TopicId: resp.TopicId})
	require.NoError(t, err)
	require.Equal(t, ons.NotificationTopicLifecycleStateActive, got.LifecycleState)

	return got.NotificationTopic
}

func (e *env) subscribe(t *testing.T, topic ons.NotificationTopic, protocol, endpoint string) ons.Subscription {
	t.Helper()

	resp, err := e.dp.CreateSubscription(t.Context(), ons.CreateSubscriptionRequest{
		TopicId:       topic.TopicId,
		CompartmentId: new(testCompartment),
		Protocol:      new(protocol),
		Endpoint:      new(endpoint),
	})
	require.NoError(t, err)

	return resp.Subscription
}

func (e *env) publish(t *testing.T, topic ons.NotificationTopic, body string) ons.PublishResult {
	t.Helper()

	resp, err := e.dp.PublishMessage(t.Context(), ons.PublishMessageRequest{
		TopicId: topic.TopicId, Body: new(body),
	})
	require.NoError(t, err)

	return resp.PublishResult
}

func (e *env) deliveries(t *testing.T, subID string) []oci.DeliveryView {
	t.Helper()

	return e.backend.Deliveries("", subID)
}

func (e *env) confirm(t *testing.T, sub ons.Subscription, confirmationURL string) ons.ConfirmationResult {
	t.Helper()

	u, err := url.Parse(confirmationURL)
	require.NoError(t, err)

	resp, err := e.dp.GetConfirmSubscription(t.Context(), ons.GetConfirmSubscriptionRequest{
		Id: sub.Id, Token: new(u.Query().Get("token")), Protocol: sub.Protocol,
	})
	require.NoError(t, err)

	return resp.ConfirmationResult
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, method, rawURL, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, rawURL, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

type received struct {
	header http.Header
	body   string
}

// sink is an HTTP endpoint that records every request and answers with the next scripted status.
type sink struct {
	srv      *httptest.Server
	requests chan received
	statuses chan int
}

func newSink(t *testing.T, statuses ...int) *sink {
	t.Helper()

	s := &sink{requests: make(chan received, 64), statuses: make(chan int, len(statuses)+1)}
	for _, st := range statuses {
		s.statuses <- st
	}

	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.requests <- received{header: r.Header.Clone(), body: string(b)}

		status := http.StatusOK

		select {
		case st := <-s.statuses:
			status = st
		default:
		}

		w.WriteHeader(status)
	}))
	t.Cleanup(s.srv.Close)

	return s
}

func (s *sink) next(t *testing.T) received {
	t.Helper()

	select {
	case r := <-s.requests:
		return r
	case <-t.Context().Done():
		t.Fatal("no request received")

		return received{}
	}
}
