package ocimonitoring_test

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
	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	oci "github.com/blackbirdworks/gopherstack/services/ocimonitoring"
)

// epoch is the fixed test start, aligned to five minutes.
func epoch() time.Time { return time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC) }

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
	c.ns.Store(epoch().UnixNano())

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
	mon     monitoring.MonitoringClient
	base    string
}

type envOpts struct {
	enforce  bool
	register bool
	// realClock starts the fake clock at the wall time, which signature verification needs.
	realClock bool
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
	if opts.realClock {
		clock.ns.Store(time.Now().UnixNano())
	}

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

	e.mon, err = monitoring.NewMonitoringClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.mon.Host = e.base

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

// post sends one metric stream with the given (offset from now, value) points.
func (e *env) post(t *testing.T, ns, name string, dims map[string]string, pts ...pt) monitoring.PostMetricDataResponse {
	t.Helper()

	resp, err := e.mon.PostMetricData(t.Context(), monitoring.PostMetricDataRequest{
		MetricData: []monitoring.MetricDataDetails{e.metric(ns, name, dims, pts...)},
	})
	require.NoError(t, err)

	return resp
}

type pt struct {
	off   time.Duration
	value float64
	count int
}

func (e *env) metric(ns, name string, dims map[string]string, pts ...pt) monitoring.MetricDataDetails {
	dps := make([]monitoring.Datapoint, 0, len(pts))

	for _, p := range pts {
		dp := monitoring.Datapoint{Timestamp: &common.SDKTime{Time: e.clock.Now().Add(p.off)}, Value: new(p.value)}
		if p.count > 0 {
			dp.Count = new(p.count)
		}

		dps = append(dps, dp)
	}

	return monitoring.MetricDataDetails{
		Namespace: new(ns), CompartmentId: new(testCompartment), Name: new(name), Dimensions: dims, Datapoints: dps,
	}
}
