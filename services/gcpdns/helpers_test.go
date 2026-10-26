package gcpdns_test

import (
	"net"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/services/gcpdns"
)

const project = "test-project"

type env struct {
	svc     *dnsapi.Service
	backend *gcpdns.InMemoryBackend
	url     string
}

func newEnv(t *testing.T) *env {
	t.Helper()

	backend := gcpdns.NewInMemoryBackend()
	e := echo.New()
	e.Any("/*", gcpdns.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	svc, err := dnsapi.NewService(t.Context(), option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)

	return &env{svc: svc, backend: backend, url: srv.URL}
}

func (e *env) zone(t *testing.T, name, dnsName string) *dnsapi.ManagedZone {
	t.Helper()

	z, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: name, DnsName: dnsName, Description: "test zone",
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	return z
}

func (e *env) addRRSet(t *testing.T, zone string, rr *dnsapi.ResourceRecordSet) {
	t.Helper()

	_, err := e.svc.ResourceRecordSets.Create(project, zone, rr).Context(t.Context()).Do()
	require.NoError(t, err)
}

func apiErr(t *testing.T, err error) *googleapi.Error {
	t.Helper()

	var ge *googleapi.Error

	require.ErrorAs(t, err, &ge)

	return ge
}

func assertAPIError(t *testing.T, err error, status int, reason string) {
	t.Helper()

	require.Error(t, err)

	ge := apiErr(t, err)
	assert.Equal(t, status, ge.Code)

	if reason != "" {
		require.NotEmpty(t, ge.Errors)
		assert.Equal(t, reason, ge.Errors[0].Reason)
	}
}

func rrset(name, typ string, ttl int64, data ...string) *dnsapi.ResourceRecordSet {
	return &dnsapi.ResourceRecordSet{Name: name, Type: typ, Ttl: ttl, Rrdatas: data}
}

func freePort(t *testing.T) int {
	t.Helper()

	for range 50 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()

		pc, err := net.ListenPacket("udp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			continue
		}

		_ = pc.Close()

		return port
	}

	t.Fatal("no free tcp+udp port")

	return 0
}
