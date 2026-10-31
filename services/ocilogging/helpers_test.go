package ocilogging_test

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
	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
	"github.com/oracle/oci-go-sdk/v65/loggingsearch"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	oci "github.com/blackbirdworks/gopherstack/services/ocilogging"
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
	mgmt    logging.LoggingManagementClient
	ingest  loggingingestion.LoggingClient
	search  loggingsearch.LogSearchClient
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

	e.mgmt, err = logging.NewLoggingManagementClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.mgmt.Host = e.base

	e.ingest, err = loggingingestion.NewLoggingClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.ingest.Host = e.base

	e.search, err = loggingsearch.NewLogSearchClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.search.Host = e.base

	return e
}

// settle advances past the CREATING/UPDATING/DELETING transition.
func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func (e *env) mkGroup(t *testing.T, name string) logging.LogGroup {
	t.Helper()

	resp, err := e.mgmt.CreateLogGroup(t.Context(), logging.CreateLogGroupRequest{
		CompartmentId: new(testCompartment), DisplayName: new(name),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)
	e.settle()

	list, err := e.mgmt.ListLogGroups(t.Context(), logging.ListLogGroupsRequest{
		CompartmentId: new(testCompartment), DisplayName: new(name),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	got, err := e.mgmt.GetLogGroup(t.Context(), logging.GetLogGroupRequest{LogGroupId: list.Items[0].Id})
	require.NoError(t, err)

	return got.LogGroup
}

func (e *env) mkLog(t *testing.T, g logging.LogGroup, name string, mod func(*logging.CreateLogDetails)) logging.Log {
	t.Helper()

	d := logging.CreateLogDetails{DisplayName: new(name), LogType: logging.CreateLogDetailsLogTypeCustom}
	if mod != nil {
		mod(&d)
	}

	_, err := e.mgmt.CreateLog(t.Context(), logging.CreateLogRequest{LogGroupId: g.Id, CreateLogDetails: d})
	require.NoError(t, err)
	e.settle()

	list, err := e.mgmt.ListLogs(t.Context(), logging.ListLogsRequest{LogGroupId: g.Id, DisplayName: new(name)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	got, err := e.mgmt.GetLog(t.Context(), logging.GetLogRequest{LogGroupId: g.Id, LogId: list.Items[0].Id})
	require.NoError(t, err)

	return got.Log
}

// put ingests one batch of entries with the given data strings, stamped at the fake clock.
func (e *env) put(t *testing.T, l logging.Log, offset time.Duration, data ...string) {
	t.Helper()

	now := common.SDKTime{Time: e.clock.Now().Add(offset)}
	entries := make([]loggingingestion.LogEntry, 0, len(data))

	for i, d := range data {
		entries = append(
			entries,
			loggingingestion.LogEntry{Data: new(d), Id: new(fmt.Sprintf("entry-%d-%d", offset, i))},
		)
	}

	_, err := e.ingest.PutLogs(t.Context(), loggingingestion.PutLogsRequest{
		LogId:       l.Id,
		Specversion: new("1.0"),
		LogEntryBatches: []loggingingestion.LogEntryBatch{{
			Entries: entries, Source: new("app"), Type: new("app.requests"), Defaultlogentrytime: &now,
		}},
	})
	require.NoError(t, err)
}

func (e *env) query(t *testing.T, q string, limit *int, page *string) loggingsearch.SearchLogsResponse {
	t.Helper()

	start := common.SDKTime{Time: e.clock.Now().Add(-48 * time.Hour)}
	end := common.SDKTime{Time: e.clock.Now().Add(48 * time.Hour)}

	resp, err := e.search.SearchLogs(t.Context(), loggingsearch.SearchLogsRequest{
		TimeStart: &start, TimeEnd: &end, SearchQuery: new(q), IsReturnFieldInfo: new(true),
		Limit: limit, Page: page,
	})
	require.NoError(t, err)

	return resp
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
