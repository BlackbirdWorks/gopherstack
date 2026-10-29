package ocinosql_test

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
	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ocinosql "github.com/blackbirdworks/gopherstack/services/ocinosql"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
)

func usersDDL(name string) string {
	return "CREATE TABLE " + name + " (id INTEGER, name STRING, age INTEGER, active BOOLEAN, info JSON, " +
		"PRIMARY KEY(id))"
}

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
	backend *ocinosql.InMemoryBackend
	handler *ocinosql.Handler
	clock   *fakeClock
	prov    common.ConfigurationProvider
	kp      keyPair
	client  nosql.NosqlClient
	base    string
	port    int
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

	be := ocinosql.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	h := ocinosql.NewHandler(be)
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
		backend: be, handler: h, clock: clock, kp: kp, port: h.Port,
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, ocinosql.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.client, err = nosql.NewNosqlClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.client.Host = e.base

	return e
}

// settle advances past the CREATING/UPDATING/DELETING transition.
func (e *env) settle() { e.clock.Advance(5 * time.Second) }

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawDo(t *testing.T, method, url, body string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out), resp.Header
}

func provisioned(r, w, s int) *nosql.TableLimits {
	return &nosql.TableLimits{
		MaxReadUnits: &r, MaxWriteUnits: &w, MaxStorageInGBs: &s,
		CapacityMode: nosql.TableLimitsCapacityModeProvisioned,
	}
}

func (e *env) createTable(t *testing.T, name, ddl string, limits *nosql.TableLimits) nosql.CreateTableResponse {
	t.Helper()

	if limits == nil {
		limits = provisioned(50, 50, 10)
	}

	resp, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
		Name: new(name), CompartmentId: new(testCompartment), DdlStatement: new(ddl), TableLimits: limits,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)

	return resp
}

// mkTable creates a table and settles it to ACTIVE.
func (e *env) mkTable(t *testing.T, name, ddl string) string {
	t.Helper()

	e.createTable(t, name, ddl, nil)
	e.settle()

	got := e.getTable(t, name)
	require.Equal(t, nosql.TableLifecycleStateActive, got.LifecycleState)

	return *got.Id
}

func (e *env) getTable(t *testing.T, ref string) nosql.Table {
	t.Helper()

	resp, err := e.client.GetTable(t.Context(), nosql.GetTableRequest{
		TableNameOrId: new(ref), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)

	return resp.Table
}

func (e *env) put(t *testing.T, table string, value map[string]any) nosql.UpdateRowResult {
	t.Helper()

	resp, err := e.client.UpdateRow(t.Context(), nosql.UpdateRowRequest{
		TableNameOrId: new(table),
		Value:         value, CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)

	return resp.UpdateRowResult
}

func (e *env) query(t *testing.T, stmt string, limit int, page *string) nosql.QueryResponse {
	t.Helper()

	req := nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new(stmt),
		Page: page,
	}
	if limit > 0 {
		req.Limit = new(limit)
	}

	resp, err := e.client.Query(t.Context(), req)
	require.NoError(t, err)

	return resp
}

func (e *env) workRequest(t *testing.T, id *string) nosql.WorkRequest {
	t.Helper()

	resp, err := e.client.GetWorkRequest(t.Context(), nosql.GetWorkRequestRequest{WorkRequestId: id})
	require.NoError(t, err)

	return resp.WorkRequest
}
