package ocidatabase_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/oracle/oci-go-sdk/v65/workrequests"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	svc "github.com/blackbirdworks/gopherstack/services/ocidatabase"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testPassword    = "Passw0rd_Adb12"
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
	backend *svc.InMemoryBackend
	handler *svc.Handler
	clock   *fakeClock
	client  database.DatabaseClient
	wrs     workrequests.WorkRequestClient
	kp      keyPair
	base    string
}

type envOpts struct {
	configure func(*svc.InMemoryBackend)
	enforce   bool
	register  bool
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	be := svc.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	if opts.configure != nil {
		opts.configure(be)
	}

	h := svc.NewHandler(be)
	h.Port = 0
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	e := &env{backend: be, handler: h, clock: clock, kp: kp, base: fmt.Sprintf("http://localhost:%d", h.Port)}
	e.client, e.wrs = e.newClients(t, kp)

	return e
}

func (e *env) newClients(t *testing.T, kp keyPair) (database.DatabaseClient, workrequests.WorkRequestClient) {
	t.Helper()

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, svc.Region, kp.fprint, kp.privPEM, nil)

	c, err := database.NewDatabaseClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = e.base

	w, err := workrequests.NewWorkRequestClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	w.Host = e.base

	return c, w
}

// settle completes pending work requests and lifecycle transitions.
func (e *env) settle() {
	e.clock.Advance(5 * time.Second)
	e.backend.Sweep()
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}

func rawGet(t *testing.T, url string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func baseDetails() database.CreateAutonomousDatabaseDetails {
	return database.CreateAutonomousDatabaseDetails{
		CompartmentId: new(testCompartment),
		DbName:        new("mydb"),
		AdminPassword: new(testPassword),
		DisplayName:   new("My Database"),
	}
}

type createOpts struct {
	mutate func(*database.CreateAutonomousDatabaseDetails)
	token  string
}

func (e *env) create(t *testing.T, o createOpts) (database.CreateAutonomousDatabaseResponse, error) {
	t.Helper()

	d := baseDetails()
	if o.mutate != nil {
		o.mutate(&d)
	}

	req := database.CreateAutonomousDatabaseRequest{CreateAutonomousDatabaseDetails: d}
	if o.token != "" {
		req.OpcRetryToken = new(o.token)
	}

	return e.client.CreateAutonomousDatabase(t.Context(), req)
}

func (e *env) mustCreate(t *testing.T, o createOpts) database.CreateAutonomousDatabaseResponse {
	t.Helper()

	resp, err := e.create(t, o)
	require.NoError(t, err)

	return resp
}

// available creates a database and settles it to AVAILABLE.
func (e *env) available(t *testing.T, o createOpts) database.AutonomousDatabase {
	t.Helper()

	resp := e.mustCreate(t, o)
	e.settle()

	return e.get(t, resp.Id)
}

func (e *env) get(t *testing.T, id *string) database.AutonomousDatabase {
	t.Helper()

	resp, err := e.client.GetAutonomousDatabase(
		t.Context(),
		database.GetAutonomousDatabaseRequest{AutonomousDatabaseId: id},
	)
	require.NoError(t, err)

	return resp.AutonomousDatabase
}

func (e *env) wr(t *testing.T, id *string) workrequests.WorkRequest {
	t.Helper()

	resp, err := e.wrs.GetWorkRequest(t.Context(), workrequests.GetWorkRequestRequest{WorkRequestId: id})
	require.NoError(t, err)

	return resp.WorkRequest
}

func (e *env) ctx() context.Context { return context.Background() }

func (e *env) manualBackup(t *testing.T, dbID *string) database.AutonomousDatabaseBackup {
	t.Helper()

	resp, err := e.client.CreateAutonomousDatabaseBackup(t.Context(), database.CreateAutonomousDatabaseBackupRequest{
		AutonomousDatabaseId: dbID, DisplayName: new("manual"),
	})
	require.NoError(t, err)

	e.settle()

	return resp.AutonomousDatabaseBackup
}

func workrequestsErrorsRequest(id *string) workrequests.ListWorkRequestErrorsRequest {
	return workrequests.ListWorkRequestErrorsRequest{WorkRequestId: id}
}
