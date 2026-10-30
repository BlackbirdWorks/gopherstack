package ocimysql_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
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
	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ocimysql "github.com/blackbirdworks/gopherstack/services/ocimysql"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	subA            = "ocid1.subnet.oc1.iad.suba"
	subB            = "ocid1.subnet.oc1.iad.subb"
	shapeSmall      = "MySQL.2"
	shapeBig        = "MySQL.4"
	adminUser       = "dbadmin"
	adminPass       = "Passw0rd!x"
	testTable       = "orders"
)

type fakeNet struct{}

func (fakeNet) Subnet(id string) (ocimysql.SubnetInfo, bool) {
	switch id {
	case subA:
		return ocimysql.SubnetInfo{
			ID:                 subA,
			VcnID:              "vcn",
			AvailabilityDomain: "Uocm:US-ASHBURN-AD-2",
			CIDR:               "10.0.1.0/24",
		}, true
	case subB:
		return ocimysql.SubnetInfo{ID: subB, VcnID: "vcn", CIDR: "10.0.2.0/29"}, true
	}

	return ocimysql.SubnetInfo{}, false
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

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	if start.IsZero() {
		start = time.Now()
	}

	c.ns.Store(start.UTC().Truncate(time.Second).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *ocimysql.InMemoryBackend
	handler *ocimysql.Handler
	clock   *fakeClock
	prov    common.ConfigurationProvider
	kp      keyPair
	db      my.DbSystemClient
	bk      my.DbBackupsClient
	ch      my.ChannelsClient
	aas     my.MysqlaasClient
	wr      my.WorkRequestsClient
	base    string
	port    int
	wait    time.Duration
}

type envOpts struct {
	start    time.Time
	launcher *fakeLauncher
	enforce  bool
	register bool
	noNet    bool
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

	be := ocimysql.NewInMemoryBackend()
	clock := newFakeClock(opts.start)
	be.SetClock(clock.Now)

	if !opts.noNet {
		be.SetNetwork(fakeNet{})
	}

	if opts.launcher != nil {
		be.SetLauncher(opts.launcher, "")
	}

	h := ocimysql.NewHandler(be)
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
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, ocimysql.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.db, err = my.NewDbSystemClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.bk, err = my.NewDbBackupsClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.ch, err = my.NewChannelsClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.aas, err = my.NewMysqlaasClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.wr, err = my.NewWorkRequestsClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.db.Host, e.bk.Host, e.ch.Host, e.aas.Host, e.wr.Host = e.base, e.base, e.base, e.base, e.base

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

func dbDetails(name string) my.CreateDbSystemDetails {
	return my.CreateDbSystemDetails{
		CompartmentId: new(testCompartment), ShapeName: new(shapeSmall), SubnetId: new(subA),
		DisplayName: new(name), AdminUsername: new(adminUser), AdminPassword: new(adminPass),
	}
}

func (e *env) createDB(t *testing.T, d my.CreateDbSystemDetails) my.CreateDbSystemResponse {
	t.Helper()

	resp, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)

	return resp
}

// mkDB creates a DB system and settles it to ACTIVE on the clock.
func (e *env) mkDB(t *testing.T, name string, mod func(*my.CreateDbSystemDetails)) my.DbSystem {
	t.Helper()

	d := dbDetails(name)
	if mod != nil {
		mod(&d)
	}

	resp := e.createDB(t, d)
	e.settle()

	return e.getDB(t, resp.Id)
}

func (e *env) getDB(t *testing.T, id *string) my.DbSystem {
	t.Helper()

	got, err := e.db.GetDbSystem(t.Context(), my.GetDbSystemRequest{DbSystemId: id})
	require.NoError(t, err)

	return got.DbSystem
}

// awaitState waits for the background engine job to move the DB system into state.
func (e *env) awaitState(t *testing.T, id *string, state my.DbSystemLifecycleStateEnum) my.DbSystem {
	t.Helper()

	var got my.DbSystem

	require.Eventually(t, func() bool {
		r, err := e.db.GetDbSystem(t.Context(), my.GetDbSystemRequest{DbSystemId: id})
		if err != nil {
			return false
		}

		got = r.DbSystem

		return got.LifecycleState == state
	}, e.waitFor(), 20*time.Millisecond)

	return got
}

func (e *env) awaitBackup(t *testing.T, id *string, state my.BackupLifecycleStateEnum) my.Backup {
	t.Helper()

	var got my.Backup

	require.Eventually(t, func() bool {
		r, err := e.bk.GetBackup(t.Context(), my.GetBackupRequest{BackupId: id})
		if err != nil {
			return false
		}

		got = r.Backup

		return got.LifecycleState == state
	}, e.waitFor(), 20*time.Millisecond)

	return got
}

// fakeLauncher stands in for the docker engine: a server keeps a set of table names that Dump and Restore carry.
var errEmptyDump = errors.New("empty dump")

type fakeLauncher struct {
	failLaunch error
	insts      map[string]*fakeInst
	specs      []ocimysql.LaunchSpec
	removed    []string
	mu         sync.Mutex
}

type fakeInst struct {
	admins  map[string]string
	tables  []string
	port    int
	stopped bool
	closed  bool
	purged  bool
	starts  int
}

func newFakeLauncher() *fakeLauncher { return &fakeLauncher{insts: map[string]*fakeInst{}} }

func (l *fakeLauncher) Launch(_ context.Context, spec ocimysql.LaunchSpec) (ocimysql.Instance, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.specs = append(l.specs, spec)

	if l.failLaunch != nil {
		return nil, l.failLaunch
	}

	in := &fakeInst{admins: map[string]string{}, port: 40000 + len(l.specs)}
	l.insts[spec.ID] = in

	return &fakeHandle{l: l, in: in}, nil
}

func (l *fakeLauncher) RemoveVolume(_ context.Context, id string) {
	l.mu.Lock()
	l.removed = append(l.removed, id)
	l.mu.Unlock()
}

func (l *fakeLauncher) inst(id string) *fakeInst {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.insts[id]
}

type fakeHandle struct {
	l  *fakeLauncher
	in *fakeInst
}

func (h *fakeHandle) Endpoint() (string, int) { return "127.0.0.1", h.in.port }

func (h *fakeHandle) EnsureAdmin(_ context.Context, user, pass string) error {
	h.l.mu.Lock()
	h.in.admins[user] = pass
	h.l.mu.Unlock()

	return nil
}

func (h *fakeHandle) DropUser(_ context.Context, user string) error {
	h.l.mu.Lock()
	delete(h.in.admins, user)
	h.l.mu.Unlock()

	return nil
}

func (h *fakeHandle) Stop(context.Context) error {
	h.l.mu.Lock()
	h.in.stopped = true
	h.l.mu.Unlock()

	return nil
}

func (h *fakeHandle) Start(context.Context) error {
	h.l.mu.Lock()
	h.in.stopped = false
	h.in.starts++
	h.l.mu.Unlock()

	return nil
}

func (h *fakeHandle) Dump(context.Context) ([]byte, error) {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()

	return []byte(strings.Join(h.in.tables, ",")), nil
}

func (h *fakeHandle) Restore(_ context.Context, data []byte) error {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()

	if len(data) == 0 {
		return errEmptyDump
	}

	h.in.tables = strings.Split(string(data), ",")

	return nil
}

func (h *fakeHandle) Close(_ context.Context, purge bool) {
	h.l.mu.Lock()
	h.in.closed, h.in.purged = true, purge
	h.l.mu.Unlock()
}

func (l *fakeLauncher) addTable(id, name string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.insts[id].tables = append(l.insts[id].tables, name)
}

func (e *env) ctx() context.Context { return context.Background() }

func (e *env) waitFor() time.Duration {
	if e.wait > 0 {
		return e.wait
	}

	return 30 * time.Second
}
