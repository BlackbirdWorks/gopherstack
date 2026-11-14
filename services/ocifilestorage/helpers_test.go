package ocifilestorage_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	svc "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testAD          = "Uocm:IAD-AD-1"
	otherAD         = "Uocm:IAD-AD-2"
	testSubnet      = "ocid1.subnet.oc1.iad.sub1"
	testVCN         = "ocid1.vcn.oc1.iad.vcn1"
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

// fakeNet stands in for OCI Core: one subnet, sequential addresses, recorded VNIC deletions.
type fakeNet struct {
	deleted []string
	next    int
	mu      sync.Mutex
}

func (n *fakeNet) deletedVnics() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	return append([]string(nil), n.deleted...)
}

func (n *fakeNet) network() svc.Network {
	return svc.Network{
		Subnet: func(id string) (svc.SubnetInfo, bool) {
			if id != testSubnet {
				return svc.SubnetInfo{}, false
			}

			return svc.SubnetInfo{VcnID: testVCN, CIDR: "10.0.1.0/24"}, true
		},
		CreateNic: func(r svc.NicRequest) (svc.Nic, error) {
			n.mu.Lock()
			defer n.mu.Unlock()

			n.next++

			ip := r.IPAddress
			if ip == "" {
				ip = fmt.Sprintf("10.0.1.%d", n.next+3)
			}

			return svc.Nic{
				VnicID: fmt.Sprintf("ocid1.vnic.oc1.iad.v%d", n.next), IPAddress: ip,
				PrivateIPID: fmt.Sprintf("ocid1.privateip.oc1.iad.p%d", n.next),
			}, nil
		},
		DeleteNic: func(id string) {
			n.mu.Lock()
			n.deleted = append(n.deleted, id)
			n.mu.Unlock()
		},
	}
}

type env struct {
	backend *svc.InMemoryBackend
	handler *svc.Handler
	clock   *fakeClock
	net     *fakeNet
	client  filestorage.FileStorageClient
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
	fn := &fakeNet{}
	be.SetClock(clock.Now)
	be.SetNetwork(fn.network())
	be.SetCompartmentChecker(func(id string) bool { return id == testCompartment || id == otherCompart })

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

	e := &env{backend: be, handler: h, clock: clock, net: fn, kp: kp, base: fmt.Sprintf("http://localhost:%d", h.Port)}
	prov := common.NewRawConfigurationProvider(testTenancy, testUser, svc.Region, kp.fprint, kp.privPEM, nil)

	c, err := filestorage.NewFileStorageClientWithConfigurationProvider(prov)
	require.NoError(t, err)

	c.Host = e.base
	e.client = c

	return e
}

// settle completes pending lifecycle transitions.
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

func assertStatus(t *testing.T, err error, status int, code string) {
	t.Helper()

	se := svcErr(t, err)
	require.Equal(t, status, se.GetHTTPStatusCode(), se.GetMessage())

	if code != "" {
		require.Equal(t, code, se.GetCode())
	}
}

func (e *env) createFS(t *testing.T, mutate func(*filestorage.CreateFileSystemDetails)) filestorage.FileSystem {
	t.Helper()

	d := filestorage.CreateFileSystemDetails{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), DisplayName: new("fs"),
	}
	if mutate != nil {
		mutate(&d)
	}

	resp, err := e.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{CreateFileSystemDetails: d})
	require.NoError(t, err)
	e.settle()

	return e.getFS(t, resp.Id)
}

func (e *env) getFS(t *testing.T, id *string) filestorage.FileSystem {
	t.Helper()

	resp, err := e.client.GetFileSystem(t.Context(), filestorage.GetFileSystemRequest{FileSystemId: id})
	require.NoError(t, err)

	return resp.FileSystem
}

func (e *env) createMT(t *testing.T, mutate func(*filestorage.CreateMountTargetDetails)) filestorage.MountTarget {
	t.Helper()

	d := filestorage.CreateMountTargetDetails{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), SubnetId: new(testSubnet),
		DisplayName: new("mt"),
	}
	if mutate != nil {
		mutate(&d)
	}

	resp, err := e.client.CreateMountTarget(
		t.Context(),
		filestorage.CreateMountTargetRequest{CreateMountTargetDetails: d},
	)
	require.NoError(t, err)
	e.settle()

	return e.getMT(t, resp.Id)
}

func (e *env) getMT(t *testing.T, id *string) filestorage.MountTarget {
	t.Helper()

	resp, err := e.client.GetMountTarget(t.Context(), filestorage.GetMountTargetRequest{MountTargetId: id})
	require.NoError(t, err)

	return resp.MountTarget
}

func (e *env) createExport(
	t *testing.T,
	set, fs *string,
	path string,
	opts []filestorage.ClientOptions,
) filestorage.Export {
	t.Helper()

	resp, err := e.client.CreateExport(t.Context(), filestorage.CreateExportRequest{
		ExportSetId: set, FileSystemId: fs, Path: new(path), ExportOptions: opts,
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.client.GetExport(t.Context(), filestorage.GetExportRequest{ExportId: resp.Id})
	require.NoError(t, err)

	return got.Export
}

func (e *env) createSnapshot(t *testing.T, fs *string, name string) filestorage.Snapshot {
	t.Helper()

	resp, err := e.client.CreateSnapshot(t.Context(), filestorage.CreateSnapshotRequest{
		FileSystemId: fs, Name: new(name),
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.client.GetSnapshot(t.Context(), filestorage.GetSnapshotRequest{SnapshotId: resp.Id})
	require.NoError(t, err)

	return got.Snapshot
}
