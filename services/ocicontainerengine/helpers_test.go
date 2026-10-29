package ocicontainerengine_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
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
	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oke "github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"

	vcnA        = "ocid1.vcn.oc1.iad.vcna"
	vcnB        = "ocid1.vcn.oc1.iad.vcnb"
	subAD1      = "ocid1.subnet.oc1.iad.subad1"
	subAD2      = "ocid1.subnet.oc1.iad.subad2"
	subRegional = "ocid1.subnet.oc1.iad.subregional"
	subLB       = "ocid1.subnet.oc1.iad.sublb"
	subOther    = "ocid1.subnet.oc1.iad.subother"
	ad1         = "Uocm:US-ASHBURN-AD-1"
	ad2         = "Uocm:US-ASHBURN-AD-2"
	imgOL8      = "ocid1.image.oc1.iad.ol8"
	imgOL8Arm   = "ocid1.image.oc1.iad.ol8arm"
	imgUbuntu   = "ocid1.image.oc1.iad.ubuntu"
	shapeFlex   = "VM.Standard.E4.Flex"
	shapeFixed  = "VM.Standard2.1"
	shapeArm    = "VM.Standard.A1.Flex"
	testVersion = "v1.34.2"
	keyOK       = "ocid1.key.oc1.iad.keyok"
	keyOff      = "ocid1.key.oc1.iad.keyoff"
)

type fakeNet struct{}

func fakeSubnets() map[string]oke.SubnetInfo {
	return map[string]oke.SubnetInfo{
		subAD1:      {ID: subAD1, VcnID: vcnA, AvailabilityDomain: ad1, CIDR: "10.0.1.0/24"},
		subAD2:      {ID: subAD2, VcnID: vcnA, AvailabilityDomain: ad2, CIDR: "10.0.2.0/24"},
		subRegional: {ID: subRegional, VcnID: vcnA, CIDR: "10.0.3.0/24"},
		subLB:       {ID: subLB, VcnID: vcnA, CIDR: "10.0.4.0/24"},
		subOther:    {ID: subOther, VcnID: vcnB, CIDR: "10.1.0.0/24"},
	}
}

func fakeShapes() []oke.ShapeInfo {
	return []oke.ShapeInfo{
		{
			Name: shapeFlex, Arch: "x86_64", Flexible: true, MinOcpus: 1, MaxOcpus: 64, MaxMemoryGBs: 1024,
			DefaultMemPerOcpu: 16,
		},
		{Name: shapeFixed, Arch: "x86_64"},
		{
			Name: shapeArm, Arch: "aarch64", Flexible: true, MinOcpus: 1, MaxOcpus: 76, MaxMemoryGBs: 472,
			DefaultMemPerOcpu: 6,
		},
	}
}

func fakeImages() []oke.ImageInfo {
	return []oke.ImageInfo{
		{ID: imgOL8, Name: "Oracle-Linux-8.10-2024.09.30-0", OS: "Oracle Linux", OSVersion: "8", Arch: "x86_64"},
		{
			ID: imgOL8Arm, Name: "Oracle-Linux-8.10-aarch64-2024.09.30-0", OS: "Oracle Linux", OSVersion: "8",
			Arch: "aarch64",
		},
		{
			ID: imgUbuntu, Name: "Canonical-Ubuntu-22.04-2024.09.30-0", OS: "Canonical Ubuntu", OSVersion: "22.04",
			Arch: "x86_64",
		},
	}
}

func (fakeNet) Subnet(id string) (oke.SubnetInfo, bool) {
	s, ok := fakeSubnets()[id]

	return s, ok
}

func (fakeNet) VCNExists(id string) bool { return id == vcnA || id == vcnB }
func (fakeNet) Shapes() []oke.ShapeInfo  { return fakeShapes() }
func (fakeNet) Images() []oke.ImageInfo  { return fakeImages() }

func (fakeNet) Shape(name string) (oke.ShapeInfo, bool) {
	for _, s := range fakeShapes() {
		if s.Name == name {
			return s, true
		}
	}

	return oke.ShapeInfo{}, false
}

func (fakeNet) Image(id string) (oke.ImageInfo, bool) {
	for _, i := range fakeImages() {
		if i.ID == id {
			return i, true
		}
	}

	return oke.ImageInfo{}, false
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
	c.ns.Store(time.Now().UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *oke.InMemoryBackend
	handler *oke.Handler
	clock   *fakeClock
	prov    common.ConfigurationProvider
	kp      keyPair
	ce      ke.ContainerEngineClient
	base    string
}

type envOpts struct {
	engine   func(*oke.InMemoryBackend)
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

	be := oke.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	if !opts.noNet {
		be.SetNetwork(fakeNet{})
	}

	be.SetKeyChecker(func(id string) (string, bool) {
		switch id {
		case keyOK:
			return "ENABLED", true
		case keyOff:
			return "DISABLED", true
		}

		return "", false
	})

	if opts.engine != nil {
		opts.engine(be)
	}

	h := oke.NewHandler(be)
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
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, oke.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.ce, err = ke.NewContainerEngineClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.ce.Host = e.base

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

func clusterDetails(name string) ke.CreateClusterDetails {
	return ke.CreateClusterDetails{
		Name: new(name), CompartmentId: new(testCompartment), VcnId: new(vcnA), KubernetesVersion: new(testVersion),
		EndpointConfig: &ke.CreateClusterEndpointConfigDetails{
			SubnetId: new(subRegional), IsPublicIpEnabled: new(true),
		},
	}
}

// mkCluster creates a cluster and settles it to ACTIVE.
func (e *env) mkCluster(
	t *testing.T,
	name string,
	mod func(*ke.CreateClusterDetails),
) ke.Cluster {
	t.Helper()

	d := clusterDetails(name)
	if mod != nil {
		mod(&d)
	}

	resp, err := e.ce.CreateCluster(t.Context(), ke.CreateClusterRequest{CreateClusterDetails: d})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)
	e.settle()

	list, err := e.ce.ListClusters(t.Context(), ke.ListClustersRequest{
		CompartmentId: new(testCompartment), Name: new(name),
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.Items)

	got, err := e.ce.GetCluster(t.Context(), ke.GetClusterRequest{ClusterId: list.Items[0].Id})
	require.NoError(t, err)

	return got.Cluster
}

func poolDetails(clusterID *string, name string) ke.CreateNodePoolDetails {
	return ke.CreateNodePoolDetails{
		CompartmentId: new(testCompartment), ClusterId: clusterID, Name: new(name), NodeShape: new(shapeFixed),
		NodeSourceDetails: ke.NodeSourceViaImageDetails{ImageId: new(imgOL8)},
		NodeConfigDetails: &ke.CreateNodePoolNodeConfigDetails{
			Size: new(3),
			PlacementConfigs: []ke.NodePoolPlacementConfigDetails{
				{AvailabilityDomain: new(ad1), SubnetId: new(subAD1)},
				{AvailabilityDomain: new(ad2), SubnetId: new(subAD2)},
			},
		},
	}
}

func (e *env) mkPool(
	t *testing.T,
	clusterID *string,
	name string,
	mod func(*ke.CreateNodePoolDetails),
) ke.NodePool {
	t.Helper()

	d := poolDetails(clusterID, name)
	if mod != nil {
		mod(&d)
	}

	resp, err := e.ce.CreateNodePool(t.Context(), ke.CreateNodePoolRequest{CreateNodePoolDetails: d})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)
	e.settle()

	list, err := e.ce.ListNodePools(t.Context(), ke.ListNodePoolsRequest{
		CompartmentId: new(testCompartment), ClusterId: clusterID, Name: new(name),
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.Items)

	got, err := e.ce.GetNodePool(t.Context(), ke.GetNodePoolRequest{NodePoolId: list.Items[0].Id})
	require.NoError(t, err)

	return got.NodePool
}

func quote(s string) string {
	b, _ := json.Marshal(s)

	return string(b)
}
