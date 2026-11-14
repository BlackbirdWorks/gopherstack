package ocidns_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/ocidns"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
	otherCompart    = "ocid1.compartment.oc1..other"
	testRegion      = "us-ashburn-1"
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

type vcnList struct {
	list []ocidns.VcnInfo
	mu   sync.Mutex
}

func (v *vcnList) set(list ...ocidns.VcnInfo) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.list = list
}

func (v *vcnList) get() []ocidns.VcnInfo {
	v.mu.Lock()
	defer v.mu.Unlock()

	return append([]ocidns.VcnInfo(nil), v.list...)
}

type env struct {
	backend *ocidns.InMemoryBackend
	handler *ocidns.Handler
	clock   *fakeClock
	client  ocidnssdk.DnsClient
	vcns    *vcnList
	base    string
}

type envOpts struct {
	enforce  bool
	register bool
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	be := ocidns.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	vcns := &vcnList{}
	be.SetVcnSource(vcns.get)
	be.SetSubnetResolver(func(id string) (ocidns.SubnetInfo, bool) {
		if id == "ocid1.subnet.oc1..sn" {
			return ocidns.SubnetInfo{VcnID: "ocid1.vcn.oc1..vcn", Cidr: "10.0.1.0/24"}, true
		}

		if id == "ocid1.subnet.oc1..other" {
			return ocidns.SubnetInfo{VcnID: "ocid1.vcn.oc1..elsewhere", Cidr: "10.9.0.0/24"}, true
		}

		return ocidns.SubnetInfo{}, false
	})
	be.SetCompartmentChecker(func(id string) bool { return id == testCompartment || id == otherCompart })

	h := ocidns.NewHandler(be)
	h.Port, h.DNSEphemeral = 0, true
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	prov := common.NewRawConfigurationProvider(testTenancy, testUser, testRegion, kp.fprint, kp.privPEM, nil)
	e := &env{backend: be, handler: h, clock: clock, vcns: vcns, base: fmt.Sprintf("http://localhost:%d", h.Port)}

	var err error

	e.client, err = ocidnssdk.NewDnsClientWithConfigurationProvider(prov)
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

func rawGet(t *testing.T, url string, headers map[string]string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out), resp.Header
}

func (e *env) createZone(t *testing.T, name string, mutate func(*ocidnssdk.CreateZoneDetails)) ocidnssdk.Zone {
	t.Helper()

	d := ocidnssdk.CreateZoneDetails{Name: new(name), CompartmentId: new(testCompartment)}
	if mutate != nil {
		mutate(&d)
	}

	resp, err := e.client.CreateZone(t.Context(), ocidnssdk.CreateZoneRequest{CreateZoneDetails: d})
	require.NoError(t, err)

	return resp.Zone
}

func (e *env) getZone(t *testing.T, ref string) ocidnssdk.GetZoneResponse {
	t.Helper()

	resp, err := e.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{ZoneNameOrId: new(ref)})
	require.NoError(t, err)

	return resp
}

func op(kind ocidnssdk.RecordOperationOperationEnum, domain, rtype, rdata string, ttl int) ocidnssdk.RecordOperation {
	o := ocidnssdk.RecordOperation{Operation: kind}
	if domain != "" {
		o.Domain = new(domain)
	}

	if rtype != "" {
		o.Rtype = new(rtype)
	}

	if rdata != "" {
		o.Rdata = new(rdata)
	}

	if ttl != 0 {
		o.Ttl = new(ttl)
	}

	return o
}

func add(domain, rtype, rdata string, ttl int) ocidnssdk.RecordOperation {
	return op(ocidnssdk.RecordOperationOperationAdd, domain, rtype, rdata, ttl)
}

func remove(domain, rtype, rdata string) ocidnssdk.RecordOperation {
	return op(ocidnssdk.RecordOperationOperationRemove, domain, rtype, rdata, 0)
}

func (e *env) patch(
	t *testing.T,
	zone string,
	ops ...ocidnssdk.RecordOperation,
) (ocidnssdk.PatchZoneRecordsResponse, error) {
	t.Helper()

	return e.client.PatchZoneRecords(t.Context(), ocidnssdk.PatchZoneRecordsRequest{
		ZoneNameOrId: new(zone),
		Items:        ops,
	})
}

func (e *env) mustPatch(t *testing.T, zone string, ops ...ocidnssdk.RecordOperation) {
	t.Helper()

	_, err := e.patch(t, zone, ops...)
	require.NoError(t, err)
}

// userRecords renders the non-protected records as "domain rtype ttl rdata".
func userRecords(items []ocidnssdk.Record) []string {
	var out []string

	for _, r := range items {
		if r.IsProtected != nil && *r.IsProtected {
			continue
		}

		out = append(out, fmt.Sprintf("%s %s %d %s", *r.Domain, *r.Rtype, *r.Ttl, *r.Rdata))
	}

	return out
}

func (e *env) records(t *testing.T, zone string) []ocidnssdk.Record {
	t.Helper()

	resp, err := e.client.GetZoneRecords(t.Context(), ocidnssdk.GetZoneRecordsRequest{ZoneNameOrId: new(zone)})
	require.NoError(t, err)

	return resp.Items
}

func readCloser(s string) io.ReadCloser { return io.NopCloser(strings.NewReader(s)) }
