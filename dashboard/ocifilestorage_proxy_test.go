package dashboard_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocifilestorage"
)

const (
	fsComp = "ocid1.compartment.oc1..fscomp"
	fsAD   = "Uocm:IAD-AD-1"
	fsBase = "/dashboard/api/v1/oci/filestorage"
	fsAPI  = fsBase + "/api"
)

type fsFixture struct {
	h      *dashboard.DashboardHandler
	ops    *ocifilestorage.Handler
	subnet string
}

func idOf(t *testing.T, v any) string {
	t.Helper()

	raw, err := json.Marshal(v, jsonx.V1())
	require.NoError(t, err)

	var out struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(raw, &out, jsonx.V1()))

	return out.ID
}

func newFSDashboard(t *testing.T, nfs bool) fsFixture {
	t.Helper()

	backend := ocifilestorage.NewInMemoryBackend()
	backend.SetTransition(0)

	core := ocicore.NewInMemoryBackend()
	vcn, _, aerr := core.CreateVcn(
		ocicore.CreateVcnIn{CompartmentID: fsComp, DisplayName: "vcn", CidrBlock: "10.0.0.0/16"},
	)
	require.Nil(t, aerr)

	sub, _, aerr := core.CreateSubnet(ocicore.CreateSubnetIn{
		CompartmentID: fsComp, VcnID: idOf(t, vcn), DisplayName: "files", CidrBlock: "10.0.1.0/24",
	})
	require.Nil(t, aerr)

	backend.SetNetwork(ocifilestorage.Network{
		Subnet: func(id string) (ocifilestorage.SubnetInfo, bool) {
			ref, live := core.LookupSubnet(id)

			return ocifilestorage.SubnetInfo{VcnID: ref.VcnID, CIDR: ref.CidrBlock, AD: ref.AvailabilityDomain}, live
		},
		CreateNic: func(r ocifilestorage.NicRequest) (ocifilestorage.Nic, error) {
			nic, err := core.CreateServiceNic(ocicore.ServiceNicIn{
				CompartmentID: r.CompartmentID, AD: r.AD, SubnetID: r.SubnetID, DisplayName: r.DisplayName,
				HostnameLabel: r.Hostname, PrivateIP: r.IPAddress,
			})
			if err != nil {
				return ocifilestorage.Nic{}, err
			}

			return ocifilestorage.Nic{VnicID: nic.VnicID, PrivateIPID: nic.PrivateIPID, IPAddress: nic.IPAddress}, nil
		},
		DeleteNic: core.DeleteServiceNic,
	})

	if nfs {
		plane, err := ocifilestorage.NewNFSPlane(ocifilestorage.NFSConfig{Dir: t.TempDir()})
		require.NoError(t, err)
		t.Cleanup(plane.Close)
		backend.SetDataPlane(plane)
	}

	ops := ocifilestorage.NewHandler(backend)

	return fsFixture{
		h:   dashboard.NewHandler(dashboard.Config{OCIFileStorageOps: ops, OCICoreOps: ocicore.NewHandler(core)}),
		ops: ops, subnet: idOf(t, sub),
	}
}

func (f fsFixture) proxy(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, f.h, method, path, body)
}

func (f fsFixture) create(t *testing.T, coll, body string) map[string]any {
	t.Helper()

	rec := f.proxy(t, http.MethodPost, fsAPI+"/"+coll, body)
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))

	return out
}

func (f fsFixture) fileSystem(t *testing.T, name string) string {
	t.Helper()

	id, ok := f.create(t, "fileSystems",
		`{"displayName":"`+name+`","compartmentId":"`+fsComp+`","availabilityDomain":"`+fsAD+`"}`)["id"].(string)
	require.True(t, ok)

	return id
}

func TestOCIFileStorageProxyLifecycle(t *testing.T) {
	t.Parallel()

	f := newFSDashboard(t, false)

	rec := f.proxy(t, http.MethodPost, fsAPI+"/fileSystems",
		`{"displayName":"data","compartmentId":"`+fsComp+`","availabilityDomain":"`+fsAD+`",`+
			`"kmsKeyId":"evil","freeformTags":{"owner":"mallory"},"locks":[{"type":"FULL"}]}`)
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "mallory")
	assert.NotContains(t, rec.Body.String(), "evil")

	fsID := f.fileSystem(t, "second")

	list := f.proxy(t, http.MethodGet,
		fsAPI+"/fileSystems?compartmentId="+fsComp+"&availabilityDomain="+fsAD+"&sneaky=1", "")
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())
	assert.Contains(t, list.Body.String(), `"displayName":"data"`)
	assert.Contains(t, list.Body.String(), `"meteredBytes":0`)

	snap := f.create(t, "snapshots", `{"fileSystemId":"`+fsID+`","name":"nightly"}`)
	snapID, ok := snap["id"].(string)
	require.True(t, ok)

	snaps := f.proxy(t, http.MethodGet, fsAPI+"/snapshots?fileSystemId="+fsID, "")
	require.Equal(t, http.StatusOK, snaps.Code, snaps.Body.String())
	assert.Contains(t, snaps.Body.String(), `"name":"nightly"`)

	clone := f.create(t, "fileSystems", `{"displayName":"copy","compartmentId":"`+fsComp+`",`+
		`"availabilityDomain":"`+fsAD+`","sourceSnapshotId":"`+snapID+`"}`)
	assert.Contains(t, clone, "sourceDetails")

	mt := f.create(t, "mountTargets", `{"displayName":"mt","compartmentId":"`+fsComp+`",`+
		`"availabilityDomain":"`+fsAD+`","subnetId":"`+f.subnet+`"}`)
	setID, ok := mt["exportSetId"].(string)
	require.True(t, ok)

	exp := f.create(t, "exports", `{"exportSetId":"`+setID+`","fileSystemId":"`+fsID+`","path":"/data",`+
		`"exportOptions":[{"source":"10.0.0.0/16","access":"READ_ONLY","identitySquash":"ALL",`+
		`"requirePrivilegedSourcePort":false,"anonymousUid":0}]}`)
	expID, ok := exp["id"].(string)
	require.True(t, ok)

	ips, ok := mt["privateIpIds"].([]any)
	require.True(t, ok)
	require.Len(t, ips, 1)

	ip := f.proxy(t, http.MethodGet, fsBase+"/privateips/"+ips[0].(string), "")
	require.Equal(t, http.StatusOK, ip.Code, ip.Body.String())
	assert.Contains(t, ip.Body.String(), `"ipAddress":"10.0.1.`)
	assert.NotContains(t, ip.Body.String(), "vnicId")

	badIP := f.proxy(t, http.MethodGet, fsBase+"/privateips/a%20b", "")
	assert.Equal(t, http.StatusBadRequest, badIP.Code)

	got := f.proxy(t, http.MethodGet, fsAPI+"/exports/"+expID, "")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Contains(t, got.Body.String(), `"access":"READ_ONLY"`)
	assert.Contains(t, got.Body.String(), `"identitySquash":"ALL"`)
	assert.NotContains(t, got.Body.String(), "anonymousUid\":0")

	for _, p := range []string{
		"exportSets?compartmentId=" + fsComp + "&availabilityDomain=" + fsAD,
		"mountTargets?compartmentId=" + fsComp + "&availabilityDomain=" + fsAD,
		"filesystemSnapshotPolicies?compartmentId=" + fsComp + "&availabilityDomain=" + fsAD,
		"replications?compartmentId=" + fsComp + "&availabilityDomain=" + fsAD,
		"exports?exportSetId=" + setID,
	} {
		r := f.proxy(t, http.MethodGet, fsAPI+"/"+p, "")
		assert.Equal(t, http.StatusOK, r.Code, "%s: %s", p, r.Body.String())
	}

	for _, p := range []string{"exports/" + expID, "snapshots/" + snapID} {
		r := f.proxy(t, http.MethodDelete, fsAPI+"/"+p, "")
		assert.Less(t, r.Code, http.StatusMultipleChoices, "%s: %s", p, r.Body.String())
	}

	comps := f.proxy(t, http.MethodGet, fsBase+"/compartments", "")
	require.Equal(t, http.StatusOK, comps.Code)
	assert.Contains(t, comps.Body.String(), fsComp)
}

func TestOCIFileStorageProxyRejectsOutsideAllowList(t *testing.T) {
	t.Parallel()

	f := newFSDashboard(t, false)
	id := f.fileSystem(t, "guarded")

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"change compartment", http.MethodPost, fsAPI + "/fileSystems/" + id + "/actions/changeCompartment", `{}`},
		{"add lock", http.MethodPost, fsAPI + "/fileSystems/" + id + "/actions/addLock", `{}`},
		{"quota rules", http.MethodGet, fsAPI + "/fileSystems/" + id + "/quotaRules", ""},
		{"delete export set", http.MethodDelete, fsAPI + "/exportSets/x", ""},
		{"create export set", http.MethodPost, fsAPI + "/exportSets", `{}`},
		{"create policy", http.MethodPost, fsAPI + "/filesystemSnapshotPolicies", `{}`},
		{"delete policy", http.MethodDelete, fsAPI + "/filesystemSnapshotPolicies/p", ""},
		{"create replication", http.MethodPost, fsAPI + "/replications", `{}`},
		{"delete replication", http.MethodDelete, fsAPI + "/replications/r", ""},
		{"connectors", http.MethodGet, fsAPI + "/outboundConnectors", ""},
		{"post to item", http.MethodPost, fsAPI + "/fileSystems/" + id, `{}`},
		{"delete collection", http.MethodDelete, fsAPI + "/fileSystems", ""},
		{"traversal", http.MethodGet, fsAPI + "/fileSystems/%2e%2e", ""},
		{"encoded slash", http.MethodGet, fsAPI + "/fileSystems/a%2Fb", ""},
		{"admin keys", http.MethodGet, fsAPI + "/_gopherstack/oci/keys", ""},
		{"empty", http.MethodGet, fsAPI + "/", ""},
		{"deep path", http.MethodGet, fsAPI + "/fileSystems/" + id + "/snapshots", ""},
		{"fs missing ad", http.MethodPost, fsAPI + "/fileSystems", `{"compartmentId":"` + fsComp + `"}`},
		{
			"fs bad source", http.MethodPost, fsAPI + "/fileSystems",
			`{"compartmentId":"` + fsComp + `","availabilityDomain":"` + fsAD + `","sourceSnapshotId":"a/b"}`,
		},
		{
			"mt bad subnet", http.MethodPost, fsAPI + "/mountTargets",
			`{"compartmentId":"` + fsComp + `","availabilityDomain":"` + fsAD + `","subnetId":"a b"}`,
		},
		{
			"mt bad hostname", http.MethodPost, fsAPI + "/mountTargets",
			`{"compartmentId":"` + fsComp + `","availabilityDomain":"` + fsAD + `","subnetId":"` + f.subnet +
				`","hostnameLabel":"-bad"}`,
		},
		{
			"mt bad ip", http.MethodPost, fsAPI + "/mountTargets",
			`{"compartmentId":"` + fsComp + `","availabilityDomain":"` + fsAD + `","subnetId":"` + f.subnet +
				`","ipAddress":"nope"}`,
		},
		{
			"export bad path", http.MethodPost, fsAPI + "/exports",
			`{"exportSetId":"s","fileSystemId":"` + id + `","path":"data"}`,
		},
		{
			"export bad source", http.MethodPost, fsAPI + "/exports",
			`{"exportSetId":"s","fileSystemId":"` + id + `","path":"/d","exportOptions":[` +
				`{"source":"everyone","access":"READ_WRITE","identitySquash":"ROOT"}]}`,
		},
		{
			"export bad access", http.MethodPost, fsAPI + "/exports",
			`{"exportSetId":"s","fileSystemId":"` + id + `","path":"/d","exportOptions":[` +
				`{"source":"10.0.0.1","access":"ADMIN","identitySquash":"ROOT"}]}`,
		},
		{
			"export bad squash", http.MethodPost, fsAPI + "/exports",
			`{"exportSetId":"s","fileSystemId":"` + id + `","path":"/d","exportOptions":[` +
				`{"source":"10.0.0.1","access":"READ_WRITE","identitySquash":"WHATEVER"}]}`,
		},
		{"snapshot bad name", http.MethodPost, fsAPI + "/snapshots", `{"fileSystemId":"` + id + `","name":"a/b"}`},
		{"snapshot no fs", http.MethodPost, fsAPI + "/snapshots", `{"name":"x"}`},
		{"malformed", http.MethodPost, fsAPI + "/snapshots", `{`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := f.proxy(t, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, got.Code, "%s %s: %s", tt.method, tt.path, got.Body.String())
		})
	}
}

func TestOCIFileStorageProxyNoUpdates(t *testing.T) {
	t.Parallel()

	f := newFSDashboard(t, false)
	id := f.fileSystem(t, "frozen")

	for _, p := range []string{"/fileSystems/" + id, "/mountTargets/m", "/exports/e"} {
		got := f.proxy(t, http.MethodPut, fsAPI+p, `{"displayName":"renamed"}`)
		assert.GreaterOrEqual(t, got.Code, http.StatusBadRequest, p)
	}

	got := f.proxy(t, http.MethodGet, fsAPI+"/fileSystems/"+id, "")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Contains(t, got.Body.String(), `"displayName":"frozen"`)
}

func TestOCIFileStorageProxyNFSEndpoint(t *testing.T) {
	t.Parallel()

	stub := newFSDashboard(t, false)
	off := stub.proxy(t, http.MethodGet, fsBase+"/nfs", "")
	require.Equal(t, http.StatusOK, off.Code)
	assert.Contains(t, off.Body.String(), `"enabled":false`)

	f := newFSDashboard(t, true)
	fsID := f.fileSystem(t, "nfs")
	mt := f.create(t, "mountTargets", `{"compartmentId":"`+fsComp+`","availabilityDomain":"`+fsAD+
		`","subnetId":"`+f.subnet+`"}`)
	setID, ok := mt["exportSetId"].(string)
	require.True(t, ok)

	f.create(t, "exports", `{"exportSetId":"`+setID+`","fileSystemId":"`+fsID+`","path":"/nfs",`+
		`"exportOptions":[{"source":"127.0.0.1","access":"READ_WRITE","identitySquash":"NONE"}]}`)

	on := f.proxy(t, http.MethodGet, fsBase+"/nfs", "")
	require.Equal(t, http.StatusOK, on.Code)

	var out struct {
		Endpoints map[string]string `json:"endpoints"`
		Enabled   bool              `json:"enabled"`
	}

	require.NoError(t, json.Unmarshal(on.Body.Bytes(), &out, jsonx.V1()))
	assert.True(t, out.Enabled)
	require.Len(t, out.Endpoints, 1)

	for _, addr := range out.Endpoints {
		assert.True(t, strings.HasPrefix(addr, "127.0.0.1:"), addr)
	}
}

func TestOCIFileStorageProxyUnavailableWithoutService(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, p := range []struct{ method, path string }{
		{http.MethodGet, fsBase + "/compartments"},
		{http.MethodGet, fsBase + "/nfs"},
		{http.MethodGet, fsBase + "/availabilitydomains"},
		{http.MethodGet, fsAPI + "/fileSystems?compartmentId=" + fsComp},
		{http.MethodPost, fsAPI + "/snapshots"},
		{http.MethodDelete, fsAPI + "/exports/e1"},
	} {
		got := lbDo(t, h, p.method, p.path, `{}`)
		assert.Equal(t, http.StatusServiceUnavailable, got.Code, "%s %s", p.method, p.path)
	}
}
