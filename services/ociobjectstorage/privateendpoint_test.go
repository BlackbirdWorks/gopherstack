package ociobjectstorage_test

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

const testSubnet = "ocid1.subnet.oc1..sub"

type fakeNet struct {
	ips  map[string]string
	mu   sync.Mutex
	next int
}

func (f *fakeNet) network() oci.Network {
	return oci.Network{
		Subnet: func(id string) (oci.SubnetInfo, bool) {
			if id != testSubnet && id != testSubnet+"2" {
				return oci.SubnetInfo{}, false
			}

			return oci.SubnetInfo{VcnID: "vcn-" + id[len(id)-1:]}, true
		},
		CreateNic: func(r oci.NicRequest) (oci.Nic, error) {
			f.mu.Lock()
			defer f.mu.Unlock()

			f.next++
			ip := r.IPAddress

			if ip == "" {
				ip = fmt.Sprintf("10.0.0.%d", f.next+1)
			}

			for _, used := range f.ips {
				if used == ip {
					return oci.Nic{}, &oci.APIError{
						Status: http.StatusBadRequest, Code: "InvalidParameter", Message: "IP in use",
					}
				}
			}

			id := fmt.Sprintf("vnic-%d", f.next)
			f.ips[id] = ip

			return oci.Nic{VnicID: id, IPAddress: ip}, nil
		},
		DeleteNic: func(id string) {
			f.mu.Lock()
			delete(f.ips, id)
			f.mu.Unlock()
		},
	}
}

func (f *fakeNet) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.ips)
}

func nameAndPrefix(name, prefix string) func(*objectstorage.CreatePrivateEndpointDetails) {
	return func(d *objectstorage.CreatePrivateEndpointDetails) {
		d.Name, d.Prefix = new(name), new(prefix)
	}
}

func peEnv(t *testing.T) (*env, *fakeNet) {
	t.Helper()

	e := newEnv(t, envOpts{})
	fn := &fakeNet{ips: map[string]string{}}
	e.backend.SetNetwork(fn.network())

	return e, fn
}

func peDetails(name, prefix string) objectstorage.CreatePrivateEndpointDetails {
	return objectstorage.CreatePrivateEndpointDetails{
		Name: new(name), CompartmentId: new(testCompartment), SubnetId: new(testSubnet), Prefix: new(prefix),
		AccessTargets: []objectstorage.AccessTargetDetails{
			{Namespace: new("gopherstack"), CompartmentId: new("*"), Bucket: new("*")},
		},
	}
}

func (e *env) createPE(t *testing.T, d objectstorage.CreatePrivateEndpointDetails) (string, error) {
	t.Helper()

	r, err := e.client.CreatePrivateEndpoint(t.Context(), objectstorage.CreatePrivateEndpointRequest{
		NamespaceName: new(e.ns), CreatePrivateEndpointDetails: d,
	})
	if err != nil {
		return "", err
	}

	require.NotNil(t, r.OpcWorkRequestId)

	return *r.OpcWorkRequestId, nil
}

func (e *env) getPE(t *testing.T, name string) (objectstorage.GetPrivateEndpointResponse, error) {
	t.Helper()

	return e.client.GetPrivateEndpoint(t.Context(), objectstorage.GetPrivateEndpointRequest{
		NamespaceName: new(e.ns), PeName: new(name),
	})
}

func TestPrivateEndpointLifecycle(t *testing.T) {
	t.Parallel()

	e, fn := peEnv(t)

	d := peDetails("pe1", "Pe1")
	d.AdditionalPrefixes = []string{"alt"}
	d.FreeformTags = map[string]string{"k": "v"}
	d.PrivateEndpointIp = new("10.0.0.50")

	wrID, err := e.createPE(t, d)
	require.NoError(t, err)

	wr, err := e.client.GetWorkRequest(t.Context(), objectstorage.GetWorkRequestRequest{WorkRequestId: &wrID})
	require.NoError(t, err)
	assert.Equal(t, objectstorage.WorkRequestOperationTypePrivateEndpointCreate, wr.OperationType)
	assert.Equal(t, objectstorage.WorkRequestStatusCompleted, wr.Status)
	require.Len(t, wr.Resources, 1)
	assert.Equal(t, objectstorage.WorkRequestResourceActionTypeCreated, wr.Resources[0].ActionType)
	assert.Equal(t, "privateendpoint", *wr.Resources[0].EntityType)

	got, err := e.getPE(t, "pe1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.50", *got.PrivateEndpointIp)
	assert.Equal(t, testSubnet, *got.SubnetId)
	assert.Equal(t, objectstorage.PrivateEndpointLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "pe1-gopherstack.private.objectstorage.us-ashburn-1.oci.customer-oci.com",
		*got.Fqdns.PrefixFqdns.ObjectStorageApiFqdn)
	assert.Contains(t, got.Fqdns.AdditionalPrefixesFqdns, "alt")
	assert.Equal(t, map[string]string{"k": "v"}, got.FreeformTags)
	require.Len(t, got.AccessTargets, 1)
	assert.NotEmpty(t, *got.Etag)

	lst, err := e.client.ListPrivateEndpoints(t.Context(), objectstorage.ListPrivateEndpointsRequest{
		NamespaceName: new(e.ns), CompartmentId: new(testCompartment),
	})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)
	assert.Equal(t, "pe1", *lst.Items[0].Name)

	ud := objectstorage.UpdatePrivateEndpointDetails{
		Name: new("pe2"),
		AccessTargets: []objectstorage.AccessTargetDetails{
			{Namespace: new("gopherstack"), CompartmentId: new("c1"), Bucket: new("b1")},
		},
	}
	upd, err := e.client.UpdatePrivateEndpoint(t.Context(), objectstorage.UpdatePrivateEndpointRequest{
		NamespaceName: new(e.ns), PeName: new("pe1"), UpdatePrivateEndpointDetails: ud,
	})
	require.NoError(t, err)
	require.NotNil(t, upd.OpcWorkRequestId)

	_, err = e.getPE(t, "pe1")
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	renamed, err := e.getPE(t, "pe2")
	require.NoError(t, err)
	assert.NotEqual(t, *got.Etag, *renamed.Etag)
	assert.Equal(t, "b1", *renamed.AccessTargets[0].Bucket)

	del, err := e.client.DeletePrivateEndpoint(t.Context(), objectstorage.DeletePrivateEndpointRequest{
		NamespaceName: new(e.ns), PeName: new("pe2"),
	})
	require.NoError(t, err)

	dwr, err := e.client.GetWorkRequest(
		t.Context(),
		objectstorage.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, objectstorage.WorkRequestOperationTypePrivateEndpointDelete, dwr.OperationType)
	assert.Zero(t, fn.count())

	_, err = e.getPE(t, "pe2")
	assert.Equal(t, "NotAuthorizedOrNotFound", svcErr(t, err).GetCode())
}

func TestPrivateEndpointCreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut    func(*objectstorage.CreatePrivateEndpointDetails)
		name   string
		code   string
		status int
	}{
		{
			name:   "duplicate name",
			mut:    func(d *objectstorage.CreatePrivateEndpointDetails) { d.Prefix = new("other"); d.Name = new("PE1") },
			code:   "NotAuthorizedOrResourceAlreadyExists",
			status: http.StatusConflict,
		},
		{name: "duplicate prefix", mut: nameAndPrefix("n2", "PE1PREFIX"),
			code: "NotAuthorizedOrResourceAlreadyExists", status: http.StatusConflict},
		{name: "prefix other vcn", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.SubnetId = new(testSubnet + "2")
		}},
		{name: "unknown subnet", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.Prefix = new("x")
			d.SubnetId = new("nope")
		},
			code: "RelatedResourceNotAuthorizedOrNotFound", status: http.StatusBadRequest},
		{name: "ip in use", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.Prefix = new("x")
			d.PrivateEndpointIp = new("10.0.0.2")
		}, code: "InvalidParameter", status: http.StatusBadRequest},
		{
			name:   "bad prefix",
			mut:    func(d *objectstorage.CreatePrivateEndpointDetails) { d.Name = new("n2"); d.Prefix = new("a.b") },
			code:   "InvalidParameter",
			status: http.StatusBadRequest,
		},
		{name: "bad name", mut: nameAndPrefix("a b", "x"),
			code: "InvalidParameter", status: http.StatusBadRequest},
		{name: "no targets", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.Prefix = new("x")
			d.AccessTargets = []objectstorage.AccessTargetDetails{}
		}, code: "MissingParameter", status: http.StatusBadRequest},
		{name: "wildcard namespace partial", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.Prefix = new("x")
			d.AccessTargets = []objectstorage.AccessTargetDetails{
				{Namespace: new("*"), CompartmentId: new("c"), Bucket: new("*")},
			}
		}, code: "InvalidParameter", status: http.StatusBadRequest},
		{name: "pattern target", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.Prefix = new("x")
			d.AccessTargets = []objectstorage.AccessTargetDetails{
				{Namespace: new("ns"), CompartmentId: new("c"), Bucket: new("b*")},
			}
		}, code: "InvalidParameter", status: http.StatusBadRequest},
		{name: "too many targets", mut: func(d *objectstorage.CreatePrivateEndpointDetails) {
			d.Name = new("n2")
			d.Prefix = new("x")
			d.AccessTargets = make([]objectstorage.AccessTargetDetails, 11)
			for i := range d.AccessTargets {
				d.AccessTargets[i] = objectstorage.AccessTargetDetails{
					Namespace:     new("ns"),
					CompartmentId: new("c"),
					Bucket:        new("b"),
				}
			}
		}, code: "InvalidParameter", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, fn := peEnv(t)
			_, err := e.createPE(t, peDetails("pe1", "pe1prefix"))
			require.NoError(t, err)

			before := fn.count()
			d := peDetails("pe1", "pe1prefix")
			tt.mut(&d)

			_, err = e.createPE(t, d)
			if tt.code == "" {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.code, se.GetCode())
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, before, fn.count())
		})
	}
}

func TestPrivateEndpointLimitAndPreconditions(t *testing.T) {
	t.Parallel()

	e, _ := peEnv(t)

	for i := range 10 {
		_, err := e.createPE(t, peDetails(fmt.Sprintf("pe%d", i), fmt.Sprintf("p%d", i)))
		require.NoError(t, err)
	}

	_, err := e.createPE(t, peDetails("pe10", "p10"))
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	_, err = e.client.DeletePrivateEndpoint(t.Context(), objectstorage.DeletePrivateEndpointRequest{
		NamespaceName: new(e.ns), PeName: new("pe0"), IfMatch: new("stale"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	got, err := e.getPE(t, "pe1")
	require.NoError(t, err)

	r, err := e.client.GetPrivateEndpoint(t.Context(), objectstorage.GetPrivateEndpointRequest{
		NamespaceName: new(e.ns), PeName: new("pe1"), IfNoneMatch: got.ETag,
	})
	require.Error(t, err)
	assert.Equal(t, http.StatusNotModified, r.RawResponse.StatusCode)

	_, err = e.client.ListPrivateEndpoints(t.Context(), objectstorage.ListPrivateEndpointsRequest{
		NamespaceName: new(
			e.ns,
		),
		CompartmentId:  new(testCompartment),
		LifecycleState: objectstorage.PrivateEndpointLifecycleStateActive,
	})
	require.NoError(t, err)
}

func TestPrivateEndpointSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	e, _ := peEnv(t)
	_, err := e.createPE(t, peDetails("pe1", "pe1"))
	require.NoError(t, err)

	before, err := e.getPE(t, "pe1")
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())

	e2, fn2 := peEnv(t)
	require.NoError(t, e2.backend.Restore(t.Context(), snap))

	after, err := e2.getPE(t, "pe1")
	require.NoError(t, err)
	assert.Equal(t, before.PrivateEndpoint, after.PrivateEndpoint)

	_, err = e2.createPE(t, peDetails("PE1", "other"))
	assert.Equal(t, "NotAuthorizedOrResourceAlreadyExists", svcErr(t, err).GetCode())

	e2.backend.Reset()
	_, err = e2.getPE(t, "pe1")
	require.Error(t, err)
	assert.Zero(t, fn2.count())
}
