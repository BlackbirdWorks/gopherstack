package ocidns_test

import (
	"net/http"
	"testing"
	"time"

	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ocidns"
)

const (
	testVcn    = "ocid1.vcn.oc1..vcn"
	testSubnet = "ocid1.subnet.oc1..sn"
)

func (e *env) createView(t *testing.T, name string) ocidnssdk.View {
	t.Helper()

	resp, err := e.client.CreateView(t.Context(), ocidnssdk.CreateViewRequest{
		CompartmentId: new(testCompartment), DisplayName: new(name)})
	require.NoError(t, err)

	return resp.View
}

func TestViewLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.createView(t, "corp")
	assert.Equal(t, ocidnssdk.ViewLifecycleStateActive, v.LifecycleState)
	assert.False(t, *v.IsProtected)
	assert.Contains(t, *v.Id, "ocid1.dnsview.oc1..")

	upd, err := e.client.UpdateView(t.Context(), ocidnssdk.UpdateViewRequest{
		ViewId: v.Id, DisplayName: new("corp2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "corp2", *upd.DisplayName)

	list, err := e.client.ListViews(t.Context(), ocidnssdk.ListViewsRequest{
		CompartmentId: new(testCompartment), DisplayName: new("CORP2"),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	empty, err := e.client.ListViews(t.Context(), ocidnssdk.ListViewsRequest{
		CompartmentId: new(testCompartment), Scope: ocidnssdk.ListViewsScopeGlobal,
	})
	require.NoError(t, err)
	assert.Empty(t, empty.Items)

	// A private zone in the view pins it.
	z := e.createZone(t, "corp.internal", func(d *ocidnssdk.CreateZoneDetails) {
		d.Scope, d.ViewId = ocidnssdk.ScopePrivate, v.Id
	})
	assert.Equal(t, ocidnssdk.ScopePrivate, z.Scope)
	assert.Equal(t, *v.Id, *z.ViewId)
	assert.Empty(t, z.Nameservers)

	_, err = e.client.DeleteView(t.Context(), ocidnssdk.DeleteViewRequest{ViewId: v.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.ChangeViewCompartment(t.Context(), ocidnssdk.ChangeViewCompartmentRequest{
		ViewId: v.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	got, err := e.client.GetView(t.Context(), ocidnssdk.GetViewRequest{ViewId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *got.CompartmentId)

	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{
		ZoneNameOrId: new("corp.internal"), Scope: ocidnssdk.DeleteZoneScopePrivate, ViewId: v.Id,
	})
	require.NoError(t, err)

	_, err = e.client.DeleteView(t.Context(), ocidnssdk.DeleteViewRequest{ViewId: v.Id})
	require.NoError(t, err)

	_, err = e.client.GetView(t.Context(), ocidnssdk.GetViewRequest{ViewId: v.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestPrivateZoneScoping(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v1, v2 := e.createView(t, "one"), e.createView(t, "two")

	mk := func(v ocidnssdk.View) ocidnssdk.Zone {
		return e.createZone(t, "same.internal", func(d *ocidnssdk.CreateZoneDetails) {
			d.Scope, d.ViewId = ocidnssdk.ScopePrivate, v.Id
		})
	}

	z1, z2 := mk(v1), mk(v2)
	assert.NotEqual(t, *z1.Id, *z2.Id, "private names only need to be unique within a view")

	_, err := e.client.CreateZone(
		t.Context(),
		ocidnssdk.CreateZoneRequest{CreateZoneDetails: ocidnssdk.CreateZoneDetails{
			Name: new("same.internal"), CompartmentId: new(testCompartment),
			Scope: ocidnssdk.ScopePrivate, ViewId: v1.Id,
		}},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	got, err := e.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{
		ZoneNameOrId: new("same.internal"), Scope: ocidnssdk.GetZoneScopePrivate, ViewId: v2.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, *z2.Id, *got.Id)

	_, err = e.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{ZoneNameOrId: new("same.internal")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "global lookups do not see private zones")

	_, err = e.client.PatchZoneRecords(t.Context(), ocidnssdk.PatchZoneRecordsRequest{
		ZoneNameOrId: new("same.internal"), Scope: ocidnssdk.PatchZoneRecordsScopePrivate, ViewId: v1.Id,
		Items: []ocidnssdk.RecordOperation{add("db.same.internal", "A", "10.0.0.5", 60)},
	})
	require.NoError(t, err)

	rec1, err := e.client.GetZoneRecords(t.Context(), ocidnssdk.GetZoneRecordsRequest{
		ZoneNameOrId: z1.Id, Scope: ocidnssdk.GetZoneRecordsScopePrivate, ViewId: v1.Id,
	})
	require.NoError(t, err)
	rec2, err := e.client.GetZoneRecords(t.Context(), ocidnssdk.GetZoneRecordsRequest{ZoneNameOrId: z2.Id})
	require.NoError(t, err)
	assert.Equal(t, []string{"db.same.internal A 60 10.0.0.5"}, userRecords(rec1.Items))
	assert.Empty(t, userRecords(rec2.Items))
	assert.Len(t, rec1.Items, 2, "private zones carry a managed SOA and no NS set")

	list, err := e.client.ListZones(t.Context(), ocidnssdk.ListZonesRequest{
		CompartmentId: new(testCompartment), Scope: ocidnssdk.ListZonesScopePrivate, ViewId: v2.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, *z2.Id, *list.Items[0].Id)
}

func TestProtectedViewRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.vcns.set(
		ocidns.VcnInfo{ID: testVcn, CompartmentID: testCompartment, DisplayName: "vcn-a", Created: e.clock.Now()},
	)

	views, err := e.client.ListViews(t.Context(), ocidnssdk.ListViewsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, views.Items, 1)

	def := views.Items[0]
	assert.True(t, *def.IsProtected)
	assert.Equal(t, "vcn-a", *def.DisplayName)

	_, err = e.client.UpdateView(t.Context(), ocidnssdk.UpdateViewRequest{
		ViewId: def.Id, DisplayName: new("x"),
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = e.client.DeleteView(t.Context(), ocidnssdk.DeleteViewRequest{ViewId: def.Id})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = e.client.CreateZone(
		t.Context(),
		ocidnssdk.CreateZoneRequest{CreateZoneDetails: ocidnssdk.CreateZoneDetails{
			Name: new("host.oraclevcn.com"), CompartmentId: new(testCompartment),
			Scope: ocidnssdk.ScopePrivate, ViewId: def.Id,
		}},
	)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	e.createZone(t, "vcn.example", func(d *ocidnssdk.CreateZoneDetails) {
		d.Scope, d.ViewId = ocidnssdk.ScopePrivate, def.Id
	})
}

func TestResolversFollowVcns(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.vcns.set(
		ocidns.VcnInfo{ID: testVcn, CompartmentID: testCompartment, DisplayName: "vcn-a", Created: e.clock.Now()},
	)

	list, err := e.client.ListResolvers(
		t.Context(),
		ocidnssdk.ListResolversRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	r := list.Items[0]
	assert.Equal(t, testVcn, *r.AttachedVcnId)
	assert.Equal(t, "vcn-a", *r.DisplayName)
	assert.Contains(t, *r.Id, "ocid1.dnsresolver.oc1..")

	got, err := e.client.GetResolver(t.Context(), ocidnssdk.GetResolverRequest{ResolverId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, *r.DefaultViewId, *got.DefaultViewId)
	assert.Empty(t, got.Endpoints)
	assert.Empty(t, got.AttachedViews)
	assert.NotEmpty(t, *got.Etag)

	view, err := e.client.GetView(t.Context(), ocidnssdk.GetViewRequest{ViewId: r.DefaultViewId})
	require.NoError(t, err)
	assert.True(t, *view.IsProtected)

	custom := e.createView(t, "custom")
	upd, err := e.client.UpdateResolver(t.Context(), ocidnssdk.UpdateResolverRequest{
		ResolverId:    r.Id,
		DisplayName:   new("renamed"),
		AttachedViews: []ocidnssdk.AttachedViewDetails{{ViewId: custom.Id}},
		FreeformTags:  map[string]string{"x": "y"},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)
	require.Len(t, upd.AttachedViews, 1)
	assert.Equal(t, *custom.Id, *upd.AttachedViews[0].ViewId)

	_, err = e.client.DeleteView(t.Context(), ocidnssdk.DeleteViewRequest{ViewId: custom.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "attached to a resolver")

	_, err = e.client.UpdateResolver(t.Context(), ocidnssdk.UpdateResolverRequest{
		ResolverId:    r.Id,
		AttachedViews: []ocidnssdk.AttachedViewDetails{{ViewId: new("ocid1.dnsview.oc1..nope")}},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.ChangeResolverCompartment(t.Context(), ocidnssdk.ChangeResolverCompartmentRequest{
		ResolverId: r.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	named, err := e.client.ListResolvers(t.Context(), ocidnssdk.ListResolversRequest{
		CompartmentId: new(otherCompart), DisplayName: new("RENAMED"),
	})
	require.NoError(t, err)
	assert.Len(t, named.Items, 1)

	// Deleting the VCN removes its resolver and the default view with it.
	e.vcns.set()

	_, err = e.client.GetResolver(t.Context(), ocidnssdk.GetResolverRequest{ResolverId: r.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetView(t.Context(), ocidnssdk.GetViewRequest{ViewId: r.DefaultViewId})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func (e *env) resolverID(t *testing.T) *string {
	t.Helper()

	e.vcns.set(
		ocidns.VcnInfo{ID: testVcn, CompartmentID: testCompartment, DisplayName: "vcn-a", Created: e.clock.Now()},
	)

	list, err := e.client.ListResolvers(
		t.Context(),
		ocidnssdk.ListResolversRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	return list.Items[0].Id
}

func vnicEndpoint(name string, fwd, listen bool) ocidnssdk.CreateResolverVnicEndpointDetails {
	return ocidnssdk.CreateResolverVnicEndpointDetails{
		Name: new(name), SubnetId: new(testSubnet), IsForwarding: new(fwd), IsListening: new(listen),
	}
}

func TestResolverEndpoints(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	rid := e.resolverID(t)

	create := func(d ocidnssdk.CreateResolverVnicEndpointDetails) (ocidnssdk.CreateResolverEndpointResponse, error) {
		return e.client.CreateResolverEndpoint(t.Context(), ocidnssdk.CreateResolverEndpointRequest{
			ResolverId: rid, CreateResolverEndpointDetails: d,
		})
	}

	resp, err := create(vnicEndpoint("fwd", true, false))
	require.NoError(t, err)

	ep, ok := resp.ResolverEndpoint.(ocidnssdk.ResolverVnicEndpoint)
	require.True(t, ok, "%T", resp.ResolverEndpoint)
	assert.Equal(t, "10.0.1.2", *ep.ForwardingAddress, "first usable subnet address")
	assert.Nil(t, ep.ListeningAddress)
	assert.Equal(t, testSubnet, *ep.SubnetId)
	assert.Equal(t, ocidnssdk.ResolverEndpointLifecycleStateActive, ep.LifecycleState)

	both, err := create(vnicEndpoint("both", true, true))
	require.NoError(t, err)

	bothEp, ok := both.ResolverEndpoint.(ocidnssdk.ResolverVnicEndpoint)
	require.True(t, ok)
	assert.Equal(t, "10.0.1.3", *bothEp.ForwardingAddress)
	assert.Equal(t, "10.0.1.4", *bothEp.ListeningAddress)

	pinned := vnicEndpoint("pinned", false, true)
	pinned.ListeningAddress = new("10.0.1.50")
	pinResp, err := create(pinned)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.50", *pinResp.ResolverEndpoint.(ocidnssdk.ResolverVnicEndpoint).ListeningAddress)

	bad := []struct {
		mutate func(*ocidnssdk.CreateResolverVnicEndpointDetails)
		name   string
		status int
	}{
		{
			name:   "duplicate name case-insensitive",
			mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) { d.Name = new("FWD") },
			status: http.StatusConflict,
		},
		{name: "neither role", mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) {
			d.Name, d.IsForwarding, d.IsListening = new("n1"), new(false), new(false)
		}, status: http.StatusBadRequest},
		{name: "unknown subnet", mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) {
			d.Name, d.SubnetId = new("n2"), new("ocid1.subnet.oc1..nope")
		}, status: http.StatusNotFound},
		{name: "subnet of another vcn", mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) {
			d.Name, d.SubnetId = new("n3"), new("ocid1.subnet.oc1..other")
		}, status: http.StatusBadRequest},
		{name: "address outside subnet", mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) {
			d.Name, d.ForwardingAddress = new("n4"), new("192.168.0.9")
		}, status: http.StatusBadRequest},
		{name: "address in use", mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) {
			d.Name, d.ForwardingAddress = new("n5"), new("10.0.1.2")
		}, status: http.StatusConflict},
		{name: "address for disabled role", mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) {
			d.Name, d.IsListening, d.ListeningAddress = new("n6"), new(false), new("10.0.1.60")
		}, status: http.StatusBadRequest},
		{
			name:   "bad name",
			mutate: func(d *ocidnssdk.CreateResolverVnicEndpointDetails) { d.Name = new("has space") },
			status: http.StatusBadRequest,
		},
	}

	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := vnicEndpoint("x", true, false)
			tt.mutate(&d)

			_, cerr := create(d)
			assert.Equal(t, tt.status, svcErr(t, cerr).GetHTTPStatusCode())
		})
	}

	got, err := e.client.GetResolverEndpoint(
		t.Context(),
		ocidnssdk.GetResolverEndpointRequest{ResolverId: rid, ResolverEndpointName: new("FWD")},
	)
	require.NoError(t, err)
	assert.Equal(t, "fwd", *got.ResolverEndpoint.(ocidnssdk.ResolverVnicEndpoint).Name)

	list, err := e.client.ListResolverEndpoints(t.Context(), ocidnssdk.ListResolverEndpointsRequest{
		ResolverId: rid,
		SortBy:     ocidnssdk.ListResolverEndpointsSortByName,
		SortOrder:  ocidnssdk.ListResolverEndpointsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 3)
	assert.Equal(t, "both", *list.Items[0].(ocidnssdk.ResolverVnicEndpointSummary).Name)

	upd, err := e.client.UpdateResolverEndpoint(t.Context(), ocidnssdk.UpdateResolverEndpointRequest{
		ResolverId: rid, ResolverEndpointName: new("fwd"),
		UpdateResolverEndpointDetails: ocidnssdk.UpdateResolverVnicEndpointDetails{
			FreeformTags: map[string]string{"t": "1"}, NsgIds: []string{"ocid1.nsg.oc1..a"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"ocid1.nsg.oc1..a"}, upd.ResolverEndpoint.(ocidnssdk.ResolverVnicEndpoint).NsgIds)

	// The resolver reports its endpoints, and a forward rule pins its source endpoint.
	r, err := e.client.GetResolver(t.Context(), ocidnssdk.GetResolverRequest{ResolverId: rid})
	require.NoError(t, err)
	assert.Len(t, r.Endpoints, 3)

	rule := func(source string) ocidnssdk.UpdateResolverRequest {
		return ocidnssdk.UpdateResolverRequest{ResolverId: rid,
			Rules: []ocidnssdk.ResolverRuleDetails{ocidnssdk.ResolverForwardRuleDetails{
				DestinationAddresses: []string{"192.0.2.53"}, SourceEndpointName: new(source),
				QnameCoverConditions: []string{"corp.example"}, ClientAddressConditions: []string{"10.0.0.0/16"},
			}}}
	}

	_, err = e.client.UpdateResolver(t.Context(), rule("pinned"))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "listening-only endpoint cannot forward")

	_, err = e.client.UpdateResolver(t.Context(), rule("nope"))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	withRule, err := e.client.UpdateResolver(t.Context(), rule("FWD"))
	require.NoError(t, err)
	require.Len(t, withRule.Rules, 1)

	fr, ok := withRule.Rules[0].(ocidnssdk.ResolverForwardRule)
	require.True(t, ok, "%T", withRule.Rules[0])
	assert.Equal(t, []string{"192.0.2.53"}, fr.DestinationAddresses)
	assert.Equal(t, "fwd", *fr.SourceEndpointName)

	_, err = e.client.DeleteResolverEndpoint(
		t.Context(),
		ocidnssdk.DeleteResolverEndpointRequest{ResolverId: rid, ResolverEndpointName: new("fwd")},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteResolverEndpoint(
		t.Context(),
		ocidnssdk.DeleteResolverEndpointRequest{ResolverId: rid, ResolverEndpointName: new("both")},
	)
	require.NoError(t, err)

	_, err = e.client.GetResolverEndpoint(
		t.Context(),
		ocidnssdk.GetResolverEndpointRequest{ResolverId: rid, ResolverEndpointName: new("both")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.CreateResolverEndpoint(t.Context(), ocidnssdk.CreateResolverEndpointRequest{
		ResolverId: new(
			"ocid1.dnsresolver.oc1..nope",
		),
		CreateResolverEndpointDetails: vnicEndpoint("z", true, false),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestTsigKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	secret := "c2VjcmV0LWJ5dGVzLTEyMzQ1Njc4OTA="

	create := func(name, algo, sec string) (ocidnssdk.CreateTsigKeyResponse, error) {
		return e.client.CreateTsigKey(t.Context(), ocidnssdk.CreateTsigKeyRequest{
			Name: new(name), Algorithm: new(algo), Secret: new(sec),
			CompartmentId: new(testCompartment)})
	}

	resp, err := create("Transfer-Key.Example.", "HMAC-SHA256", secret)
	require.NoError(t, err)
	assert.Equal(t, "transfer-key.example", *resp.Name)
	assert.Equal(t, "hmac-sha256", *resp.Algorithm)
	assert.Equal(t, secret, *resp.Secret)

	got, err := e.client.GetTsigKey(t.Context(), ocidnssdk.GetTsigKeyRequest{TsigKeyId: resp.Id})
	require.NoError(t, err)
	assert.Equal(t, secret, *got.Secret, "the SDK TsigKey model carries the secret on get")

	status, body, _ := rawGet(t, e.base+"/20180115/tsigKeys?compartmentId="+testCompartment, nil)
	assert.Equal(t, http.StatusOK, status)
	assert.NotContains(t, body, "secret", "TsigKeySummary has no secret")
	assert.Contains(t, body, "transfer-key.example")

	bad := []struct {
		name, algo, secret string
		status             int
	}{
		{"dup.example", "hmac-md5", secret, http.StatusBadRequest},
		{"dup.example", "hmac-sha1", "***", http.StatusBadRequest},
		{"bad name", "hmac-sha1", secret, http.StatusBadRequest},
		{"transfer-key.example", "hmac-sha1", secret, http.StatusConflict},
		{"", "hmac-sha1", secret, http.StatusBadRequest},
	}
	for _, tt := range bad {
		_, err = create(tt.name, tt.algo, tt.secret)
		assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode(), tt.name+"/"+tt.algo)
	}

	list, err := e.client.ListTsigKeys(
		t.Context(),
		ocidnssdk.ListTsigKeysRequest{CompartmentId: new(testCompartment), Name: new("TRANSFER-KEY.example")},
	)
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	upd, err := e.client.UpdateTsigKey(t.Context(), ocidnssdk.UpdateTsigKeyRequest{
		TsigKeyId: resp.Id, FreeformTags: map[string]string{"a": "b"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, upd.FreeformTags)
	assert.Equal(t, secret, *upd.Secret)

	// A zone that names the key pins it.
	e.createZone(t, "tsig.example", func(d *ocidnssdk.CreateZoneDetails) {
		d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
		d.ExternalMasters = []ocidnssdk.ExternalMaster{{Address: new("192.0.2.5"), TsigKeyId: resp.Id}}
	})

	byKey, err := e.client.ListZones(
		t.Context(),
		ocidnssdk.ListZonesRequest{CompartmentId: new(testCompartment), TsigKeyId: resp.Id},
	)
	require.NoError(t, err)
	assert.Len(t, byKey.Items, 1)

	_, err = e.client.DeleteTsigKey(t.Context(), ocidnssdk.DeleteTsigKeyRequest{TsigKeyId: resp.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: new("tsig.example")})
	require.NoError(t, err)

	_, err = e.client.ChangeTsigKeyCompartment(t.Context(), ocidnssdk.ChangeTsigKeyCompartmentRequest{
		TsigKeyId: resp.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	_, err = e.client.DeleteTsigKey(t.Context(), ocidnssdk.DeleteTsigKeyRequest{TsigKeyId: resp.Id})
	require.NoError(t, err)

	_, err = e.client.GetTsigKey(t.Context(), ocidnssdk.GetTsigKeyRequest{TsigKeyId: resp.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSteeringPoliciesAndAttachments(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	zone := e.createZone(t, "lb.example", nil)

	answers := []ocidnssdk.SteeringPolicyAnswer{
		{Name: new("east"), Rtype: new("A"), Rdata: new("192.0.2.10"), Pool: new("east")},
		{Name: new("west"), Rtype: new("AAAA"), Rdata: new("2001:db8::10"), Pool: new("west")},
	}

	resp, err := e.client.CreateSteeringPolicy(t.Context(), ocidnssdk.CreateSteeringPolicyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("failover"),
		Template: ocidnssdk.CreateSteeringPolicyDetailsTemplateFailover, Answers: answers,
		Rules: []ocidnssdk.SteeringPolicyRule{ocidnssdk.SteeringPolicyPriorityRule{
			Cases: []ocidnssdk.SteeringPolicyPriorityRuleCase{{AnswerData: []ocidnssdk.SteeringPolicyPriorityAnswerData{
				{AnswerCondition: new("answer.name == 'east'"), Value: new(1)},
			}}},
		}},
	})
	require.NoError(t, err)

	p := resp.SteeringPolicy
	assert.Equal(t, 30, *p.Ttl, "default ttl")
	assert.Equal(t, ocidnssdk.SteeringPolicyLifecycleStateActive, p.LifecycleState)
	require.Len(t, p.Rules, 1)
	assert.IsType(t, ocidnssdk.SteeringPolicyPriorityRule{}, p.Rules[0])
	assert.Len(t, p.Answers, 2)

	bad := []struct {
		mutate func(*ocidnssdk.CreateSteeringPolicyDetails)
		name   string
	}{
		{name: "duplicate answer names", mutate: func(d *ocidnssdk.CreateSteeringPolicyDetails) {
			d.Answers = []ocidnssdk.SteeringPolicyAnswer{answers[0], answers[0]}
		}},
		{name: "bad rdata", mutate: func(d *ocidnssdk.CreateSteeringPolicyDetails) {
			d.Answers = []ocidnssdk.SteeringPolicyAnswer{{Name: new("x"), Rtype: new("A"), Rdata: new("nope")}}
		}},
		{name: "ttl out of range", mutate: func(d *ocidnssdk.CreateSteeringPolicyDetails) { d.Ttl = new(-1) }},
	}
	for _, tt := range bad {
		d := ocidnssdk.CreateSteeringPolicyDetails{
			CompartmentId: new(testCompartment), DisplayName: new("bad"),
			Template: ocidnssdk.CreateSteeringPolicyDetailsTemplateCustom,
		}
		tt.mutate(&d)

		_, err = e.client.CreateSteeringPolicy(
			t.Context(),
			ocidnssdk.CreateSteeringPolicyRequest{CreateSteeringPolicyDetails: d},
		)
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), tt.name)
	}

	att, err := e.client.CreateSteeringPolicyAttachment(t.Context(), ocidnssdk.CreateSteeringPolicyAttachmentRequest{
		SteeringPolicyId: p.Id, ZoneId: zone.Id, DomainName: new("www.lb.example"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "AAAA"}, att.Rtypes)
	assert.Equal(t, testCompartment, *att.CompartmentId)

	_, err = e.client.CreateSteeringPolicyAttachment(t.Context(), ocidnssdk.CreateSteeringPolicyAttachmentRequest{
		SteeringPolicyId: p.Id, ZoneId: zone.Id, DomainName: new("www.lb.example"),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
	assert.Equal(t, "NotAuthorizedOrResourceAlreadyExists", svcErr(t, err).GetCode())

	_, err = e.client.CreateSteeringPolicyAttachment(t.Context(), ocidnssdk.CreateSteeringPolicyAttachmentRequest{
		SteeringPolicyId: p.Id, ZoneId: zone.Id, DomainName: new("www.other.example"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "domain outside the zone")

	// Changing the answers refreshes the attachment's record types.
	_, err = e.client.UpdateSteeringPolicy(t.Context(), ocidnssdk.UpdateSteeringPolicyRequest{
		SteeringPolicyId: p.Id,
		Answers:          answers[:1], Ttl: new(120), DisplayName: new("failover2"),
	})
	require.NoError(t, err)

	gotAtt, err := e.client.GetSteeringPolicyAttachment(
		t.Context(),
		ocidnssdk.GetSteeringPolicyAttachmentRequest{SteeringPolicyAttachmentId: att.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"A"}, gotAtt.Rtypes)

	_, err = e.client.DeleteSteeringPolicy(t.Context(), ocidnssdk.DeleteSteeringPolicyRequest{SteeringPolicyId: p.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "attached policies cannot be deleted")

	lst, err := e.client.ListSteeringPolicyAttachments(t.Context(), ocidnssdk.ListSteeringPolicyAttachmentsRequest{
		CompartmentId: new(testCompartment), DomainContains: new("WWW"),
	})
	require.NoError(t, err)
	assert.Len(t, lst.Items, 1)
	assert.Equal(t, 1, *lst.OpcTotalItems)

	pols, err := e.client.ListSteeringPolicies(t.Context(), ocidnssdk.ListSteeringPoliciesRequest{
		CompartmentId: new(testCompartment), DisplayNameContains: new("OVER2"), Template: new("failover"),
	})
	require.NoError(t, err)
	require.Len(t, pols.Items, 1)
	assert.Equal(t, 120, *pols.Items[0].Ttl)

	_, err = e.client.ChangeSteeringPolicyCompartment(t.Context(), ocidnssdk.ChangeSteeringPolicyCompartmentRequest{
		SteeringPolicyId: p.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	// Moving the zone moves its attachments with it.
	_, err = e.client.ChangeZoneCompartment(t.Context(), ocidnssdk.ChangeZoneCompartmentRequest{
		ZoneId: zone.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	moved, err := e.client.GetSteeringPolicyAttachment(
		t.Context(),
		ocidnssdk.GetSteeringPolicyAttachmentRequest{SteeringPolicyAttachmentId: att.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *moved.CompartmentId)

	renamed, err := e.client.UpdateSteeringPolicyAttachment(
		t.Context(),
		ocidnssdk.UpdateSteeringPolicyAttachmentRequest{
			SteeringPolicyAttachmentId: att.Id, DisplayName: new("att"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "att", *renamed.DisplayName)

	// Deleting the zone deletes its attachments, which frees the policy.
	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: zone.Id})
	require.NoError(t, err)

	_, err = e.client.GetSteeringPolicyAttachment(
		t.Context(),
		ocidnssdk.GetSteeringPolicyAttachmentRequest{SteeringPolicyAttachmentId: att.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteSteeringPolicy(t.Context(), ocidnssdk.DeleteSteeringPolicyRequest{SteeringPolicyId: p.Id})
	require.NoError(t, err)
}

func TestSteeringAttachmentNeedsPublicZone(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.createView(t, "v")
	priv := e.createZone(
		t,
		"p.internal",
		func(d *ocidnssdk.CreateZoneDetails) { d.Scope, d.ViewId = ocidnssdk.ScopePrivate, v.Id },
	)

	pol, err := e.client.CreateSteeringPolicy(t.Context(), ocidnssdk.CreateSteeringPolicyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("p"),
		Template: ocidnssdk.CreateSteeringPolicyDetailsTemplateLoadBalance,
	})
	require.NoError(t, err)

	_, err = e.client.CreateSteeringPolicyAttachment(t.Context(), ocidnssdk.CreateSteeringPolicyAttachmentRequest{
		SteeringPolicyId: pol.Id, ZoneId: priv.Id, DomainName: new("a.p.internal"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestEtagsAdvanceOnEveryMutation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.createView(t, "etag")

	first, err := e.client.GetView(t.Context(), ocidnssdk.GetViewRequest{ViewId: v.Id})
	require.NoError(t, err)

	e.clock.Advance(time.Second)

	_, err = e.client.UpdateView(t.Context(), ocidnssdk.UpdateViewRequest{
		ViewId: v.Id, DisplayName: new("etag2"),
	})
	require.NoError(t, err)

	second, err := e.client.GetView(t.Context(), ocidnssdk.GetViewRequest{ViewId: v.Id})
	require.NoError(t, err)
	assert.NotEqual(t, *first.Etag, *second.Etag)
	assert.True(t, second.TimeUpdated.After(first.TimeUpdated.Time))

	_, err = e.client.UpdateView(t.Context(), ocidnssdk.UpdateViewRequest{
		ViewId: v.Id, IfMatch: first.Etag, DisplayName: new("etag3"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteView(t.Context(), ocidnssdk.DeleteViewRequest{ViewId: v.Id, IfMatch: first.Etag})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
}
