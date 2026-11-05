package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func (e *env) mkDrg(t *testing.T, name string) core.Drg {
	t.Helper()

	r, err := e.client.CreateDrg(t.Context(), core.CreateDrgRequest{
		CompartmentId: new(testComp), DisplayName: new(name)})
	require.NoError(t, err)

	return r.Drg
}

func (e *env) attachVcn(t *testing.T, drg core.Drg, vcn core.Vcn) core.DrgAttachment {
	t.Helper()

	r, err := e.client.CreateDrgAttachment(t.Context(), core.CreateDrgAttachmentRequest{
		DrgId: drg.Id, VcnId: vcn.Id})
	require.NoError(t, err)

	return r.DrgAttachment
}

func (e *env) listDrgRules(t *testing.T, tbl *string, typ core.ListDrgRouteRulesRouteTypeEnum) []core.DrgRouteRule {
	t.Helper()

	r, err := e.client.ListDrgRouteRules(
		t.Context(),
		core.ListDrgRouteRulesRequest{DrgRouteTableId: tbl, RouteType: typ},
	)
	require.NoError(t, err)

	return r.Items
}

func TestDrgLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "hub")

	assert.Equal(t, core.DrgLifecycleStateProvisioning, d.LifecycleState)
	require.NotNil(t, d.DefaultDrgRouteTables)
	assert.NotEmpty(t, *d.DefaultDrgRouteTables.Vcn)
	assert.Equal(t, *d.DefaultDrgRouteTables.IpsecTunnel, *d.DefaultDrgRouteTables.RemotePeeringConnection)
	assert.NotEqual(t, *d.DefaultDrgRouteTables.Vcn, *d.DefaultDrgRouteTables.VirtualCircuit)
	assert.NotEmpty(t, *d.DefaultExportDrgRouteDistributionId)

	e.settle()

	got, err := e.client.GetDrg(ctx, core.GetDrgRequest{DrgId: d.Id})
	require.NoError(t, err)
	assert.Equal(t, core.DrgLifecycleStateAvailable, got.LifecycleState)
	assert.NotEmpty(t, *got.Etag)

	tables, err := e.client.ListDrgRouteTables(ctx, core.ListDrgRouteTablesRequest{DrgId: d.Id})
	require.NoError(t, err)
	assert.Len(t, tables.Items, 2)

	dists, err := e.client.ListDrgRouteDistributions(ctx, core.ListDrgRouteDistributionsRequest{DrgId: d.Id})
	require.NoError(t, err)
	assert.Len(t, dists.Items, 3)

	upd, err := e.client.UpdateDrg(ctx, core.UpdateDrgRequest{DrgId: d.Id, IfMatch: got.Etag,
		DisplayName: new("renamed")})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)

	_, err = e.client.UpdateDrg(ctx, core.UpdateDrgRequest{DrgId: d.Id, IfMatch: new("stale"),
		DisplayName: new("x")})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	l, err := e.client.ListDrgs(ctx, core.ListDrgsRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)

	_, err = e.client.DeleteDrg(ctx, core.DeleteDrgRequest{DrgId: d.Id})
	require.NoError(t, err)
	e.settle()

	got, err = e.client.GetDrg(ctx, core.GetDrgRequest{DrgId: d.Id})
	require.NoError(t, err)
	assert.Equal(t, core.DrgLifecycleStateTerminated, got.LifecycleState)

	tables, err = e.client.ListDrgRouteTables(ctx, core.ListDrgRouteTablesRequest{DrgId: d.Id,
		LifecycleState: core.DrgRouteTableLifecycleStateAvailable})
	require.NoError(t, err)
	assert.Empty(t, tables.Items)
}

func TestDrgLimitAndCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	for i := range 5 {
		e.mkDrg(t, string(rune('a'+i)))
	}

	_, err := e.client.CreateDrg(ctx, core.CreateDrgRequest{
		CompartmentId: new(testComp)})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	_, err = e.client.CreateDrg(ctx, core.CreateDrgRequest{})
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())
}

func TestDrgChangeCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "move")
	other := testComp + "x"

	_, err := e.client.ChangeDrgCompartment(ctx, core.ChangeDrgCompartmentRequest{DrgId: d.Id,
		CompartmentId: new(other)})
	require.NoError(t, err)

	tbl, err := e.client.GetDrgRouteTable(
		ctx,
		core.GetDrgRouteTableRequest{DrgRouteTableId: d.DefaultDrgRouteTables.Vcn},
	)
	require.NoError(t, err)
	assert.Equal(t, other, *tbl.CompartmentId)

	dist, err := e.client.GetDrgRouteDistribution(ctx,
		core.GetDrgRouteDistributionRequest{DrgRouteDistributionId: d.DefaultExportDrgRouteDistributionId})
	require.NoError(t, err)
	assert.Equal(t, other, *dist.CompartmentId)
}

func TestDrgAttachmentCreate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "att")
	d2 := e.mkDrg(t, "att2")
	taken := e.mkVcn(t, "10.9.0.0/16", "taken")
	e.attachVcn(t, d, taken)

	other := e.mkDrg(t, "other")
	foreignTable, err := e.client.CreateDrgRouteTable(ctx, core.CreateDrgRouteTableRequest{
		DrgId: other.Id})
	require.NoError(t, err)

	tests := []struct {
		mod      func(*core.CreateDrgAttachmentDetails, core.Vcn)
		name     string
		cidr     string
		wantCode string
	}{
		{nil, "legacy vcn id", "10.10.0.0/16", ""},
		{func(c *core.CreateDrgAttachmentDetails, v core.Vcn) {
			c.VcnId = nil
			c.NetworkDetails = core.VcnDrgAttachmentNetworkCreateDetails{
				Id: v.Id, VcnRouteType: core.VcnDrgAttachmentNetworkDetailsVcnRouteTypeSubnetCidrs}
		}, "network details", "10.11.0.0/16", ""},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.DrgRouteTableId = d.DefaultDrgRouteTables.IpsecTunnel
		}, "explicit table", "10.12.0.0/16", ""},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.VcnId, c.DrgId = taken.Id, d2.Id
		}, "vcn already attached", "", "Conflict"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.VcnId = new("ocid1.vcn.oc1.iad.nope")
		}, "unknown vcn", "", "NotAuthorizedOrNotFound"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.DrgId = new("ocid1.drg.oc1.iad.nope")
		}, "unknown drg", "10.13.0.0/16", "NotAuthorizedOrNotFound"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.DrgId = nil
		}, "missing drg", "10.14.0.0/16", "MissingParameter"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.DrgRouteTableId = foreignTable.Id
		}, "foreign drg table", "10.15.0.0/16", "InvalidParameter"},
		{func(c *core.CreateDrgAttachmentDetails, v core.Vcn) {
			c.VcnId = nil
			c.NetworkDetails = core.VcnDrgAttachmentNetworkCreateDetails{Id: v.Id, VcnRouteType: "BOGUS"}
		}, "bad route type", "10.16.0.0/16", "InvalidParameter"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.DrgNatPolicyId = new("ocid1.drgnatpolicy.oc1.iad.x")
		}, "nat policy", "10.17.0.0/16", "NotAuthorizedOrNotFound"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) { c.DoesPreserveOriginalRoutesWithNat = new(true) }, "preserve routes without policy", "10.18.0.0/16", "InvalidParameter"},
		{func(c *core.CreateDrgAttachmentDetails, _ core.Vcn) {
			c.RouteTableId = taken.DefaultRouteTableId
		}, "vcn route table elsewhere", "10.19.0.0/16", "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := taken
			if tt.cidr != "" {
				v = e.mkVcn(t, tt.cidr, "v"+tt.cidr[3:5])
			}

			det := core.CreateDrgAttachmentDetails{DrgId: d.Id, VcnId: v.Id}
			if tt.mod != nil {
				tt.mod(&det, v)
			}

			r, cerr := e.client.CreateDrgAttachment(
				ctx,
				core.CreateDrgAttachmentRequest{CreateDrgAttachmentDetails: det},
			)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())

				return
			}

			require.NoError(t, cerr)
			assert.Equal(t, core.DrgAttachmentLifecycleStateAttaching, r.LifecycleState)
			assert.Equal(t, *v.Id, *r.VcnId)
			assert.Equal(t, testComp, *r.CompartmentId)
			assert.NotEmpty(t, *r.DrgRouteTableId)

			nd, ok := r.NetworkDetails.(core.VcnDrgAttachmentNetworkDetails)
			require.True(t, ok, "%T", r.NetworkDetails)
			assert.Equal(t, *v.Id, *nd.Id)
			assert.NotEmpty(t, nd.VcnRouteType)
		})
	}
}

func TestDrgAttachmentLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "life")
	v := e.mkVcn(t, "10.0.0.0/16", "life")
	a := e.attachVcn(t, d, v)

	assert.Equal(t, *d.DefaultDrgRouteTables.Vcn, *a.DrgRouteTableId)
	e.settle()

	got, err := e.client.GetDrgAttachment(ctx, core.GetDrgAttachmentRequest{DrgAttachmentId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, core.DrgAttachmentLifecycleStateAttached, got.LifecycleState)

	_, err = e.client.DeleteDrg(ctx, core.DeleteDrgRequest{DrgId: d.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.client.DeleteVcn(ctx, core.DeleteVcnRequest{VcnId: v.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	rt, err := e.client.CreateRouteTable(
		ctx,
		core.CreateRouteTableRequest{
			CompartmentId: new(testComp), VcnId: v.Id, RouteRules: []core.RouteRule{{
				Destination: new("172.16.0.0/12"), NetworkEntityId: d.Id}}},
	)
	require.NoError(t, err)

	_, err = e.client.DeleteDrgAttachment(ctx, core.DeleteDrgAttachmentRequest{DrgAttachmentId: a.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.client.UpdateRouteTable(ctx, core.UpdateRouteTableRequest{RtId: rt.Id,
		RouteRules: []core.RouteRule{}})
	require.NoError(t, err)

	_, err = e.client.DeleteDrgAttachment(ctx, core.DeleteDrgAttachmentRequest{DrgAttachmentId: a.Id})
	require.NoError(t, err)

	got, err = e.client.GetDrgAttachment(ctx, core.GetDrgAttachmentRequest{DrgAttachmentId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, core.DrgAttachmentLifecycleStateDetaching, got.LifecycleState)
	e.settle()

	got, err = e.client.GetDrgAttachment(ctx, core.GetDrgAttachmentRequest{DrgAttachmentId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, core.DrgAttachmentLifecycleStateDetached, got.LifecycleState)

	e.attachVcn(t, d, v)

	_, err = e.client.DeleteDrgAttachment(
		ctx,
		core.DeleteDrgAttachmentRequest{DrgAttachmentId: new("ocid1.drgattachment.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestListDrgAttachments(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d1, d2 := e.mkDrg(t, "d1"), e.mkDrg(t, "d2")
	v1, v2, v3 := e.mkVcn(t, "10.1.0.0/16", "l1"), e.mkVcn(t, "10.2.0.0/16", "l2"), e.mkVcn(t, "10.3.0.0/16", "l3")
	a1, a2, a3 := e.attachVcn(t, d1, v1), e.attachVcn(t, d1, v2), e.attachVcn(t, d2, v3)
	e.settle()

	_, err := e.client.DeleteDrgAttachment(ctx, core.DeleteDrgAttachmentRequest{DrgAttachmentId: a2.Id})
	require.NoError(t, err)
	e.settle()

	id := func(a core.DrgAttachment) string { return *a.Id }

	tests := []struct {
		name string
		req  core.ListDrgAttachmentsRequest
		want []string
	}{
		{"by drg", core.ListDrgAttachmentsRequest{DrgId: d1.Id}, []string{id(a2), id(a1)}},
		{"other drg", core.ListDrgAttachmentsRequest{DrgId: d2.Id}, []string{id(a3)}},
		{"by vcn", core.ListDrgAttachmentsRequest{VcnId: v1.Id}, []string{id(a1)}},
		{"by network", core.ListDrgAttachmentsRequest{NetworkId: v3.Id}, []string{id(a3)}},
		{
			"by state",
			core.ListDrgAttachmentsRequest{DrgId: d1.Id, LifecycleState: core.DrgAttachmentLifecycleStateDetached},
			[]string{id(a2)},
		},
		{
			"by type",
			core.ListDrgAttachmentsRequest{
				DrgId:          d1.Id,
				AttachmentType: core.ListDrgAttachmentsAttachmentTypeIpsecTunnel,
			},
			nil,
		},
		{
			"by vcn type",
			core.ListDrgAttachmentsRequest{DrgId: d2.Id, AttachmentType: core.ListDrgAttachmentsAttachmentTypeVcn},
			[]string{id(a3)},
		},
		{"by table", core.ListDrgAttachmentsRequest{DrgRouteTableId: d2.DefaultDrgRouteTables.Vcn}, []string{id(a3)}},
		{"all", core.ListDrgAttachmentsRequest{}, []string{id(a3), id(a2), id(a1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.req.CompartmentId = new(testComp)
			tt.req.SortOrder = core.ListDrgAttachmentsSortOrderDesc

			r, lerr := e.client.ListDrgAttachments(ctx, tt.req)
			require.NoError(t, lerr)

			got := make([]string, 0, len(r.Items))
			for _, a := range r.Items {
				got = append(got, *a.Id)
			}

			if tt.name == "by drg" || tt.name == "all" {
				assert.ElementsMatch(t, tt.want, got)

				return
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}

	all, err := e.client.GetAllDrgAttachments(ctx, core.GetAllDrgAttachmentsRequest{DrgId: d1.Id})
	require.NoError(t, err)
	require.Len(t, all.Items, 1)
	assert.Equal(t, *a1.Id, *all.Items[0].Id)

	none, err := e.client.GetAllDrgAttachments(ctx, core.GetAllDrgAttachmentsRequest{DrgId: d1.Id,
		AttachmentType: core.GetAllDrgAttachmentsAttachmentTypeVirtualCircuit})
	require.NoError(t, err)
	assert.Empty(t, none.Items)

	cross, err := e.client.GetAllDrgAttachments(
		ctx,
		core.GetAllDrgAttachmentsRequest{DrgId: d1.Id, IsCrossTenancy: new(true)},
	)
	require.NoError(t, err)
	assert.Empty(t, cross.Items)
}

func TestUpdateDrgAttachment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "upd")
	v := e.mkVcn(t, "10.0.0.0/16", "upd")
	a := e.attachVcn(t, d, v)
	tbl := d.DefaultDrgRouteTables.IpsecTunnel
	custom, err := e.client.CreateRouteTable(
		ctx,
		core.CreateRouteTableRequest{
			CompartmentId: new(testComp), VcnId: v.Id, RouteRules: []core.RouteRule{}},
	)
	require.NoError(t, err)

	tests := []struct {
		det      core.UpdateDrgAttachmentDetails
		name     string
		wantCode string
	}{
		{core.UpdateDrgAttachmentDetails{DisplayName: new("renamed"), DrgRouteTableId: tbl}, "rename and table", ""},
		{core.UpdateDrgAttachmentDetails{RouteTableId: custom.Id}, "vcn route table", ""},
		{core.UpdateDrgAttachmentDetails{NetworkDetails: core.VcnDrgAttachmentNetworkUpdateDetails{
			VcnRouteType: core.VcnDrgAttachmentNetworkDetailsVcnRouteTypeSubnetCidrs}}, "route type", ""},
		{core.UpdateDrgAttachmentDetails{
			ExportDrgRouteDistributionId: d.DefaultExportDrgRouteDistributionId}, "export on vcn", "InvalidParameter"},
		{core.UpdateDrgAttachmentDetails{DrgRouteTableId: new("ocid1.drgroutetable.oc1.iad.x")}, "unknown table", "NotAuthorizedOrNotFound"},
		{core.UpdateDrgAttachmentDetails{DrgNatPolicyId: new("ocid1.drgnatpolicy.oc1.iad.x")}, "nat policy", "NotAuthorizedOrNotFound"},
		{core.UpdateDrgAttachmentDetails{NetworkDetails: core.VcnDrgAttachmentNetworkUpdateDetails{
			VcnRouteType: "BOGUS"}}, "bad route type", "InvalidParameter"},
	}

	for _, tt := range tests {
		r, uerr := e.client.UpdateDrgAttachment(ctx, core.UpdateDrgAttachmentRequest{
			DrgAttachmentId: a.Id, UpdateDrgAttachmentDetails: tt.det})
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, uerr).GetCode())

			continue
		}

		require.NoError(t, uerr)
		assert.NotEmpty(t, *r.Etag)
	}

	got, err := e.client.GetDrgAttachment(ctx, core.GetDrgAttachmentRequest{DrgAttachmentId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *got.DisplayName)
	assert.Equal(t, *tbl, *got.DrgRouteTableId)
	assert.Equal(t, *custom.Id, *got.RouteTableId)

	nd, ok := got.NetworkDetails.(core.VcnDrgAttachmentNetworkDetails)
	require.True(t, ok)
	assert.Equal(t, core.VcnDrgAttachmentNetworkDetailsVcnRouteTypeSubnetCidrs, nd.VcnRouteType)
	assert.Equal(t, *custom.Id, *nd.RouteTableId)
}

func TestDrgRouteTables(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "rt")
	other := e.mkDrg(t, "rtother")

	imp, err := e.client.CreateDrgRouteDistribution(ctx, core.CreateDrgRouteDistributionRequest{
		DrgId: d.Id, DistributionType: core.CreateDrgRouteDistributionDetailsDistributionTypeImport})
	require.NoError(t, err)

	tests := []struct {
		det      core.CreateDrgRouteTableDetails
		name     string
		wantCode string
	}{
		{core.CreateDrgRouteTableDetails{DrgId: d.Id, DisplayName: new("p")}, "plain", ""},
		{core.CreateDrgRouteTableDetails{DrgId: d.Id, ImportDrgRouteDistributionId: imp.Id,
			IsEcmpEnabled: new(true)}, "with import and ecmp", ""},
		{core.CreateDrgRouteTableDetails{DrgId: d.Id,
			ImportDrgRouteDistributionId: d.DefaultExportDrgRouteDistributionId}, "export as import", "InvalidParameter"},
		{core.CreateDrgRouteTableDetails{DrgId: other.Id, ImportDrgRouteDistributionId: imp.Id}, "foreign import", "InvalidParameter"},
		{core.CreateDrgRouteTableDetails{}, "missing drg", "MissingParameter"},
		{core.CreateDrgRouteTableDetails{DrgId: new("ocid1.drg.oc1.iad.x")}, "unknown drg", "NotAuthorizedOrNotFound"},
	}

	for _, tt := range tests {
		r, cerr := e.client.CreateDrgRouteTable(
			ctx,
			core.CreateDrgRouteTableRequest{CreateDrgRouteTableDetails: tt.det},
		)
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())

			continue
		}

		require.NoError(t, cerr)
		assert.Equal(t, *d.Id, *r.DrgId)
		assert.Equal(t, tt.det.IsEcmpEnabled != nil, *r.IsEcmpEnabled)
	}

	tbl, err := e.client.CreateDrgRouteTable(ctx, core.CreateDrgRouteTableRequest{
		DrgId: d.Id, ImportDrgRouteDistributionId: imp.Id})
	require.NoError(t, err)

	upd, err := e.client.UpdateDrgRouteTable(ctx, core.UpdateDrgRouteTableRequest{DrgRouteTableId: tbl.Id,
		IsEcmpEnabled: new(true), DisplayName: new("n")})
	require.NoError(t, err)
	assert.True(t, *upd.IsEcmpEnabled)
	assert.Equal(t, *imp.Id, *upd.ImportDrgRouteDistributionId)

	cleared, err := e.client.RemoveImportDrgRouteDistribution(ctx,
		core.RemoveImportDrgRouteDistributionRequest{DrgRouteTableId: tbl.Id})
	require.NoError(t, err)
	assert.Nil(t, cleared.ImportDrgRouteDistributionId)

	scratch, err := e.client.CreateDrgRouteDistribution(ctx, core.CreateDrgRouteDistributionRequest{
		DrgId: d.Id, DistributionType: core.CreateDrgRouteDistributionDetailsDistributionTypeImport})
	require.NoError(t, err)

	_, err = e.client.DeleteDrgRouteDistribution(
		ctx,
		core.DeleteDrgRouteDistributionRequest{DrgRouteDistributionId: scratch.Id},
	)
	require.NoError(t, err)

	_, err = e.client.DeleteDrgRouteTable(
		ctx,
		core.DeleteDrgRouteTableRequest{DrgRouteTableId: d.DefaultDrgRouteTables.Vcn},
	)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.client.DeleteDrgRouteTable(ctx, core.DeleteDrgRouteTableRequest{DrgRouteTableId: tbl.Id})
	require.NoError(t, err)

	byDist, err := e.client.ListDrgRouteTables(ctx, core.ListDrgRouteTablesRequest{DrgId: d.Id,
		ImportDrgRouteDistributionId: d.DefaultExportDrgRouteDistributionId})
	require.NoError(t, err)
	assert.Empty(t, byDist.Items)
}

func TestDrgRouteRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "rules")
	v1, v2 := e.mkVcn(t, "10.1.0.0/16", "r1"), e.mkVcn(t, "10.2.0.0/16", "r2")
	a1, a2 := e.attachVcn(t, d, v1), e.attachVcn(t, d, v2)
	other := e.mkDrg(t, "otherrules")
	foreign := e.attachVcn(t, other, e.mkVcn(t, "10.3.0.0/16", "r3"))
	tbl, err := e.client.CreateDrgRouteTable(ctx, core.CreateDrgRouteTableRequest{
		DrgId: d.Id, DisplayName: new("static")})
	require.NoError(t, err)

	add := func(dest string, hop *string, typ core.AddDrgRouteRuleDetailsDestinationTypeEnum) (core.AddDrgRouteRulesResponse, error) {
		return e.client.AddDrgRouteRules(ctx, core.AddDrgRouteRulesRequest{DrgRouteTableId: tbl.Id,
			RouteRules: []core.AddDrgRouteRuleDetails{{
				Destination: new(dest), DestinationType: typ, NextHopDrgAttachmentId: hop}}})
	}

	tests := []struct {
		name     string
		dest     string
		hop      *string
		typ      core.AddDrgRouteRuleDetailsDestinationTypeEnum
		wantCode string
	}{
		{"ok", "192.168.0.0/16", a1.Id, core.AddDrgRouteRuleDetailsDestinationTypeCidrBlock, ""},
		{"duplicate", "192.168.0.0/16", a2.Id, core.AddDrgRouteRuleDetailsDestinationTypeCidrBlock, "Conflict"},
		{"bad cidr", "nonsense", a1.Id, core.AddDrgRouteRuleDetailsDestinationTypeCidrBlock, "InvalidParameter"},
		{
			"foreign hop",
			"172.16.0.0/12",
			foreign.Id,
			core.AddDrgRouteRuleDetailsDestinationTypeCidrBlock,
			"InvalidParameter",
		},
		{
			"unknown hop",
			"172.17.0.0/16",
			new("ocid1.drgattachment.oc1.iad.x"),
			core.AddDrgRouteRuleDetailsDestinationTypeCidrBlock,
			"NotAuthorizedOrNotFound",
		},
		{"bad type", "172.18.0.0/16", a1.Id, "SERVICE_CIDR_BLOCK", "InvalidParameter"},
	}

	for _, tt := range tests {
		r, aerr := add(tt.dest, tt.hop, tt.typ)
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, aerr).GetCode(), tt.name)

			continue
		}

		require.NoError(t, aerr, tt.name)
		require.Len(t, r.Items, 1)
		assert.Equal(t, core.DrgRouteRuleRouteTypeStatic, r.Items[0].RouteType)
		assert.Equal(t, core.DrgRouteRuleRouteProvenanceStatic, r.Items[0].RouteProvenance)
	}

	rules := e.listDrgRules(t, tbl.Id, "")
	require.Len(t, rules, 1)

	upd, err := e.client.UpdateDrgRouteRules(ctx, core.UpdateDrgRouteRulesRequest{DrgRouteTableId: tbl.Id,
		RouteRules: []core.UpdateDrgRouteRuleDetails{{
			Id: rules[0].Id, Destination: new("192.168.0.0/24"), NextHopDrgAttachmentId: a2.Id}}})
	require.NoError(t, err)
	assert.Equal(t, "192.168.0.0/24", *upd.Items[0].Destination)
	assert.Equal(t, *a2.Id, *upd.Items[0].NextHopDrgAttachmentId)

	_, err = e.client.UpdateDrgRouteRules(ctx, core.UpdateDrgRouteRulesRequest{DrgRouteTableId: tbl.Id,
		RouteRules: []core.UpdateDrgRouteRuleDetails{{
			Id: new("ocid1.drgrouterule.oc1.iad.x")}}})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteDrgAttachment(ctx, core.DeleteDrgAttachmentRequest{DrgAttachmentId: a2.Id})
	require.NoError(t, err)

	_, err = e.client.RemoveDrgRouteRules(ctx, core.RemoveDrgRouteRulesRequest{
		DrgRouteTableId: tbl.Id,
		RouteRuleIds:    []string{"ocid1.drgrouterule.oc1.iad.x"},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.RemoveDrgRouteRules(ctx, core.RemoveDrgRouteRulesRequest{DrgRouteTableId: tbl.Id,
		RouteRuleIds: []string{*rules[0].Id}})
	require.NoError(t, err)
	assert.Empty(t, e.listDrgRules(t, tbl.Id, ""))
}

func TestDrgDynamicRoutes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "dyn")
	v1, v2 := e.mkVcn(t, "10.1.0.0/16", "d1"), e.mkVcn(t, "10.2.0.0/16", "d2")
	e.mkSubnet(t, v2, "10.2.5.0/24")
	a1 := e.attachVcn(t, d, v1)
	a2 := e.attachVcn(t, d, v2)

	_, err := e.client.UpdateDrgAttachment(ctx, core.UpdateDrgAttachmentRequest{
		DrgAttachmentId: a2.Id,
		NetworkDetails: core.VcnDrgAttachmentNetworkUpdateDetails{
			VcnRouteType: core.VcnDrgAttachmentNetworkDetailsVcnRouteTypeSubnetCidrs},
	})
	require.NoError(t, err)

	vcnTable := e.listDrgRules(t, d.DefaultDrgRouteTables.Vcn, core.ListDrgRouteRulesRouteTypeDynamic)
	got := map[string]string{}

	for _, r := range vcnTable {
		got[*r.Destination] = *r.NextHopDrgAttachmentId

		assert.Equal(t, core.DrgRouteRuleRouteProvenanceVcn, r.RouteProvenance)
		assert.Equal(t, core.DrgRouteRuleRouteTypeDynamic, r.RouteType)
		assert.False(t, *r.IsConflict)
		assert.NotEmpty(t, *r.Id)
	}

	assert.Equal(t, map[string]string{"10.1.0.0/16": *a1.Id, "10.2.5.0/24": *a2.Id}, got)

	assert.Empty(t, e.listDrgRules(t, d.DefaultDrgRouteTables.Vcn, core.ListDrgRouteRulesRouteTypeStatic))

	otherTable := e.listDrgRules(t, d.DefaultDrgRouteTables.IpsecTunnel, "")
	assert.Len(t, otherTable, 2)

	_, err = e.client.RemoveImportDrgRouteDistribution(ctx,
		core.RemoveImportDrgRouteDistributionRequest{DrgRouteTableId: d.DefaultDrgRouteTables.Vcn})
	require.NoError(t, err)
	assert.Empty(t, e.listDrgRules(t, d.DefaultDrgRouteTables.Vcn, ""))

	_, err = e.client.DeleteDrgAttachment(ctx, core.DeleteDrgAttachmentRequest{DrgAttachmentId: a1.Id})
	require.NoError(t, err)
	e.settle()
	assert.Len(t, e.listDrgRules(t, d.DefaultDrgRouteTables.IpsecTunnel, ""), 1)
}

func TestDrgEcmpConflict(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "ecmp")
	e.attachVcn(t, d, e.mkVcn(t, "10.1.0.0/16", "e1"))
	e.attachVcn(t, d, e.mkVcn(t, "10.2.0.0/16", "e2"))

	tbl := d.DefaultDrgRouteTables.IpsecTunnel

	for _, r := range e.listDrgRules(t, tbl, "") {
		assert.False(t, *r.IsConflict)
	}

	// Overlapping destinations from two attachments cannot occur between VCNs of one tenancy; ECMP stays off.
	upd, err := e.client.UpdateDrgRouteTable(ctx, core.UpdateDrgRouteTableRequest{DrgRouteTableId: tbl,
		IsEcmpEnabled: new(true)})
	require.NoError(t, err)
	assert.True(t, *upd.IsEcmpEnabled)
}

func TestDrgDistributions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "dist")
	a := e.attachVcn(t, d, e.mkVcn(t, "10.1.0.0/16", "dist"))

	_, err := e.client.CreateDrgRouteDistribution(ctx, core.CreateDrgRouteDistributionRequest{
		DistributionType: core.CreateDrgRouteDistributionDetailsDistributionTypeImport})
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())

	dist, err := e.client.CreateDrgRouteDistribution(ctx, core.CreateDrgRouteDistributionRequest{
		DrgId: d.Id, DistributionType: core.CreateDrgRouteDistributionDetailsDistributionTypeImport})
	require.NoError(t, err)
	assert.Equal(t, core.DrgRouteDistributionDistributionTypeImport, dist.DistributionType)

	byType := core.DrgAttachmentTypeDrgRouteDistributionMatchCriteria{
		AttachmentType: core.DrgAttachmentTypeDrgRouteDistributionMatchCriteriaAttachmentTypeVcn}
	byID := core.DrgAttachmentIdDrgRouteDistributionMatchCriteria{DrgAttachmentId: a.Id}
	accept := core.AddDrgRouteDistributionStatementDetailsActionAccept

	stmt := func(pri int, mc ...core.DrgRouteDistributionMatchCriteria) core.AddDrgRouteDistributionStatementDetails {
		return core.AddDrgRouteDistributionStatementDetails{MatchCriteria: mc, Action: accept, Priority: new(pri)}
	}

	addReq := func(s ...core.AddDrgRouteDistributionStatementDetails) core.AddDrgRouteDistributionStatementsRequest {
		return core.AddDrgRouteDistributionStatementsRequest{DrgRouteDistributionId: dist.Id,
			Statements: s}
	}

	tests := []struct {
		name     string
		wantCode string
		stmts    []core.AddDrgRouteDistributionStatementDetails
	}{
		{"by type", "", []core.AddDrgRouteDistributionStatementDetails{stmt(10, byType)}},
		{"by id and match all", "", []core.AddDrgRouteDistributionStatementDetails{stmt(20, byID),
			stmt(30, core.DrgAttachmentMatchAllDrgRouteDistributionMatchCriteria{})}},
		{"duplicate priority", "Conflict", []core.AddDrgRouteDistributionStatementDetails{stmt(10, byType)}},
		{
			"duplicate within request",
			"Conflict",
			[]core.AddDrgRouteDistributionStatementDetails{stmt(40, byType), stmt(40, byType)},
		},
		{"priority range", "InvalidParameter", []core.AddDrgRouteDistributionStatementDetails{stmt(70000, byType)}},
		{
			"unknown attachment",
			"NotAuthorizedOrNotFound",
			[]core.AddDrgRouteDistributionStatementDetails{
				stmt(
					50,
					core.DrgAttachmentIdDrgRouteDistributionMatchCriteria{
						DrgAttachmentId: new("ocid1.drgattachment.oc1.iad.x"),
					},
				),
			},
		},
		{"bad attachment type", "InvalidParameter", []core.AddDrgRouteDistributionStatementDetails{stmt(60,
			core.DrgAttachmentTypeDrgRouteDistributionMatchCriteria{AttachmentType: "BOGUS"})}},
	}

	for _, tt := range tests {
		r, aerr := e.client.AddDrgRouteDistributionStatements(ctx, addReq(tt.stmts...))
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, aerr).GetCode(), tt.name)

			continue
		}

		require.NoError(t, aerr, tt.name)
		require.Len(t, r.Items, len(tt.stmts))
	}

	list, err := e.client.ListDrgRouteDistributionStatements(ctx,
		core.ListDrgRouteDistributionStatementsRequest{DrgRouteDistributionId: dist.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 3)
	assert.Equal(t, []int{10, 20, 30}, []int{*list.Items[0].Priority, *list.Items[1].Priority, *list.Items[2].Priority})
	assert.Len(t, list.Items[0].MatchCriteria, 1)

	first, ok := list.Items[0].MatchCriteria[0].(core.DrgAttachmentTypeDrgRouteDistributionMatchCriteria)
	require.True(t, ok, "%T", list.Items[0].MatchCriteria[0])
	assert.Equal(t, core.DrgAttachmentTypeDrgRouteDistributionMatchCriteriaAttachmentTypeVcn, first.AttachmentType)

	upd, err := e.client.UpdateDrgRouteDistributionStatements(ctx, core.UpdateDrgRouteDistributionStatementsRequest{
		DrgRouteDistributionId: dist.Id,
		Statements: []core.UpdateDrgRouteDistributionStatementDetails{
			{Id: list.Items[0].Id, Priority: new(5)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 5, *upd.Items[0].Priority)

	_, err = e.client.UpdateDrgRouteDistributionStatements(ctx, core.UpdateDrgRouteDistributionStatementsRequest{
		DrgRouteDistributionId: dist.Id,
		Statements: []core.UpdateDrgRouteDistributionStatementDetails{
			{Id: list.Items[0].Id, Priority: new(20)},
		},
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.client.RemoveDrgRouteDistributionStatements(ctx, core.RemoveDrgRouteDistributionStatementsRequest{
		DrgRouteDistributionId: dist.Id,
		StatementIds:           []string{"ocid1.x.oc1.iad.nope"},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.RemoveDrgRouteDistributionStatements(ctx, core.RemoveDrgRouteDistributionStatementsRequest{
		DrgRouteDistributionId: dist.Id,
		StatementIds:           []string{*list.Items[0].Id},
	})
	require.NoError(t, err)

	rename, err := e.client.UpdateDrgRouteDistribution(
		ctx,
		core.UpdateDrgRouteDistributionRequest{DrgRouteDistributionId: dist.Id,
			DisplayName: new("x")},
	)
	require.NoError(t, err)
	assert.Equal(t, "x", *rename.DisplayName)
}

func TestDrgDistributionRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "route")
	a1 := e.attachVcn(t, d, e.mkVcn(t, "10.1.0.0/16", "x1"))
	e.attachVcn(t, d, e.mkVcn(t, "10.2.0.0/16", "x2"))

	dist, err := e.client.CreateDrgRouteDistribution(ctx, core.CreateDrgRouteDistributionRequest{
		DrgId: d.Id, DistributionType: core.CreateDrgRouteDistributionDetailsDistributionTypeImport})
	require.NoError(t, err)

	_, err = e.client.AddDrgRouteDistributionStatements(ctx, core.AddDrgRouteDistributionStatementsRequest{
		DrgRouteDistributionId: dist.Id,
		Statements: []core.AddDrgRouteDistributionStatementDetails{{
			Action: core.AddDrgRouteDistributionStatementDetailsActionAccept, Priority: new(1),
			MatchCriteria: []core.DrgRouteDistributionMatchCriteria{
				core.DrgAttachmentIdDrgRouteDistributionMatchCriteria{DrgAttachmentId: a1.Id}}}},
	})
	require.NoError(t, err)

	tbl, err := e.client.CreateDrgRouteTable(
		ctx,
		core.CreateDrgRouteTableRequest{
			DrgId: d.Id, ImportDrgRouteDistributionId: dist.Id},
	)
	require.NoError(t, err)

	rules := e.listDrgRules(t, tbl.Id, "")
	require.Len(t, rules, 1)
	assert.Equal(t, "10.1.0.0/16", *rules[0].Destination)

	_, err = e.client.DeleteDrgRouteDistribution(
		ctx,
		core.DeleteDrgRouteDistributionRequest{DrgRouteDistributionId: dist.Id},
	)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.client.DeleteDrgRouteDistribution(ctx, core.DeleteDrgRouteDistributionRequest{
		DrgRouteDistributionId: d.DefaultExportDrgRouteDistributionId})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())
}

func TestRemoveExportDrgRouteDistribution(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "export")
	id, err := e.backend.AttachDrgNetwork(
		oci.DrgNetworkIn{DrgID: *d.Id, Type: "IPSEC_TUNNEL", NetworkID: "ocid1.ipsecconnection.oc1.iad.x"},
	)
	require.NoError(t, err)

	got, err := e.client.GetDrgAttachment(ctx, core.GetDrgAttachmentRequest{DrgAttachmentId: &id})
	require.NoError(t, err)
	assert.Equal(t, *d.DefaultExportDrgRouteDistributionId, *got.ExportDrgRouteDistributionId)
	assert.Equal(t, *d.DefaultDrgRouteTables.IpsecTunnel, *got.DrgRouteTableId)
	nd, ok := got.NetworkDetails.(core.IpsecTunnelDrgAttachmentNetworkDetails)
	require.True(t, ok, "%T", got.NetworkDetails)
	assert.Equal(t, "ocid1.ipsecconnection.oc1.iad.x", *nd.Id)

	cleared, err := e.client.RemoveExportDrgRouteDistribution(
		ctx,
		core.RemoveExportDrgRouteDistributionRequest{DrgAttachmentId: &id},
	)
	require.NoError(t, err)
	assert.Nil(t, cleared.ExportDrgRouteDistributionId)

	_, err = e.client.RemoveExportDrgRouteDistribution(ctx, core.RemoveExportDrgRouteDistributionRequest{
		DrgAttachmentId: new("ocid1.drgattachment.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestDrgRedundancyStatus(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		name  string
		want  core.DrgRedundancyStatusStatusEnum
		types []string
	}{
		{"none", core.DrgRedundancyStatusStatusNotRedundantNoConnection, nil},
		{"single ipsec", core.DrgRedundancyStatusStatusNotRedundantSingleIpsec, []string{"IPSEC_TUNNEL"}},
		{"single vc", core.DrgRedundancyStatusStatusNotRedundantSingleVirtualcircuit, []string{"VIRTUAL_CIRCUIT"}},
		{"multiple ipsec", core.DrgRedundancyStatusStatusNotRedundantMultipleIpsecs, []string{"IPSEC_TUNNEL", "IPSEC_TUNNEL"}},
		{"multiple vc", core.DrgRedundancyStatusStatusNotRedundantMultipleVirtualcircuits, []string{"VIRTUAL_CIRCUIT", "VIRTUAL_CIRCUIT"}},
		{"mixed", core.DrgRedundancyStatusStatusNotRedundantMixConnections, []string{"IPSEC_TUNNEL", "VIRTUAL_CIRCUIT"}},
		{"rpc only", core.DrgRedundancyStatusStatusNotRedundantNoConnection, []string{"REMOTE_PEERING_CONNECTION"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			d := e.mkDrg(t, "red")
			e.settle()

			for _, typ := range tt.types {
				_, err := e.backend.AttachDrgNetwork(
					oci.DrgNetworkIn{DrgID: *d.Id, Type: typ, NetworkID: "ocid1.x.oc1.iad.y"},
				)
				require.NoError(t, err)
			}

			r, err := e.client.GetDrgRedundancyStatus(ctx, core.GetDrgRedundancyStatusRequest{DrgId: d.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.want, r.Status)
			assert.Equal(t, *d.Id, *r.Id)
		})
	}

	e := newEnv(t, false)

	_, err := e.backend.AttachDrgNetwork(oci.DrgNetworkIn{DrgID: "ocid1.drg.oc1.iad.nope", Type: "IPSEC_TUNNEL"})
	require.Error(t, err)

	d := e.mkDrg(t, "bad")
	_, err = e.backend.AttachDrgNetwork(oci.DrgNetworkIn{DrgID: *d.Id, Type: "VCN"})
	require.Error(t, err)

	_, err = e.client.GetDrgRedundancyStatus(
		ctx,
		core.GetDrgRedundancyStatusRequest{DrgId: new("ocid1.drg.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestDrgUpgrade(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "upg")
	e.attachVcn(t, d, e.mkVcn(t, "10.1.0.0/16", "upg"))

	st, err := e.client.GetUpgradeStatus(ctx, core.GetUpgradeStatusRequest{DrgId: d.Id})
	require.NoError(t, err)
	assert.Equal(t, core.UpgradeStatusStatusUpgraded, st.Status)
	assert.Equal(t, "1", *st.UpgradedConnections)
	assert.Equal(t, *d.Id, *st.DrgId)

	_, err = e.client.UpgradeDrg(ctx, core.UpgradeDrgRequest{DrgId: d.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = e.client.UpgradeDrg(ctx, core.UpgradeDrgRequest{DrgId: new("ocid1.drg.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestDrgSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "snap")
	v := e.mkVcn(t, "10.1.0.0/16", "snap")
	a := e.attachVcn(t, d, v)

	tbl, err := e.client.CreateDrgRouteTable(ctx, core.CreateDrgRouteTableRequest{
		DrgId: d.Id})
	require.NoError(t, err)

	_, err = e.client.AddDrgRouteRules(ctx, core.AddDrgRouteRulesRequest{DrgRouteTableId: tbl.Id,
		RouteRules: []core.AddDrgRouteRuleDetails{{
			Destination: new("192.168.0.0/16"), DestinationType: core.AddDrgRouteRuleDetailsDestinationTypeCidrBlock,
			NextHopDrgAttachmentId: a.Id}}})
	require.NoError(t, err)

	data := e.backend.Snapshot(ctx)

	e2 := newEnv(t, false)
	require.NoError(t, e2.backend.Restore(ctx, data))

	got, err := e2.client.GetDrgAttachment(ctx, core.GetDrgAttachmentRequest{DrgAttachmentId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, *d.Id, *got.DrgId)

	rules, err := e2.client.ListDrgRouteRules(ctx, core.ListDrgRouteRulesRequest{DrgRouteTableId: tbl.Id})
	require.NoError(t, err)
	require.Len(t, rules.Items, 1)

	dg, err := e2.client.GetDrg(ctx, core.GetDrgRequest{DrgId: d.Id})
	require.NoError(t, err)
	assert.Equal(t, *d.DefaultDrgRouteTables.Vcn, *dg.DefaultDrgRouteTables.Vcn)
}
