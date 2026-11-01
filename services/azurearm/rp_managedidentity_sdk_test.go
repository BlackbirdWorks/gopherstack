package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedIdentitySDK_Lifecycle(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	c := s.msi.NewUserAssignedIdentitiesClient()

	tests := []struct {
		name     string
		ident    string
		loc      *string
		wantCode string
	}{
		{name: "valid", ident: "id-app", loc: new("westus")},
		{name: "too short", ident: "ab", loc: new("westus"), wantCode: "InvalidResourceName"},
		{name: "bad chars", ident: "id.app", loc: new("westus"), wantCode: "InvalidResourceName"},
		{name: "no location", ident: "id-nol", wantCode: "LocationRequired"},
	}

	for _, tt := range tests {
		got, err := c.CreateOrUpdate(ctx, netRG, tt.ident, armmsi.Identity{Location: tt.loc}, nil)
		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.NotEmpty(t, *got.Properties.PrincipalID)
		assert.NotEmpty(t, *got.Properties.ClientID)
		assert.NotEmpty(t, *got.Properties.TenantID)
	}

	first, err := c.Get(ctx, netRG, "id-app", nil)
	require.NoError(t, err)
	assert.NotEqual(t, *first.Properties.PrincipalID, *first.Properties.ClientID)

	upd, err := c.CreateOrUpdate(ctx, netRG, "id-app", armmsi.Identity{
		Location: new("westus"), Tags: map[string]*string{"k": new("v")},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, *first.Properties.PrincipalID, *upd.Properties.PrincipalID, "ids are generated once")
	assert.Equal(t, *first.Properties.ClientID, *upd.Properties.ClientID)

	patched, err := c.Update(ctx, netRG, "id-app", armmsi.IdentityUpdate{Tags: map[string]*string{"z": new("1")}},
		nil)
	require.NoError(t, err)
	assert.Equal(t, map[string]*string{"z": new("1")}, patched.Tags)
	assert.Equal(t, *first.Properties.ClientID, *patched.Properties.ClientID)

	_, err = c.Update(ctx, netRG, "ghost-id", armmsi.IdentityUpdate{}, nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	s.rawDo(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/rg-two", `{"location":"westus"}`)
	_, err = c.CreateOrUpdate(ctx, "rg-two", "id-two", armmsi.Identity{Location: new("westus")}, nil)
	require.NoError(t, err)

	var inRG, inSub int

	for pg := c.NewListByResourceGroupPager(netRG, nil); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		inRG += len(page.Value)
	}

	for pg := c.NewListBySubscriptionPager(nil); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		inSub += len(page.Value)
	}

	assert.Equal(t, 1, inRG)
	assert.Equal(t, 2, inSub)

	_, err = c.Delete(ctx, netRG, "id-app", nil)
	require.NoError(t, err)

	_, err = c.Get(ctx, netRG, "id-app", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	_, err = c.Delete(ctx, netRG, "id-app", nil)
	require.NoError(t, err, "deleting a missing identity is a no-op")

	resp := s.rawDo(t, http.MethodDelete, "/subscriptions/"+aciSub+"/resourcegroups/rg-two", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	_, err = c.Get(ctx, "rg-two", "id-two", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestManagedIdentitySDK_FederatedCredentials(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	_, err := s.msi.NewUserAssignedIdentitiesClient().CreateOrUpdate(ctx, netRG, "id-app",
		armmsi.Identity{Location: new("westus")}, nil)
	require.NoError(t, err)

	fc := s.msi.NewFederatedIdentityCredentialsClient()
	cred := func(issuer, subject string, aud ...string) armmsi.FederatedIdentityCredential {
		a := make([]*string, 0, len(aud))
		for _, x := range aud {
			a = append(a, new(x))
		}

		return armmsi.FederatedIdentityCredential{Properties: &armmsi.FederatedIdentityCredentialProperties{
			Issuer: new(issuer), Subject: new(subject), Audiences: a,
		}}
	}

	const aad = "api://AzureADTokenExchange"

	tests := []struct {
		name     string
		cred     armmsi.FederatedIdentityCredential
		fname    string
		wantCode string
	}{
		{
			name:  "ok",
			fname: "gh-main",
			cred:  cred("https://token.actions.githubusercontent.com", "repo:o/r:ref:refs/heads/main", aad),
		},
		{
			name:  "ok2",
			fname: "gh-pr",
			cred:  cred("https://token.actions.githubusercontent.com", "repo:o/r:pull_request", aad),
		},
		{name: "same issuer subject", fname: "gh-dup", wantCode: "Conflict",
			cred: cred("https://token.actions.githubusercontent.com", "repo:o/r:pull_request", aad)},
		{
			name:     "http issuer",
			fname:    "gh-http",
			wantCode: "InvalidRequestContent",
			cred:     cred("http://x.example", "s", aad),
		},
		{
			name:     "no subject",
			fname:    "gh-nosub",
			wantCode: "InvalidRequestContent",
			cred:     cred("https://x.example", "", aad),
		},
		{
			name:     "two audiences",
			fname:    "gh-aud",
			wantCode: "InvalidRequestContent",
			cred:     cred("https://x.example", "s", aad, "b"),
		},
		{name: "bad name", fname: "x", wantCode: "InvalidResourceName", cred: cred("https://x.example", "s", aad)},
	}

	for _, tt := range tests {
		got, cerr := fc.CreateOrUpdate(ctx, netRG, "id-app", tt.fname, tt.cred, nil)
		if tt.wantCode != "" {
			require.Error(t, cerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(cerr), tt.name)

			continue
		}

		require.NoError(t, cerr, tt.name)
		assert.Contains(t, *got.ID, "/userAssignedIdentities/id-app/federatedIdentityCredentials/"+tt.fname)
	}

	_, err = fc.CreateOrUpdate(ctx, netRG, "ghost-id", "gh-main", cred("https://x.example", "s", aad), nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	var names []string

	for pg := fc.NewListPager(netRG, "id-app",
		&armmsi.FederatedIdentityCredentialsClientListOptions{Top: new(int32(1))}); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)
		require.LessOrEqual(t, len(page.Value), 1)

		for _, v := range page.Value {
			names = append(names, *v.Name)
		}
	}

	assert.Equal(t, []string{"gh-main", "gh-pr"}, names)

	_, err = fc.Delete(ctx, netRG, "id-app", "gh-main", nil)
	require.NoError(t, err)

	_, err = fc.Get(ctx, netRG, "id-app", "gh-main", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	_, err = s.msi.NewUserAssignedIdentitiesClient().Delete(ctx, netRG, "id-app", nil)
	require.NoError(t, err)

	_, err = fc.Get(ctx, netRG, "id-app", "gh-pr", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err), "credentials go with the identity")
}

func TestPrivateDNSSDK_ZonesRecordsLinks(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	zc := s.dns.NewPrivateZonesClient()

	_, err := zc.BeginCreateOrUpdate(ctx, netRG, "single", armprivatedns.PrivateZone{Location: new("global")}, nil)
	assert.Equal(t, "InvalidResourceName", sdkErrCode(err))

	zp, err := zc.BeginCreateOrUpdate(
		ctx,
		netRG,
		"contoso.internal",
		armprivatedns.PrivateZone{Location: new("global")},
		nil,
	)
	require.NoError(t, err)
	zone, err := zp.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.EqualValues(t, 1, *zone.Properties.NumberOfRecordSets, "the SOA set exists from creation")
	assert.EqualValues(t, 25000, *zone.Properties.MaxNumberOfRecordSets)

	_, err = zc.BeginCreateOrUpdate(ctx, netRG, "contoso.internal",
		armprivatedns.PrivateZone{Location: new("global")},
		&armprivatedns.PrivateZonesClientBeginCreateOrUpdateOptions{IfNoneMatch: new("*")})
	assert.Equal(t, "PreconditionFailed", sdkErrCode(err))

	_, err = zc.BeginCreateOrUpdate(ctx, netRG, "contoso.internal",
		armprivatedns.PrivateZone{Location: new("global")},
		&armprivatedns.PrivateZonesClientBeginCreateOrUpdateOptions{IfMatch: new("stale")})
	assert.Equal(t, "PreconditionFailed", sdkErrCode(err))

	rc := s.dns.NewRecordSetsClient()
	ttl := new(int64(300))

	tests := []struct {
		name     string
		rtype    armprivatedns.RecordType
		rname    string
		set      armprivatedns.RecordSet
		wantCode string
	}{
		{
			name:  "a",
			rtype: armprivatedns.RecordTypeA,
			rname: "www",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: ttl, ARecords: []*armprivatedns.ARecord{{IPv4Address: new("10.0.0.4")}},
			}},
		},
		{
			name:     "bad a",
			rtype:    armprivatedns.RecordTypeA,
			rname:    "bad",
			wantCode: "BadRequest",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: ttl, ARecords: []*armprivatedns.ARecord{{IPv4Address: new("nope")}},
			}},
		},
		{
			name:  "txt",
			rtype: armprivatedns.RecordTypeTXT,
			rname: "info",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: ttl, TxtRecords: []*armprivatedns.TxtRecord{{Value: []*string{new("hello")}}},
			}},
		},
		{
			name:  "cname",
			rtype: armprivatedns.RecordTypeCNAME,
			rname: "alias",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: ttl, CnameRecord: &armprivatedns.CnameRecord{Cname: new("www.contoso.internal")},
			}},
		},
		{name: "cname clashes with a", rtype: armprivatedns.RecordTypeCNAME, rname: "www", wantCode: "Conflict",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: ttl, CnameRecord: &armprivatedns.CnameRecord{Cname: new("x.contoso.internal")},
			}}},
		{name: "cname at apex", rtype: armprivatedns.RecordTypeCNAME, rname: "@", wantCode: "BadRequest",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				TTL: ttl, CnameRecord: &armprivatedns.CnameRecord{Cname: new("x.contoso.internal")},
			}}},
		{
			name:     "no ttl",
			rtype:    armprivatedns.RecordTypeA,
			rname:    "nottl",
			wantCode: "BadRequest",
			set: armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
				ARecords: []*armprivatedns.ARecord{{IPv4Address: new("10.0.0.5")}},
			}},
		},
	}

	for _, tt := range tests {
		got, rerr := rc.CreateOrUpdate(ctx, netRG, "contoso.internal", tt.rtype, tt.rname, tt.set, nil)
		if tt.wantCode != "" {
			require.Error(t, rerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(rerr), tt.name)

			continue
		}

		require.NoError(t, rerr, tt.name)
		assert.Equal(t, tt.rname+".contoso.internal.", *got.Properties.Fqdn, tt.name)
	}

	var all []string

	for pg := rc.NewListPager(netRG, "contoso.internal", nil); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		for _, v := range page.Value {
			all = append(all, *v.Type+"/"+*v.Name)
		}
	}

	assert.ElementsMatch(t, []string{
		"Microsoft.Network/privateDnsZones/A/www", "Microsoft.Network/privateDnsZones/TXT/info",
		"Microsoft.Network/privateDnsZones/CNAME/alias", "Microsoft.Network/privateDnsZones/SOA/@",
	}, all)

	soa, err := rc.Get(ctx, netRG, "contoso.internal", armprivatedns.RecordTypeSOA, "@", nil)
	require.NoError(t, err)
	assert.Equal(t, "azureprivatedns.net", *soa.Properties.SoaRecord.Host)

	_, err = rc.Delete(ctx, netRG, "contoso.internal", armprivatedns.RecordTypeSOA, "@", nil)
	assert.Equal(t, "BadRequest", sdkErrCode(err))

	_, err = rc.Delete(ctx, netRG, "contoso.internal", armprivatedns.RecordTypeA, "www", nil)
	require.NoError(t, err)

	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, nil)
	lc := s.dns.NewVirtualNetworkLinksClient()

	_, err = lc.BeginCreateOrUpdate(ctx, netRG, "contoso.internal", "ghost", armprivatedns.VirtualNetworkLink{
		Location: new("global"),
		Properties: &armprivatedns.VirtualNetworkLinkProperties{
			VirtualNetwork: &armprivatedns.SubResource{ID: new(*v.ID + "-ghost")},
		},
	}, nil)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	lp, err := lc.BeginCreateOrUpdate(ctx, netRG, "contoso.internal", "link1", armprivatedns.VirtualNetworkLink{
		Location: new("global"),
		Properties: &armprivatedns.VirtualNetworkLinkProperties{
			VirtualNetwork: &armprivatedns.SubResource{ID: v.ID}, RegistrationEnabled: new(true),
		},
	}, nil)
	require.NoError(t, err)
	link, err := lp.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, armprivatedns.VirtualNetworkLinkStateCompleted, *link.Properties.VirtualNetworkLinkState)

	gz, err := zc.Get(ctx, netRG, "contoso.internal", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, *gz.Properties.NumberOfVirtualNetworkLinks)
	assert.EqualValues(t, 1, *gz.Properties.NumberOfVirtualNetworkLinksWithRegistration)
	assert.EqualValues(t, 3, *gz.Properties.NumberOfRecordSets)

	// deleting the virtual network clears its links
	vd, err := s.net.NewVirtualNetworksClient().BeginDelete(ctx, netRG, "vnet1", nil)
	require.NoError(t, err)
	_, err = vd.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	gz, err = zc.Get(ctx, netRG, "contoso.internal", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, *gz.Properties.NumberOfVirtualNetworkLinks)

	zd, err := zc.BeginDelete(ctx, netRG, "contoso.internal", nil)
	require.NoError(t, err)
	_, err = zd.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = zc.Get(ctx, netRG, "contoso.internal", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}
