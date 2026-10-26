package ocicore_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) services(t *testing.T) []core.Service {
	t.Helper()

	r, err := e.client.ListServices(t.Context(), core.ListServicesRequest{})
	require.NoError(t, err)

	return r.Items
}

func TestServices(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	svcs := e.services(t)
	require.Len(t, svcs, 2)

	assert.Equal(t, "All IAD Services In Oracle Services Network", *svcs[0].Name)
	assert.Equal(t, "all-iad-services-in-oracle-services-network", *svcs[0].CidrBlock)

	got, err := e.client.GetService(t.Context(), core.GetServiceRequest{ServiceId: svcs[1].Id})
	require.NoError(t, err)
	assert.Equal(t, "oci-iad-objectstorage", *got.CidrBlock)

	_, err = e.client.GetService(t.Context(), core.GetServiceRequest{ServiceId: new("ocid1.service.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	paged, err := e.client.ListServices(t.Context(), core.ListServicesRequest{Limit: new(1)})
	require.NoError(t, err)
	assert.Len(t, paged.Items, 1)
	assert.NotNil(t, paged.OpcNextPage)
}

func TestInternetGateway(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "ig")

	_, err := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
		CreateInternetGatewayDetails: core.CreateInternetGatewayDetails{CompartmentId: new(testComp), VcnId: v.Id},
	})
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())

	r, err := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
		CreateInternetGatewayDetails: core.CreateInternetGatewayDetails{
			CompartmentId: new(testComp), VcnId: v.Id, IsEnabled: new(true),
		},
	})
	require.NoError(t, err)
	assert.True(t, *r.IsEnabled)

	_, err = e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
		CreateInternetGatewayDetails: core.CreateInternetGatewayDetails{
			CompartmentId: new(testComp), VcnId: v.Id, IsEnabled: new(true),
		},
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	u, err := e.client.UpdateInternetGateway(ctx, core.UpdateInternetGatewayRequest{
		IgId: r.Id, UpdateInternetGatewayDetails: core.UpdateInternetGatewayDetails{IsEnabled: new(false)},
	})
	require.NoError(t, err)
	assert.False(t, *u.IsEnabled)
}

func TestNatGateway(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "nat")
	reserved, err := e.client.CreatePublicIp(ctx, core.CreatePublicIpRequest{CreatePublicIpDetails: core.CreatePublicIpDetails{
		CompartmentId: new(testComp), Lifetime: core.CreatePublicIpDetailsLifetimeReserved,
	}})
	require.NoError(t, err)

	tests := []struct {
		name     string
		pubIP    *string
		wantCode string
	}{
		{"unknown public ip", new("ocid1.publicip.oc1.iad.nope"), "NotAuthorizedOrNotFound"},
		{"reserved public ip", reserved.Id, ""},
		{"second gateway hits limit", nil, "LimitExceeded"},
	}

	var natID *string

	for _, tt := range tests {
		r, err := e.client.CreateNatGateway(ctx, core.CreateNatGatewayRequest{CreateNatGatewayDetails: core.CreateNatGatewayDetails{
			CompartmentId: new(testComp), VcnId: v.Id, PublicIpId: tt.pubIP,
		}})
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode(), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.Equal(t, *reserved.IpAddress, *r.NatIp)
		assert.False(t, *r.BlockTraffic)

		natID = r.Id
	}

	ip, err := e.client.GetPublicIp(ctx, core.GetPublicIpRequest{PublicIpId: reserved.Id})
	require.NoError(t, err)
	assert.Equal(t, core.PublicIpLifecycleStateAssigned, ip.LifecycleState)
	assert.Equal(t, core.PublicIpAssignedEntityTypeNatGateway, ip.AssignedEntityType)
	assert.Equal(t, *natID, *ip.AssignedEntityId)

	_, err = e.client.DeletePublicIp(ctx, core.DeletePublicIpRequest{PublicIpId: reserved.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.client.UpdateNatGateway(ctx, core.UpdateNatGatewayRequest{NatGatewayId: natID,
		UpdateNatGatewayDetails: core.UpdateNatGatewayDetails{BlockTraffic: new(true)}})
	require.NoError(t, err)

	_, err = e.client.DeleteNatGateway(ctx, core.DeleteNatGatewayRequest{NatGatewayId: natID})
	require.NoError(t, err)

	ip, err = e.client.GetPublicIp(ctx, core.GetPublicIpRequest{PublicIpId: reserved.Id})
	require.NoError(t, err)
	assert.Equal(t, core.PublicIpLifecycleStateAvailable, ip.LifecycleState)
	assert.Nil(t, ip.AssignedEntityId)

	e.clock.Advance(3 * time.Second)

	fresh, err := e.client.CreateNatGateway(ctx, core.CreateNatGatewayRequest{CreateNatGatewayDetails: core.CreateNatGatewayDetails{
		CompartmentId: new(testComp), VcnId: v.Id,
	}})
	require.NoError(t, err)
	assert.NotEmpty(t, *fresh.NatIp)
	assert.NotEqual(t, *reserved.IpAddress, *fresh.NatIp)

	eph, err := e.client.GetPublicIp(ctx, core.GetPublicIpRequest{PublicIpId: fresh.PublicIpId})
	require.NoError(t, err)
	assert.Equal(t, core.PublicIpLifetimeEphemeral, eph.Lifetime)
}

func TestServiceGateway(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "sg")
	svcs := e.services(t)
	all, osvc := svcs[0], svcs[1]

	create := func(vcn *string, ids ...*string) (core.CreateServiceGatewayResponse, error) {
		refs := []core.ServiceIdRequestDetails{}
		for _, id := range ids {
			refs = append(refs, core.ServiceIdRequestDetails{ServiceId: id})
		}

		return e.client.CreateServiceGateway(ctx, core.CreateServiceGatewayRequest{
			CreateServiceGatewayDetails: core.CreateServiceGatewayDetails{
				CompartmentId: new(testComp), VcnId: vcn, Services: refs,
			},
		})
	}

	_, err := create(v.Id)
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())

	_, err = create(v.Id, new("ocid1.service.oc1.iad.nope"))
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = create(v.Id, all.Id, osvc.Id)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	g, err := create(v.Id, osvc.Id)
	require.NoError(t, err)
	require.Len(t, g.Services, 1)
	assert.Equal(t, *osvc.Name, *g.Services[0].ServiceName)

	_, err = create(v.Id, osvc.Id)
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	a, err := e.client.AttachServiceId(ctx, core.AttachServiceIdRequest{
		ServiceGatewayId: g.Id, AttachServiceDetails: core.ServiceIdRequestDetails{ServiceId: all.Id},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "all-services cannot be mixed")
	_ = a

	d, err := e.client.DetachServiceId(ctx, core.DetachServiceIdRequest{
		ServiceGatewayId: g.Id, DetachServiceDetails: core.ServiceIdRequestDetails{ServiceId: osvc.Id},
	})
	require.NoError(t, err)
	assert.Empty(t, d.Services)

	_, err = e.client.DetachServiceId(ctx, core.DetachServiceIdRequest{
		ServiceGatewayId: g.Id, DetachServiceDetails: core.ServiceIdRequestDetails{ServiceId: osvc.Id},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	a, err = e.client.AttachServiceId(ctx, core.AttachServiceIdRequest{
		ServiceGatewayId: g.Id, AttachServiceDetails: core.ServiceIdRequestDetails{ServiceId: all.Id},
	})
	require.NoError(t, err)
	assert.Len(t, a.Services, 1)

	u, err := e.client.UpdateServiceGateway(ctx, core.UpdateServiceGatewayRequest{
		ServiceGatewayId: g.Id, UpdateServiceGatewayDetails: core.UpdateServiceGatewayDetails{
			BlockTraffic: new(true), Services: []core.ServiceIdRequestDetails{{ServiceId: osvc.Id}},
		},
	})
	require.NoError(t, err)
	assert.True(t, *u.BlockTraffic)
	assert.Equal(t, *osvc.Id, *u.Services[0].ServiceId)
}

func TestPublicIP(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	tests := []struct {
		name     string
		d        core.CreatePublicIpDetails
		wantCode string
	}{
		{"reserved", core.CreatePublicIpDetails{Lifetime: core.CreatePublicIpDetailsLifetimeReserved,
			DisplayName: new("r1")}, ""},
		{"ephemeral needs private ip", core.CreatePublicIpDetails{Lifetime: core.CreatePublicIpDetailsLifetimeEphemeral},
			"InvalidParameter"},
		{"private ip unknown", core.CreatePublicIpDetails{Lifetime: core.CreatePublicIpDetailsLifetimeReserved,
			PrivateIpId: new("ocid1.privateip.oc1.iad.x")}, "NotAuthorizedOrNotFound"},
	}

	var created core.PublicIp

	for _, tt := range tests {
		tt.d.CompartmentId = new(testComp)
		r, err := e.client.CreatePublicIp(ctx, core.CreatePublicIpRequest{CreatePublicIpDetails: tt.d})

		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode(), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.Equal(t, core.PublicIpScopeRegion, r.Scope)
		assert.Equal(t, core.PublicIpLifecycleStateProvisioning, r.LifecycleState)

		created = r.PublicIp
	}

	e.clock.Advance(3 * time.Second)

	byAddr, err := e.client.GetPublicIpByIpAddress(ctx, core.GetPublicIpByIpAddressRequest{
		GetPublicIpByIpAddressDetails: core.GetPublicIpByIpAddressDetails{IpAddress: created.IpAddress},
	})
	require.NoError(t, err)
	assert.Equal(t, *created.Id, *byAddr.Id)
	assert.Equal(t, core.PublicIpLifecycleStateAvailable, byAddr.LifecycleState)

	l, err := e.client.ListPublicIps(ctx, core.ListPublicIpsRequest{
		Scope: core.ListPublicIpsScopeRegion, CompartmentId: new(testComp), Lifetime: core.ListPublicIpsLifetimeReserved,
	})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)

	l, err = e.client.ListPublicIps(ctx, core.ListPublicIpsRequest{
		Scope: core.ListPublicIpsScopeRegion, CompartmentId: new(testComp), Lifetime: core.ListPublicIpsLifetimeEphemeral,
	})
	require.NoError(t, err)
	assert.Empty(t, l.Items)

	_, err = e.client.GetPublicIpByIpAddress(ctx, core.GetPublicIpByIpAddressRequest{
		GetPublicIpByIpAddressDetails: core.GetPublicIpByIpAddressDetails{IpAddress: new("1.2.3.4")},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdatePublicIp(ctx, core.UpdatePublicIpRequest{PublicIpId: created.Id,
		UpdatePublicIpDetails: core.UpdatePublicIpDetails{DisplayName: new("renamed")}})
	require.NoError(t, err)

	_, err = e.client.DeletePublicIp(ctx, core.DeletePublicIpRequest{PublicIpId: created.Id})
	require.NoError(t, err)
}
