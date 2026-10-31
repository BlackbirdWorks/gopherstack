package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "snap")
	s := e.mkSubnet(t, v, "10.0.1.0/24")

	nsg, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
		CompartmentId: new(testComp),
		VcnId:         v.Id,
	})
	require.NoError(t, err)

	rules, err := e.client.AddNetworkSecurityGroupSecurityRules(ctx, core.AddNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: nsg.Id,
		SecurityRules: []core.AddSecurityRuleDetails{{Direction: core.AddSecurityRuleDetailsDirectionIngress,
			Protocol: new("all"), Source: new("0.0.0.0/0")}}})
	require.NoError(t, err)

	data := e.backend.Snapshot(ctx)
	require.NotEmpty(t, data)

	e2 := newEnv(t, false)
	require.NoError(t, e2.backend.Restore(ctx, data))

	got, err := e2.client.GetSubnet(ctx, core.GetSubnetRequest{SubnetId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.0/24", *got.CidrBlock)
	assert.NotNil(t, got.Etag)

	l, err := e2.client.ListNetworkSecurityGroupSecurityRules(ctx, core.ListNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: nsg.Id})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)
	assert.Equal(t, *rules.SecurityRules[0].Id, *l.Items[0].Id)

	_, err = e2.client.GetRouteTable(ctx, core.GetRouteTableRequest{RtId: v.DefaultRouteTableId})
	require.NoError(t, err)

	other := e2.mkVcn(t, "10.5.0.0/16", "after")
	assert.NotEqual(t, *v.Id, *other.Id)

	require.Error(t, e2.backend.Restore(ctx, []byte("not json")))
}

func TestResetClears(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	v := e.mkVcn(t, "10.0.0.0/16", "reset")
	e.handler.Reset()

	_, err := e.client.GetVcn(t.Context(), core.GetVcnRequest{VcnId: v.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestEnforcedAuth(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	ctx := t.Context()

	_, err := e.client.ListServices(ctx, core.ListServicesRequest{})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusUnauthorized, se.GetHTTPStatusCode())
	assert.Equal(t, "NotAuthenticated", se.GetCode())

	user := "ocid1.user.oc1..regd"
	_, err = e.backend.Auth.Register(testTenancy, user, e.kp.pubPEM)
	require.NoError(t, err)

	c := e.clientFor(t, user, e.kp)
	_, err = c.ListServices(ctx, core.ListServicesRequest{})
	require.NoError(t, err)

	other := newKeyPair(t)
	c = e.clientFor(t, user, other)
	_, err = c.ListServices(ctx, core.ListServicesRequest{})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())
}

func TestUnknownRoute(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	resp, err := testclient.New(t).Get(e.base + "/20160918/nothing")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &oci.Provider{}
	assert.Equal(t, "OCICore", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, oci.ErrNilAppContext)
}
