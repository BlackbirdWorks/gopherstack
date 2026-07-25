package ec2_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Handler tests for ec2core ops ----

func TestEC2Core_Handler_EgressOnlyInternetGateway(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create VPC first.
	vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")
	require.Equal(t, http.StatusOK, vpcRec.Code)

	body := vpcRec.Body.String()
	vpcIDStart := indexOf(body, "<vpcId>") + len("<vpcId>")
	vpcIDEnd := indexOf(body, "</vpcId>")
	require.Greater(t, vpcIDEnd, vpcIDStart)
	vpcID := body[vpcIDStart:vpcIDEnd]

	rec := postForm(t, h, fmt.Sprintf(
		"Action=CreateEgressOnlyInternetGateway&Version=2016-11-15&VpcId=%s",
		vpcID,
	))
	assert.Equal(t, http.StatusOK, rec.Code)

	descRec := postForm(t, h, "Action=DescribeEgressOnlyInternetGateways&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
}

func TestEC2Core_Handler_VpnGateway(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(t, h, formBody(
		"Action", "CreateVpnGateway",
		"Version", "2016-11-15",
		"Type", "ipsec.1",
	))
	assert.Equal(t, http.StatusOK, createRec.Code)

	descRec := postForm(t, h, formBody(
		"Action", "DescribeVpnGateways",
		"Version", "2016-11-15",
	))
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Extract VPN gateway ID from response body.
	body := createRec.Body.String()
	assert.Contains(t, body, "vgw-")
}

func TestEC2Core_Handler_VpnGatewayCRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	// Create via backend to test all paths.
	vgw, err := bk.CreateVpnGateway("ipsec.1")
	require.NoError(t, err)
	assert.NotEmpty(t, vgw.VpnGatewayID)

	// Describe.
	vgws := bk.DescribeVpnGateways([]string{vgw.VpnGatewayID})
	require.Len(t, vgws, 1)

	vgws2 := bk.DescribeVpnGateways(nil)
	assert.Len(t, vgws2, 1)

	// Attach.
	vpc, _ := bk.CreateVpc("10.0.0.0/16")
	err2 := bk.AttachVpnGateway(vgw.VpnGatewayID, vpc.ID)
	require.NoError(t, err2)

	// Detach.
	err3 := bk.DetachVpnGateway(vgw.VpnGatewayID, vpc.ID)
	require.NoError(t, err3)

	// Delete.
	require.NoError(t, bk.DeleteVpnGateway(vgw.VpnGatewayID))

	// Error cases.
	err4 := bk.DeleteVpnGateway("nonexistent")
	require.Error(t, err4)

	err5 := bk.AttachVpnGateway("nonexistent", vpc.ID)
	require.Error(t, err5)

	err6 := bk.DetachVpnGateway("nonexistent", vpc.ID)
	require.Error(t, err6)
}

func TestEC2Core_Handler_CustomerGateway(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	cgw, err := bk.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
	require.NoError(t, err)
	assert.NotEmpty(t, cgw.CustomerGatewayID)

	cgws := bk.DescribeCustomerGateways([]string{cgw.CustomerGatewayID})
	require.Len(t, cgws, 1)

	cgws2 := bk.DescribeCustomerGateways(nil)
	assert.Len(t, cgws2, 1)

	require.NoError(t, bk.DeleteCustomerGateway(cgw.CustomerGatewayID))
	assert.Empty(t, bk.DescribeCustomerGateways(nil))

	err2 := bk.DeleteCustomerGateway("nonexistent")
	require.Error(t, err2)

	_, err3 := bk.CreateCustomerGateway("ipsec.1", "", "65000")
	require.Error(t, err3)
}

func TestEC2Core_Handler_VpnConnection(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	vgw, err := bk.CreateVpnGateway("ipsec.1")
	require.NoError(t, err)

	cgw, err := bk.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
	require.NoError(t, err)

	conn, err := bk.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, err)
	assert.NotEmpty(t, conn.VpnConnectionID)

	conns := bk.DescribeVpnConnections([]string{conn.VpnConnectionID})
	require.Len(t, conns, 1)

	conns2 := bk.DescribeVpnConnections(nil)
	assert.Len(t, conns2, 1)

	require.NoError(t, bk.DeleteVpnConnection(conn.VpnConnectionID))
	assert.Empty(t, bk.DescribeVpnConnections(nil))

	err2 := bk.DeleteVpnConnection("nonexistent")
	require.Error(t, err2)

	_, err3 := bk.CreateVpnConnection("ipsec.1", "", vgw.VpnGatewayID)
	require.Error(t, err3)

	_, err4 := bk.CreateVpnConnection("ipsec.1", "nonexistent", vgw.VpnGatewayID)
	require.Error(t, err4)

	_, err5 := bk.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, "nonexistent")
	require.Error(t, err5)
}

func TestEC2Core_Handler_VpcEndpointServiceConfig(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	cfg, err := bk.CreateVpcEndpointServiceConfiguration(
		true,
		[]string{"arn:aws:elasticloadbalancing::nlb/test"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.ServiceID)

	cfgs := bk.DescribeVpcEndpointServiceConfigurations([]string{cfg.ServiceID})
	require.Len(t, cfgs, 1)

	cfgs2 := bk.DescribeVpcEndpointServiceConfigurations(nil)
	assert.Len(t, cfgs2, 1)

	// Modify.
	require.NoError(t, bk.ModifyVpcEndpointServiceConfiguration(cfg.ServiceID, false))

	// Delete.
	require.NoError(t, bk.DeleteVpcEndpointServiceConfigurations([]string{cfg.ServiceID}))
	assert.Empty(t, bk.DescribeVpcEndpointServiceConfigurations(nil))

	// Delete not found.
	err2 := bk.DeleteVpcEndpointServiceConfigurations([]string{"nonexistent"})
	require.Error(t, err2)

	// Modify not found.
	err3 := bk.ModifyVpcEndpointServiceConfiguration("nonexistent", true)
	require.Error(t, err3)
}

func TestEC2Core_IPAM(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ipam, err := bk.CreateIpam()
	require.NoError(t, err)
	assert.NotEmpty(t, ipam.IpamID)

	ipams := bk.DescribeIpams([]string{ipam.IpamID})
	require.Len(t, ipams, 1)

	ipams2 := bk.DescribeIpams(nil)
	assert.Len(t, ipams2, 1)

	// Create pool.
	pool, err := bk.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "10.0.0.0/8")
	require.NoError(t, err)
	assert.NotEmpty(t, pool.IpamPoolID)

	pools := bk.DescribeIpamPools([]string{pool.IpamPoolID})
	require.Len(t, pools, 1)

	pools2 := bk.DescribeIpamPools(nil)
	assert.Len(t, pools2, 1)

	// Allocate CIDR.
	alloc, err := bk.AllocateIpamPoolCidr(pool.IpamPoolID, "10.1.0.0/24", 0)
	require.NoError(t, err)
	assert.NotEmpty(t, alloc.IpamPoolAllocationID)

	// Get pool CIDRs.
	allocs := bk.GetIpamPoolCidrs(pool.IpamPoolID)
	assert.NotEmpty(t, allocs)

	// Release allocation.
	require.NoError(t, bk.ReleaseIpamPoolAllocation(pool.IpamPoolID, alloc.IpamPoolAllocationID))

	// Delete pool.
	require.NoError(t, bk.DeleteIpamPool(pool.IpamPoolID))

	// Delete IPAM.
	require.NoError(t, bk.DeleteIpam(ipam.IpamID))
	assert.Empty(t, bk.DescribeIpams(nil))

	// Error cases.
	err2 := bk.DeleteIpam("nonexistent")
	require.Error(t, err2)

	err3 := bk.DeleteIpamPool("nonexistent")
	require.Error(t, err3)

	_, err4 := bk.CreateIpamPool("nonexistent", "ipv4", "", "")
	require.Error(t, err4)

	err5 := bk.ReleaseIpamPoolAllocation("nonexistent", "alloc")
	require.Error(t, err5)
}

func TestEC2Core_RejectVpcPeeringConnection(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	// Add a peering connection via internal helper.
	pc := &ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-test001",
		RequesterVpcID:         "vpc-1",
		AccepterVpcID:          "vpc-2",
		State:                  "pending-acceptance",
	}
	bk.AddVpcPeeringConnectionInternal(pc)
	peers := bk.DescribeVpcPeeringConnections(nil)
	require.Len(t, peers, 1)
	peeringID := peers[0].VpcPeeringConnectionID

	err := bk.RejectVpcPeeringConnection(peeringID)
	require.NoError(t, err)

	// Should be rejected.
	peers2 := bk.DescribeVpcPeeringConnections(nil)
	require.Len(t, peers2, 1)
	assert.Equal(t, "rejected", peers2[0].State)

	// Not found.
	err2 := bk.RejectVpcPeeringConnection("nonexistent")
	require.Error(t, err2)
}

// ---- Handler-level tests for advanced networking ----

func TestEC2Core_Handler_VpnGatewayCRUDViaHandler(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create VPN gateway.
	createRec := postForm(t, h, "Action=CreateVpnGateway&Version=2016-11-15&Type=ipsec.1")
	assert.Equal(t, http.StatusOK, createRec.Code)

	// Describe.
	descRec := postForm(t, h, "Action=DescribeVpnGateways&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Customer gateway.
	cgwRec := postForm(
		t,
		h,
		"Action=CreateCustomerGateway&Version=2016-11-15&Type=ipsec.1&IpAddress=1.2.3.4&BgpAsn=65000",
	)
	assert.Equal(t, http.StatusOK, cgwRec.Code)

	descCgwRec := postForm(t, h, "Action=DescribeCustomerGateways&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descCgwRec.Code)

	// Describe VPN connections.
	descConnRec := postForm(t, h, "Action=DescribeVpnConnections&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descConnRec.Code)
}

func TestEC2Core_Handler_VpcEndpointServiceConfigViaHandler(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(
		t, h,
		"Action=CreateVpcEndpointServiceConfiguration&Version=2016-11-15"+
			"&AcceptanceRequired=true&NetworkLoadBalancerArn.1=arn:nlb:test",
	)
	assert.Equal(t, http.StatusOK, createRec.Code)

	descRec := postForm(t, h, "Action=DescribeVpcEndpointServiceConfigurations&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
}

func TestEC2Core_Handler_IPAMViaHandler(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create IPAM.
	createRec := postForm(t, h, "Action=CreateIpam&Version=2016-11-15")
	require.Equal(t, http.StatusOK, createRec.Code)

	// Describe.
	descRec := postForm(t, h, "Action=DescribeIpams&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Extract IPAM ID from response.
	body := createRec.Body.String()
	ipamIDStart := indexOf(body, "<ipamId>") + len("<ipamId>")
	ipamIDEnd := indexOf(body, "</ipamId>")
	if ipamIDStart > 0 && ipamIDEnd > ipamIDStart {
		ipamID := body[ipamIDStart:ipamIDEnd]

		// Create pool.
		poolRec := postForm(t, h, fmt.Sprintf(
			"Action=CreateIpamPool&Version=2016-11-15&IpamId=%s"+
				"&AddressFamily=ipv4&Locale=us-east-1&ProvisionedCidrs.item.1.Cidr=10.0.0.0/8",
			ipamID,
		))
		assert.Equal(t, http.StatusOK, poolRec.Code)

		// Describe pools.
		descPoolsRec := postForm(t, h, "Action=DescribeIpamPools&Version=2016-11-15")
		assert.Equal(t, http.StatusOK, descPoolsRec.Code)

		// Delete IPAM.
		delRec := postForm(t, h, fmt.Sprintf(
			"Action=DeleteIpam&Version=2016-11-15&IpamId=%s",
			ipamID,
		))
		assert.Equal(t, http.StatusOK, delRec.Code)
	}
}

func TestEC2Core_Handler_TGWRouteTableViaHandler(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create TGW.
	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15&Description=test")
	require.Equal(t, http.StatusOK, tgwRec.Code)

	body := tgwRec.Body.String()
	tgwIDStart := indexOf(body, "<transitGatewayId>") + len("<transitGatewayId>")
	tgwIDEnd := indexOf(body, "</transitGatewayId>")
	if tgwIDStart <= 0 || tgwIDEnd <= tgwIDStart {
		// Try id tag.
		tgwIDStart = indexOf(body, "<id>") + len("<id>")
		tgwIDEnd = indexOf(body, "</id>")
	}

	if tgwIDStart > 0 && tgwIDEnd > tgwIDStart {
		tgwID := body[tgwIDStart:tgwIDEnd]

		rtRec := postForm(t, h, fmt.Sprintf(
			"Action=CreateTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayId=%s",
			tgwID,
		))
		assert.Equal(t, http.StatusOK, rtRec.Code)

		descRtRec := postForm(t, h, "Action=DescribeTransitGatewayRouteTables&Version=2016-11-15")
		assert.Equal(t, http.StatusOK, descRtRec.Code)
	}
}

func TestEC2Core_Handler_AssociateVpcCidrBlock(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create VPC.
	vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")
	require.Equal(t, http.StatusOK, vpcRec.Code)

	body := vpcRec.Body.String()
	vpcIDStart := indexOf(body, "<vpcId>") + len("<vpcId>")
	vpcIDEnd := indexOf(body, "</vpcId>")

	if vpcIDStart > 0 && vpcIDEnd > vpcIDStart {
		vpcID := body[vpcIDStart:vpcIDEnd]

		assocRec := postForm(t, h, fmt.Sprintf(
			"Action=AssociateVpcCidrBlock&Version=2016-11-15&VpcId=%s&CidrBlock=192.168.0.0/24",
			vpcID,
		))
		assert.Equal(t, http.StatusOK, assocRec.Code)
	}
}

// indexOf returns the index of substr in s, or -1 if not found.
func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}

	return -1
}

// TestHandler_IpamPoolAllocations_DescribeAndModify verifies
// DescribeIpamPoolAllocations (a cross-pool describe by allocation ID, unlike
// GetIpamPoolAllocations which requires a single IpamPoolId) and
// ModifyIpamPoolAllocation (parity-4).
func TestHandler_IpamPoolAllocations_DescribeAndModify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *ec2.Handler) url.Values
		name     string
		wantBody []string
		wantCode int
	}{
		{
			name: "modify_unknown_allocation_not_found",
			setup: func(*testing.T, *ec2.Handler) url.Values {
				return url.Values{
					"Action":               {"ModifyIpamPoolAllocation"},
					"IpamPoolAllocationId": {"ipam-alloc-missing"},
					"Description":          {"anything"},
				}
			},
			wantCode: http.StatusBadRequest,
			wantBody: []string{"InvalidIpamPoolAllocationId.NotFound"},
		},
		{
			name: "describe_across_pools_by_allocation_id",
			setup: func(t *testing.T, h *ec2.Handler) url.Values {
				t.Helper()

				ipam, err := h.Backend.CreateIpam()
				require.NoError(t, err)
				pool, err := h.Backend.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "10.0.0.0/8")
				require.NoError(t, err)
				alloc, err := h.Backend.AllocateIpamPoolCidr(pool.IpamPoolID, "10.1.0.0/24", 0)
				require.NoError(t, err)

				return url.Values{
					"Action":                 {"DescribeIpamPoolAllocations"},
					"IpamPoolAllocationId.1": {alloc.IpamPoolAllocationID},
				}
			},
			wantCode: http.StatusOK,
			wantBody: []string{"DescribeIpamPoolAllocationsResponse", "10.1.0.0/24"},
		},
		{
			name: "modify_updates_description",
			setup: func(t *testing.T, h *ec2.Handler) url.Values {
				t.Helper()

				ipam, err := h.Backend.CreateIpam()
				require.NoError(t, err)
				pool, err := h.Backend.CreateIpamPool(ipam.IpamID, "ipv4", "us-east-1", "10.0.0.0/8")
				require.NoError(t, err)
				alloc, err := h.Backend.AllocateIpamPoolCidr(pool.IpamPoolID, "10.2.0.0/24", 0)
				require.NoError(t, err)

				return url.Values{
					"Action":               {"ModifyIpamPoolAllocation"},
					"IpamPoolAllocationId": {alloc.IpamPoolAllocationID},
					"Description":          {"prod workload"},
				}
			},
			wantCode: http.StatusOK,
			wantBody: []string{
				"ModifyIpamPoolAllocationResponse",
				"<description>prod workload</description>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			vals := tt.setup(t, h)

			rec := postForm(t, h, vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}
