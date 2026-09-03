package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- TGW VPC Attachments ----

func TestNetworking1_TGWVpcAttachment(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test"})
	require.NoError(t, err)

	vpc, err := bk.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	att, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, vpc.ID, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, att.TransitGatewayAttachmentID)

	// Describe with filter.
	atts := bk.DescribeTransitGatewayVpcAttachments([]string{att.TransitGatewayAttachmentID})
	require.Len(t, atts, 1)

	// Describe all.
	atts2 := bk.DescribeTransitGatewayVpcAttachments(nil)
	assert.Len(t, atts2, 1)

	// Delete.
	require.NoError(t, bk.DeleteTransitGatewayVpcAttachment(att.TransitGatewayAttachmentID))
	assert.Empty(t, bk.DescribeTransitGatewayVpcAttachments(nil))

	// Error cases.
	_, err2 := bk.CreateTransitGatewayVpcAttachment("", vpc.ID, nil, nil)
	require.Error(t, err2)

	_, err3 := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "", nil, nil)
	require.Error(t, err3)

	_, err4 := bk.CreateTransitGatewayVpcAttachment("nonexistent", vpc.ID, nil, nil)
	require.Error(t, err4)

	err5 := bk.DeleteTransitGatewayVpcAttachment("")
	require.Error(t, err5)

	err6 := bk.DeleteTransitGatewayVpcAttachment("nonexistent")
	require.Error(t, err6)
}

// ---- Flow Logs ----

func TestNetworking1_FlowLogs(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	vpc, err := bk.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	fls, err := bk.CreateFlowLogs([]string{vpc.ID}, "ALL", "cloud-watch-logs", "/aws/vpc/flow-logs", nil)
	require.NoError(t, err)
	require.Len(t, fls, 1)

	// Describe with filter.
	desc := bk.DescribeFlowLogs([]string{fls[0].FlowLogID})
	require.Len(t, desc, 1)

	// Describe all.
	desc2 := bk.DescribeFlowLogs(nil)
	assert.Len(t, desc2, 1)

	// Delete.
	require.NoError(t, bk.DeleteFlowLogs([]string{fls[0].FlowLogID}))
	assert.Empty(t, bk.DescribeFlowLogs(nil))

	// Error cases.
	_, err2 := bk.CreateFlowLogs(nil, "ALL", "", "", nil)
	require.Error(t, err2)

	err3 := bk.DeleteFlowLogs(nil)
	require.Error(t, err3)

	err4 := bk.DeleteFlowLogs([]string{"nonexistent"})
	require.Error(t, err4)
}

// ---- DHCP Options ----

func TestNetworking1_DhcpOptions(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	vpc, err := bk.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	opts, err := bk.CreateDhcpOptions(nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, opts.DhcpOptionsID)

	// Describe with filter.
	all := bk.DescribeDhcpOptions([]string{opts.DhcpOptionsID})
	require.Len(t, all, 1)

	// Describe all.
	all2 := bk.DescribeDhcpOptions(nil)
	assert.NotEmpty(t, all2)

	// Associate.
	require.NoError(t, bk.AssociateDhcpOptions(opts.DhcpOptionsID, vpc.ID))

	// Associate default (special case).
	require.NoError(t, bk.AssociateDhcpOptions("default", vpc.ID))

	// Delete.
	require.NoError(t, bk.DeleteDhcpOptions(opts.DhcpOptionsID))

	// Error cases.
	err2 := bk.AssociateDhcpOptions("", "")
	require.Error(t, err2)

	err3 := bk.AssociateDhcpOptions(opts.DhcpOptionsID, "nonexistent")
	require.Error(t, err3)

	err4 := bk.AssociateDhcpOptions("nonexistent-opts", vpc.ID)
	require.Error(t, err4)

	err5 := bk.DeleteDhcpOptions("")
	require.Error(t, err5)

	err6 := bk.DeleteDhcpOptions("nonexistent")
	require.Error(t, err6)
}

// ---- Launch Template extras ----

func TestNetworking1_LaunchTemplateExtras(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create launch template via handler.
	ltRec := postForm(
		t, h,
		"Action=CreateLaunchTemplate&Version=2016-11-15"+
			"&LaunchTemplateName=my-lt"+
			"&LaunchTemplateData.ImageId=ami-123"+
			"&LaunchTemplateData.InstanceType=t2.micro",
	)
	require.Equal(t, http.StatusOK, ltRec.Code)

	body := ltRec.Body.String()
	ltIDStart := indexOf(body, "<launchTemplateId>") + len("<launchTemplateId>")
	ltIDEnd := indexOf(body, "</launchTemplateId>")
	require.Greater(t, ltIDEnd, ltIDStart)
	ltID := body[ltIDStart:ltIDEnd]

	bk := newTestBackend()
	bk2 := newTestBackend()
	_ = bk
	_ = bk2

	// Use our newHandler backend directly via the backend.
	bk3 := newTestBackend()
	_, err := bk3.CreateLaunchTemplate("lt-test", "ami-123", "t2.micro", nil)
	require.NoError(t, err)

	// Find the template.
	lts := bk3.DescribeLaunchTemplates(nil)
	require.Len(t, lts, 1)
	ltIDLocal := lts[0].ID

	// Modify.
	modified, err := bk3.ModifyLaunchTemplate(ltIDLocal, 1)
	require.NoError(t, err)
	assert.Equal(t, ltIDLocal, modified.ID)

	// Create new version.
	ver, err := bk3.CreateLaunchTemplateVersion(ltIDLocal, "ami-456", "t3.micro")
	require.NoError(t, err)
	assert.Equal(t, int64(2), ver.VersionNumber)

	// Delete versions.
	deleted, err := bk3.DeleteLaunchTemplateVersions(ltIDLocal, []int64{2})
	require.NoError(t, err)
	assert.Equal(t, []int64{2}, deleted)

	// Error cases.
	_, err2 := bk3.ModifyLaunchTemplate("", 1)
	require.Error(t, err2)

	_, err3 := bk3.ModifyLaunchTemplate("nonexistent", 1)
	require.Error(t, err3)

	_, err4 := bk3.CreateLaunchTemplateVersion("", "", "")
	require.Error(t, err4)

	_, err5 := bk3.CreateLaunchTemplateVersion("nonexistent", "", "")
	require.Error(t, err5)

	_, err6 := bk3.DeleteLaunchTemplateVersions("", nil)
	require.Error(t, err6)

	_, err7 := bk3.DeleteLaunchTemplateVersions("nonexistent", nil)
	require.Error(t, err7)

	// GetLaunchTemplateData.
	insts, err := bk3.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)

	ltData, err := bk3.GetLaunchTemplateData(insts[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "ami-123", ltData.ImageID)

	_, err8 := bk3.GetLaunchTemplateData("")
	require.Error(t, err8)

	_, err9 := bk3.GetLaunchTemplateData("nonexistent")
	require.Error(t, err9)

	// Use ltID (from handler) to exercise handler.
	modRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyLaunchTemplate&Version=2016-11-15&LaunchTemplateId=%s&SetDefaultVersion.Value=1",
		ltID,
	))
	assert.Equal(t, http.StatusOK, modRec.Code)
}

// ---- Handler-level tests for networking1 ops ----

func TestNetworking1_Handler_TGWVpcAttachment(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create TGW.
	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15&Description=test")
	require.Equal(t, http.StatusOK, tgwRec.Code)
	tgwBody := tgwRec.Body.String()
	tgwIDStart := indexOf(tgwBody, "<transitGatewayId>") + len("<transitGatewayId>")
	tgwIDEnd := indexOf(tgwBody, "</transitGatewayId>")
	require.Greater(t, tgwIDEnd, tgwIDStart)
	tgwID := tgwBody[tgwIDStart:tgwIDEnd]

	// Create VPC.
	vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")
	require.Equal(t, http.StatusOK, vpcRec.Code)
	vpcBody := vpcRec.Body.String()
	vpcIDStart := indexOf(vpcBody, "<vpcId>") + len("<vpcId>")
	vpcIDEnd := indexOf(vpcBody, "</vpcId>")
	require.Greater(t, vpcIDEnd, vpcIDStart)
	vpcID := vpcBody[vpcIDStart:vpcIDEnd]

	// Create attachment.
	attRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayId=%s&VpcId=%s",
		tgwID, vpcID,
	))
	assert.Equal(t, http.StatusOK, attRec.Code)

	// Describe attachments.
	descRec := postForm(t, h, "Action=DescribeTransitGatewayVpcAttachments&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Delete attachment.
	attBody := attRec.Body.String()
	attIDStart := indexOf(
		attBody,
		"<transitGatewayAttachmentId>",
	) + len(
		"<transitGatewayAttachmentId>",
	)
	attIDEnd := indexOf(attBody, "</transitGatewayAttachmentId>")
	if attIDStart > 0 && attIDEnd > attIDStart {
		attID := attBody[attIDStart:attIDEnd]
		delRec := postForm(t, h, fmt.Sprintf(
			"Action=DeleteTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayAttachmentId=%s",
			attID,
		))
		assert.Equal(t, http.StatusOK, delRec.Code)
	}
}

func TestNetworking1_Handler_FlowLogs(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")
	require.Equal(t, http.StatusOK, vpcRec.Code)
	vpcBody := vpcRec.Body.String()
	vpcIDStart := indexOf(vpcBody, "<vpcId>") + len("<vpcId>")
	vpcIDEnd := indexOf(vpcBody, "</vpcId>")
	require.Greater(t, vpcIDEnd, vpcIDStart)
	vpcID := vpcBody[vpcIDStart:vpcIDEnd]

	createRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateFlowLogs&Version=2016-11-15&ResourceId.1=%s&TrafficType=ALL&LogDestinationType=cloud-watch-logs",
		vpcID,
	))
	assert.Equal(t, http.StatusOK, createRec.Code)

	descRec := postForm(t, h, "Action=DescribeFlowLogs&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
}

func TestNetworking1_Handler_DhcpOptions(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(
		t, h,
		"Action=CreateDhcpOptions&Version=2016-11-15"+
			"&DhcpConfiguration.1.Key=domain-name&DhcpConfiguration.1.Value.1=example.com",
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	body := createRec.Body.String()
	dhcpIDStart := indexOf(body, "<dhcpOptionsId>") + len("<dhcpOptionsId>")
	dhcpIDEnd := indexOf(body, "</dhcpOptionsId>")

	descRec := postForm(t, h, "Action=DescribeDhcpOptions&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)

	if dhcpIDStart > 0 && dhcpIDEnd > dhcpIDStart {
		dhcpID := body[dhcpIDStart:dhcpIDEnd]

		vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.0.0.0/16")
		require.Equal(t, http.StatusOK, vpcRec.Code)
		vpcBody := vpcRec.Body.String()
		vpcIDStart := indexOf(vpcBody, "<vpcId>") + len("<vpcId>")
		vpcIDEnd := indexOf(vpcBody, "</vpcId>")
		require.Greater(t, vpcIDEnd, vpcIDStart)
		vpcID := vpcBody[vpcIDStart:vpcIDEnd]

		assocRec := postForm(t, h, fmt.Sprintf(
			"Action=AssociateDhcpOptions&Version=2016-11-15&DhcpOptionsId=%s&VpcId=%s",
			dhcpID, vpcID,
		))
		assert.Equal(t, http.StatusOK, assocRec.Code)

		delRec := postForm(t, h, fmt.Sprintf(
			"Action=DeleteDhcpOptions&Version=2016-11-15&DhcpOptionsId=%s",
			dhcpID,
		))
		assert.Equal(t, http.StatusOK, delRec.Code)
	}
}

// ---- Handler tests for advanced networking (missed handlers) ----

func TestNetworking1_Handler_AdvancedNetworkingOps(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create customer gateway.
	cgwRec := postForm(
		t,
		h,
		"Action=CreateCustomerGateway&Version=2016-11-15&Type=ipsec.1&IpAddress=5.6.7.8&BgpAsn=65001",
	)
	require.Equal(t, http.StatusOK, cgwRec.Code)
	cgwBody := cgwRec.Body.String()
	cgwIDStart := indexOf(cgwBody, "<customerGatewayId>") + len("<customerGatewayId>")
	cgwIDEnd := indexOf(cgwBody, "</customerGatewayId>")
	require.Greater(t, cgwIDEnd, cgwIDStart)
	cgwID := cgwBody[cgwIDStart:cgwIDEnd]

	// Create VPN gateway.
	vgwRec := postForm(t, h, "Action=CreateVpnGateway&Version=2016-11-15&Type=ipsec.1")
	require.Equal(t, http.StatusOK, vgwRec.Code)
	vgwBody := vgwRec.Body.String()
	vgwIDStart := indexOf(vgwBody, "<vpnGatewayId>") + len("<vpnGatewayId>")
	vgwIDEnd := indexOf(vgwBody, "</vpnGatewayId>")
	require.Greater(t, vgwIDEnd, vgwIDStart)
	vgwID := vgwBody[vgwIDStart:vgwIDEnd]

	// Create VPN connection.
	connRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateVpnConnection&Version=2016-11-15&Type=ipsec.1&CustomerGatewayId=%s&VpnGatewayId=%s",
		cgwID,
		vgwID,
	))
	assert.Equal(t, http.StatusOK, connRec.Code)
	connBody := connRec.Body.String()
	connIDStart := indexOf(connBody, "<vpnConnectionId>") + len("<vpnConnectionId>")
	connIDEnd := indexOf(connBody, "</vpnConnectionId>")

	// Describe VPN connections.
	descVpnRec := postForm(t, h, "Action=DescribeVpnConnections&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descVpnRec.Code)

	// Delete VPN connection.
	if connIDStart > 0 && connIDEnd > connIDStart {
		connID := connBody[connIDStart:connIDEnd]
		delConnRec := postForm(t, h, fmt.Sprintf(
			"Action=DeleteVpnConnection&Version=2016-11-15&VpnConnectionId=%s",
			connID,
		))
		assert.Equal(t, http.StatusOK, delConnRec.Code)
	}

	// Delete customer gateway.
	delCgwRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteCustomerGateway&Version=2016-11-15&CustomerGatewayId=%s",
		cgwID,
	))
	assert.Equal(t, http.StatusOK, delCgwRec.Code)

	// Create VPC and attach/detach VPN gateway.
	vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=172.16.0.0/16")
	require.Equal(t, http.StatusOK, vpcRec.Code)
	vpcBody := vpcRec.Body.String()
	vpcIDStart := indexOf(vpcBody, "<vpcId>") + len("<vpcId>")
	vpcIDEnd := indexOf(vpcBody, "</vpcId>")
	require.Greater(t, vpcIDEnd, vpcIDStart)
	vpcID := vpcBody[vpcIDStart:vpcIDEnd]

	attachRec := postForm(t, h, fmt.Sprintf(
		"Action=AttachVpnGateway&Version=2016-11-15&VpnGatewayId=%s&VpcId=%s",
		vgwID, vpcID,
	))
	assert.Equal(t, http.StatusOK, attachRec.Code)

	detachRec := postForm(t, h, fmt.Sprintf(
		"Action=DetachVpnGateway&Version=2016-11-15&VpnGatewayId=%s&VpcId=%s",
		vgwID, vpcID,
	))
	assert.Equal(t, http.StatusOK, detachRec.Code)

	// Delete VPN gateway.
	delVgwRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteVpnGateway&Version=2016-11-15&VpnGatewayId=%s",
		vgwID,
	))
	assert.Equal(t, http.StatusOK, delVgwRec.Code)
}

func TestNetworking1_Handler_VpcEndpointServiceConfigOps(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create.
	createRec := postForm(
		t, h,
		"Action=CreateVpcEndpointServiceConfiguration&Version=2016-11-15"+
			"&AcceptanceRequired=false&NetworkLoadBalancerArn.1=arn:aws:elasticloadbalancing::nlb/my-nlb",
	)
	require.Equal(t, http.StatusOK, createRec.Code)
	body := createRec.Body.String()
	svcIDStart := indexOf(body, "<serviceId>") + len("<serviceId>")
	svcIDEnd := indexOf(body, "</serviceId>")
	require.Greater(t, svcIDEnd, svcIDStart)
	svcID := body[svcIDStart:svcIDEnd]

	// Modify.
	modRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyVpcEndpointServiceConfiguration&Version=2016-11-15&ServiceId=%s&AcceptanceRequired=true",
		svcID,
	))
	assert.Equal(t, http.StatusOK, modRec.Code)

	// Delete.
	delRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteVpcEndpointServiceConfigurations&Version=2016-11-15&ServiceId.1=%s",
		svcID,
	))
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestNetworking1_Handler_IPAMPoolOps(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create IPAM.
	ipamRec := postForm(t, h, "Action=CreateIpam&Version=2016-11-15")
	require.Equal(t, http.StatusOK, ipamRec.Code)
	ipamBody := ipamRec.Body.String()
	ipamIDStart := indexOf(ipamBody, "<ipamId>") + len("<ipamId>")
	ipamIDEnd := indexOf(ipamBody, "</ipamId>")
	require.Greater(t, ipamIDEnd, ipamIDStart)
	ipamID := ipamBody[ipamIDStart:ipamIDEnd]

	// Create pool.
	poolRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateIpamPool&Version=2016-11-15&IpamId=%s&AddressFamily=ipv4&ProvisionedCidrs.item.1.Cidr=10.0.0.0/8",
		ipamID,
	))
	require.Equal(t, http.StatusOK, poolRec.Code)
	poolBody := poolRec.Body.String()
	poolIDStart := indexOf(poolBody, "<ipamPoolId>") + len("<ipamPoolId>")
	poolIDEnd := indexOf(poolBody, "</ipamPoolId>")
	require.Greater(t, poolIDEnd, poolIDStart)
	poolID := poolBody[poolIDStart:poolIDEnd]

	// Allocate CIDR.
	allocRec := postForm(t, h, fmt.Sprintf(
		"Action=AllocateIpamPoolCidr&Version=2016-11-15&IpamPoolId=%s&Cidr=10.1.0.0/24",
		poolID,
	))
	assert.Equal(t, http.StatusOK, allocRec.Code)

	// Get pool CIDRs.
	getCidrsRec := postForm(t, h, fmt.Sprintf(
		"Action=GetIpamPoolCidrs&Version=2016-11-15&IpamPoolId=%s",
		poolID,
	))
	assert.Equal(t, http.StatusOK, getCidrsRec.Code)

	// Release allocation.
	allocBody := allocRec.Body.String()
	allocIDStart := indexOf(allocBody, "<ipamPoolAllocationId>") + len("<ipamPoolAllocationId>")
	allocIDEnd := indexOf(allocBody, "</ipamPoolAllocationId>")
	if allocIDStart > 0 && allocIDEnd > allocIDStart {
		allocID := allocBody[allocIDStart:allocIDEnd]
		releaseRec := postForm(t, h, fmt.Sprintf(
			"Action=ReleaseIpamPoolAllocation&Version=2016-11-15&IpamPoolId=%s&IpamPoolAllocationId=%s",
			poolID,
			allocID,
		))
		assert.Equal(t, http.StatusOK, releaseRec.Code)
	}

	// Delete pool.
	delPoolRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteIpamPool&Version=2016-11-15&IpamPoolId=%s",
		poolID,
	))
	assert.Equal(t, http.StatusOK, delPoolRec.Code)
}

// ---- Handler tests for ec2core ops (remaining) ----

func TestNetworking1_Handler_IamInstanceProfile(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Run an instance first.
	runRec := postForm(
		t, h,
		"Action=RunInstances&Version=2016-11-15"+
			"&ImageId=ami-123&InstanceType=t2.micro&MinCount=1&MaxCount=1",
	)
	require.Equal(t, http.StatusOK, runRec.Code)
	runBody := runRec.Body.String()
	instIDStart := indexOf(runBody, "<instanceId>") + len("<instanceId>")
	instIDEnd := indexOf(runBody, "</instanceId>")
	require.Greater(t, instIDEnd, instIDStart)
	instID := runBody[instIDStart:instIDEnd]

	// Associate.
	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateIamInstanceProfile&Version=2016-11-15"+
			"&InstanceId=%s&IamInstanceProfile.Arn=arn:aws:iam::000000000000:instance-profile/MyRole",
		instID,
	))
	assert.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assocIDStart := indexOf(assocBody, "<associationId>") + len("<associationId>")
	assocIDEnd := indexOf(assocBody, "</associationId>")

	// Describe.
	descRec := postForm(t, h, "Action=DescribeIamInstanceProfileAssociations&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)

	if assocIDStart > 0 && assocIDEnd > assocIDStart {
		assocID := assocBody[assocIDStart:assocIDEnd]

		// Replace.
		replaceRec := postForm(t, h, fmt.Sprintf(
			"Action=ReplaceIamInstanceProfileAssociation&Version=2016-11-15&AssociationId=%s&IamInstanceProfile.Arn=arn:new",
			assocID,
		))
		assert.Equal(t, http.StatusOK, replaceRec.Code)

		// Disassociate.
		disRec := postForm(t, h, fmt.Sprintf(
			"Action=DisassociateIamInstanceProfile&Version=2016-11-15&AssociationId=%s",
			assocID,
		))
		assert.Equal(t, http.StatusOK, disRec.Code)
	}
}

func TestNetworking1_Handler_DeleteVpcAndSubnet(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vpcRec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.100.0.0/16")
	require.Equal(t, http.StatusOK, vpcRec.Code)
	vpcBody := vpcRec.Body.String()
	vpcIDStart := indexOf(vpcBody, "<vpcId>") + len("<vpcId>")
	vpcIDEnd := indexOf(vpcBody, "</vpcId>")
	require.Greater(t, vpcIDEnd, vpcIDStart)
	vpcID := vpcBody[vpcIDStart:vpcIDEnd]

	subRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateSubnet&Version=2016-11-15&VpcId=%s&CidrBlock=10.100.0.0/24",
		vpcID,
	))
	require.Equal(t, http.StatusOK, subRec.Code)
	subBody := subRec.Body.String()
	subIDStart := indexOf(subBody, "<subnetId>") + len("<subnetId>")
	subIDEnd := indexOf(subBody, "</subnetId>")
	require.Greater(t, subIDEnd, subIDStart)
	subID := subBody[subIDStart:subIDEnd]

	// Delete subnet.
	delSubRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteSubnet&Version=2016-11-15&SubnetId=%s",
		subID,
	))
	assert.Equal(t, http.StatusOK, delSubRec.Code)

	// Delete VPC.
	delVpcRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteVpc&Version=2016-11-15&VpcId=%s",
		vpcID,
	))
	assert.Equal(t, http.StatusOK, delVpcRec.Code)
}

// ---- backend_refinement2 sorted/filtered describe helpers ----

func TestNetworking1HelperMethods(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	// DescribeLaunchTemplatesSorted.
	_, err := bk.CreateLaunchTemplate("sorted-lt-1", "ami-1", "t2.micro", nil)
	require.NoError(t, err)
	_, err = bk.CreateLaunchTemplate("sorted-lt-2", "ami-2", "t3.micro", nil)
	require.NoError(t, err)

	lts := bk.DescribeLaunchTemplatesSorted(nil)
	assert.Len(t, lts, 2)

	ltsByName := bk.DescribeLaunchTemplatesSortedByName()
	assert.Len(t, ltsByName, 2)

	// DescribeVpcEndpointsByVPC.
	vpc, err := bk.CreateVpc("10.10.0.0/16", "default")
	require.NoError(t, err)
	_ = bk.DescribeVpcEndpointsByVPC(vpc.ID)

	// DescribeNetworkAclsFiltered.
	_ = bk.DescribeNetworkAclsFiltered(nil)

	// DescribeSnapshotsSorted.
	_ = bk.DescribeSnapshotsSorted(nil)
}

func TestDhcpOptions_Tagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantKey    string
		wantVal    string
		createTags bool
		extraTags  bool
	}{
		{
			name:       "tag_specification_at_create",
			createTags: true,
			extraTags:  false,
			wantKey:    "Environment",
			wantVal:    "Production",
		},
		{
			name:       "create_tags_after_create",
			createTags: false,
			extraTags:  true,
			wantKey:    "Owner",
			wantVal:    "TeamAlpha",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			query := "Action=CreateDhcpOptions&Version=2016-11-15" +
				"&DhcpConfiguration.1.Key=domain-name&DhcpConfiguration.1.Value.1=example.com"
			if tt.createTags {
				query += "&TagSpecification.1.ResourceType=dhcp-options" +
					"&TagSpecification.1.Tag.1.Key=" + tt.wantKey +
					"&TagSpecification.1.Tag.1.Value=" + tt.wantVal
			}

			createRec := postForm(t, h, query)
			require.Equal(t, http.StatusOK, createRec.Code)
			body := createRec.Body.String()
			dhcpIDStart := indexOf(body, "<dhcpOptionsId>") + len("<dhcpOptionsId>")
			dhcpIDEnd := indexOf(body, "</dhcpOptionsId>")
			require.Greater(t, dhcpIDEnd, dhcpIDStart)
			dhcpID := body[dhcpIDStart:dhcpIDEnd]

			if tt.extraTags {
				tagRec := postForm(t, h, fmt.Sprintf(
					"Action=CreateTags&Version=2016-11-15&ResourceId.1=%s&Tag.1.Key=%s&Tag.1.Value=%s",
					dhcpID, tt.wantKey, tt.wantVal,
				))
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			descRec := postForm(t, h, "Action=DescribeDhcpOptions&Version=2016-11-15&DhcpOptionsId.1="+dhcpID)
			require.Equal(t, http.StatusOK, descRec.Code)
			descBody := descRec.Body.String()

			assert.Contains(t, descBody, "<key>"+tt.wantKey+"</key>")
			assert.Contains(t, descBody, "<value>"+tt.wantVal+"</value>")
		})
	}
}
