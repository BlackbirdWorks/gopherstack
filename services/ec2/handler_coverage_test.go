package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestHandlerDeleteEgressOnlyInternetGateway covers handleDeleteEgressOnlyInternetGateway.
func TestHandlerDeleteEgressOnlyInternetGateway(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// First create a VPC and IGW.
	rec := postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.5.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	// Extract vpc ID from response.
	vpcID := extractXMLField(body, "vpcId")
	require.NotEmpty(t, vpcID)

	rec = postForm(t, h, "Action=CreateEgressOnlyInternetGateway&Version=2016-11-15&VpcId="+vpcID)
	require.Equal(t, http.StatusOK, rec.Code)
	igwID := extractXMLField(rec.Body.String(), "egressOnlyInternetGatewayId")
	require.NotEmpty(t, igwID)

	// Delete it.
	rec = postForm(
		t,
		h,
		"Action=DeleteEgressOnlyInternetGateway&Version=2016-11-15&EgressOnlyInternetGatewayId="+igwID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteEgressOnlyInternetGatewayResponse")

	// Delete not found.
	rec = postForm(
		t,
		h,
		"Action=DeleteEgressOnlyInternetGateway&Version=2016-11-15&EgressOnlyInternetGatewayId=eigw-notfound",
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerReplaceRouteTableAssociation covers handleReplaceRouteTableAssociation
// and ReplaceRouteTableAssociation backend.
func TestHandlerReplaceRouteTableAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create a VPC, subnet, and two route tables.
	vpc, err := b.CreateVpc("10.6.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.6.1.0/24", "us-east-1a")
	require.NoError(t, err)

	rt1, err := b.CreateRouteTable(vpc.ID)
	require.NoError(t, err)

	rt2, err := b.CreateRouteTable(vpc.ID)
	require.NoError(t, err)

	// Associate subnet with rt1.
	assocID, err := b.AssociateRouteTable(rt1.ID, subnet.ID)
	require.NoError(t, err)
	require.NotEmpty(t, assocID)

	// Replace association to point at rt2 via handler.
	rec := postForm(
		t,
		h,
		"Action=ReplaceRouteTableAssociation&Version=2016-11-15&AssociationId="+assocID+"&RouteTableId="+rt2.ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceRouteTableAssociationResponse")

	// Missing params.
	rec = postForm(t, h, "Action=ReplaceRouteTableAssociation&Version=2016-11-15")
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerDeleteTransitGatewayRouteTable covers handleDeleteTransitGatewayRouteTable.
func TestHandlerDeleteTransitGatewayRouteTable(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	rec := postForm(
		t,
		h,
		"Action=DeleteTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayRouteTableId="+rt.RouteTableID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayRouteTableResponse")

	// Not found.
	rec = postForm(
		t,
		h,
		"Action=DeleteTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayRouteTableId=tgw-rtb-notfound",
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerTGWRoutes covers handleCreateTransitGatewayRoute, handleDeleteTransitGatewayRoute,
// handleReplaceTransitGatewayRoute, tgwRouteToItem.
func TestHandlerTGWRoutes(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	rtID := rt.RouteTableID

	// Create a route.
	rec := postForm(t, h, "Action=CreateTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTransitGatewayRouteResponse")

	// Replace route.
	rec = postForm(t, h, "Action=ReplaceTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceTransitGatewayRouteResponse")

	// Delete route.
	rec = postForm(t, h, "Action=DeleteTransitGatewayRoute&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&DestinationCidrBlock=10.100.0.0/16")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayRouteResponse")
}

// TestHandlerTGWRouteTableAssociation covers handleAssociateTransitGatewayRouteTable
// and handleDisassociateTransitGatewayRouteTable.
func TestHandlerTGWRouteTableAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	rtID := rt.RouteTableID

	vpc, err := b.CreateVpc("10.7.0.0/16")
	require.NoError(t, err)

	attach, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, vpc.ID, []string{})
	require.NoError(t, err)
	attachID := attach.TransitGatewayAttachmentID

	// Associate.
	rec := postForm(t, h, "Action=AssociateTransitGatewayRouteTable&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&TransitGatewayAttachmentId="+attachID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AssociateTransitGatewayRouteTableResponse")

	// Disassociate.
	rec = postForm(t, h, "Action=DisassociateTransitGatewayRouteTable&Version=2016-11-15"+
		"&TransitGatewayRouteTableId="+rtID+
		"&TransitGatewayAttachmentId="+attachID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DisassociateTransitGatewayRouteTableResponse")
}

// TestHandlerModifyTransitGatewayAttribute covers handleModifyTransitGatewayAttribute.
func TestHandlerModifyTransitGatewayAttribute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("original-desc")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ModifyTransitGatewayAttribute&Version=2016-11-15"+
		"&TransitGatewayId="+tgw.ID+
		"&Description=updated-desc")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ModifyTransitGatewayAttributeResponse")
}

// TestHandlerDeleteFlowLogs covers handleDeleteFlowLogs.
func TestHandlerDeleteFlowLogs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create flow logs first via backend.
	vpc, err := b.CreateVpc("10.8.0.0/16")
	require.NoError(t, err)

	logs, err := b.CreateFlowLogs([]string{vpc.ID}, "ALL", "cloud-watch-logs", "/aws/vpc/flow")
	require.NoError(t, err)
	require.Len(t, logs, 1)
	logID := logs[0].FlowLogID

	rec := postForm(t, h, "Action=DeleteFlowLogs&Version=2016-11-15&FlowLogId.1="+logID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteFlowLogsResponse")
}

// TestHandlerLaunchTemplateVersions covers handleCreateLaunchTemplateVersion,
// handleDeleteLaunchTemplateVersions, handleGetLaunchTemplateData,
// handleDescribeLaunchTemplateVersions.
func TestHandlerLaunchTemplateVersions(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create a launch template.
	lt, err := b.CreateLaunchTemplate("test-lt", "ami-12345", "t3.micro")
	require.NoError(t, err)
	ltID := lt.ID

	// Create a new version.
	rec := postForm(t, h, "Action=CreateLaunchTemplateVersion&Version=2016-11-15"+
		"&LaunchTemplateId="+ltID+
		"&LaunchTemplateData.ImageId=ami-99999"+
		"&LaunchTemplateData.InstanceType=t3.small")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateLaunchTemplateVersionResponse")

	// Describe versions.
	rec = postForm(
		t,
		h,
		"Action=DescribeLaunchTemplateVersions&Version=2016-11-15&LaunchTemplateId="+ltID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeLaunchTemplateVersionsResponse")

	// Get launch template data from an instance.
	insts, err := b.RunInstances("ami-12345", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := insts[0].ID

	rec = postForm(t, h, "Action=GetLaunchTemplateData&Version=2016-11-15&InstanceId="+instanceID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetLaunchTemplateDataResponse")

	// Delete a version.
	rec = postForm(t, h, "Action=DeleteLaunchTemplateVersions&Version=2016-11-15"+
		"&LaunchTemplateId="+ltID+
		"&LaunchTemplateVersion.1=2")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteLaunchTemplateVersionsResponse")
}

// TestHandlerDeleteSnapshot covers handleDeleteSnapshot.
func TestHandlerDeleteSnapshot(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create a volume and snapshot.
	vol, err := b.CreateVolume("us-east-1a", "gp2", 10)
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(vol.ID, "test snap")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=DeleteSnapshot&Version=2016-11-15&SnapshotId="+snap.SnapshotID)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found.
	rec = postForm(t, h, "Action=DeleteSnapshot&Version=2016-11-15&SnapshotId=snap-notfound")
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerDeregisterImage covers handleDeregisterImage.
func TestHandlerDeregisterImage(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create instance and image.
	insts, err := b.RunInstances("ami-base", "t3.micro", "", 1)
	require.NoError(t, err)

	img, err := b.CreateImage(insts[0].ID, "my-ami", "test image")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=DeregisterImage&Version=2016-11-15&ImageId="+img.ImageID)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found.
	rec = postForm(t, h, "Action=DeregisterImage&Version=2016-11-15&ImageId=ami-notfound")
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerModifySubnetAttribute covers handleModifySubnetAttribute.
func TestHandlerModifySubnetAttribute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.9.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.9.1.0/24", "us-east-1a")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ModifySubnetAttribute&Version=2016-11-15"+
		"&SubnetId="+subnet.ID+
		"&MapPublicIpOnLaunch.Value=true")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Default branch (no MapPublicIpOnLaunch.Value).
	rec = postForm(t, h, "Action=ModifySubnetAttribute&Version=2016-11-15&SubnetId="+subnet.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerNetworkACLHandlers covers handleDeleteNetworkACL, handleCreateNetworkACLEntry,
// handleDeleteNetworkACLEntry, handleModifySecurityGroupRules.
func TestHandlerNetworkACLHandlers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.10.0.0/16")
	require.NoError(t, err)

	acl, err := b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)
	aclID := acl.ID

	// Create NACL entry.
	rec := postForm(t, h, "Action=CreateNetworkAclEntry&Version=2016-11-15"+
		"&NetworkAclId="+aclID+
		"&RuleNumber=100"+
		"&Protocol=6"+
		"&RuleAction=allow"+
		"&CidrBlock=0.0.0.0/0"+
		"&Egress=false"+
		"&PortRange.From=80"+
		"&PortRange.To=80")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete NACL entry.
	rec = postForm(t, h, "Action=DeleteNetworkAclEntry&Version=2016-11-15"+
		"&NetworkAclId="+aclID+
		"&RuleNumber=100"+
		"&Egress=false")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete NACL.
	rec = postForm(t, h, "Action=DeleteNetworkAcl&Version=2016-11-15&NetworkAclId="+aclID)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerModifySecurityGroupRules covers handleModifySecurityGroupRules.
func TestHandlerModifySecurityGroupRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.11.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("test-sg", "test", vpc.ID)
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ModifySecurityGroupRules&Version=2016-11-15"+
		"&GroupId="+sg.ID+
		"&Egress=false"+
		"&IpPermissions.1.IpProtocol=tcp"+
		"&IpPermissions.1.FromPort=443"+
		"&IpPermissions.1.ToPort=443"+
		"&IpPermissions.1.IpRanges.1.CidrIp=10.0.0.0/8")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerReplaceNetworkACLAssociation covers handleReplaceNetworkACLAssociation.
func TestHandlerReplaceNetworkACLAssociation(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.12.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.12.1.0/24", "us-east-1a")
	require.NoError(t, err)

	acl, err := b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ReplaceNetworkAclAssociation&Version=2016-11-15"+
		"&NetworkAclId="+acl.ID+
		"&SubnetId="+subnet.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReplaceNetworkAclAssociationResponse")
}

// TestHandlerDescribeInstanceTypeOfferings covers handleDescribeInstanceTypeOfferings.
func TestHandlerDescribeInstanceTypeOfferings(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeInstanceTypeOfferings&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeInstanceTypeOfferingsResponse")
}

// TestHandlerVpcPeeringConnectionHandlers covers handleCreateVpcPeeringConnection,
// handleDeleteVpcPeeringConnection, and handleRejectVpcPeeringConnection.
func TestHandlerVpcPeeringConnectionHandlers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc1, err := b.CreateVpc("10.20.0.0/16")
	require.NoError(t, err)
	vpc2, err := b.CreateVpc("10.21.0.0/16")
	require.NoError(t, err)

	// Create peering connection.
	rec := postForm(t, h, "Action=CreateVpcPeeringConnection&Version=2016-11-15"+
		"&VpcId="+vpc1.ID+
		"&PeerVpcId="+vpc2.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateVpcPeeringConnectionResponse")
	pcID := extractXMLField(rec.Body.String(), "vpcPeeringConnectionId")
	require.NotEmpty(t, pcID)

	// Create another for reject test.
	vpc3, err := b.CreateVpc("10.22.0.0/16")
	require.NoError(t, err)
	pc2, err := b.CreateVpcPeeringConnection(vpc1.ID, vpc3.ID)
	require.NoError(t, err)

	// Reject it.
	rec = postForm(
		t,
		h,
		"Action=RejectVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId="+pc2.VpcPeeringConnectionID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RejectVpcPeeringConnectionResponse")

	// Delete the first one.
	rec = postForm(
		t,
		h,
		"Action=DeleteVpcPeeringConnection&Version=2016-11-15&VpcPeeringConnectionId="+pcID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteVpcPeeringConnectionResponse")
}

// TestHandlerDescribeTransitGatewaysAndDelete covers handleDescribeTransitGateways
// and handleDeleteTransitGateway.
func TestHandlerDescribeTransitGatewaysAndDelete(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	tgw, err := b.CreateTransitGateway("test-describe-tgw")
	require.NoError(t, err)

	// Describe with ID filter.
	rec := postForm(
		t,
		h,
		"Action=DescribeTransitGateways&Version=2016-11-15&TransitGatewayIds.TransitGatewayId.1="+tgw.ID,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeTransitGatewaysResponse")
	assert.Contains(t, rec.Body.String(), tgw.ID)

	// Describe all (no ID filter).
	rec = postForm(t, h, "Action=DescribeTransitGateways&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete.
	rec = postForm(t, h, "Action=DeleteTransitGateway&Version=2016-11-15&TransitGatewayId="+tgw.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteTransitGatewayResponse")

	// Delete not found.
	rec = postForm(
		t,
		h,
		"Action=DeleteTransitGateway&Version=2016-11-15&TransitGatewayId=tgw-notfound",
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// extractXMLField extracts a simple XML element value from a string.
// It looks for <tag>value</tag> patterns.
func extractXMLField(body, field string) string {
	open := "<" + field + ">"
	closeTag := "</" + field + ">"

	start := indexOf(body, open)
	if start < 0 {
		return ""
	}

	valStart := start + len(open)
	end := indexOf(body[valStart:], closeTag)

	if end < 0 {
		return ""
	}

	return body[valStart : valStart+end]
}
