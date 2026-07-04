package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// newLocalGatewayHandler creates a handler backed by a backend seeded with one
// local gateway, one virtual interface group, and one virtual interface.
func newLocalGatewayHandler(t *testing.T) (*ec2.Handler, *ec2.LocalGateway, *ec2.LocalGatewayVirtualInterfaceGroup) {
	t.Helper()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{
		OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1",
	})
	require.NoError(t, err)

	group, err := bk.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)

	_, err = bk.SeedLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterface{
		LocalGatewayID:                      lg.LocalGatewayID,
		LocalGatewayVirtualInterfaceGroupID: group.LocalGatewayVirtualInterfaceGroupID,
		Vlan:                                42,
	})
	require.NoError(t, err)

	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	return h, lg, group
}

func TestHandler_DescribeLocalGateways(t *testing.T) {
	t.Parallel()

	h, lg, _ := newLocalGatewayHandler(t)

	rec := postForm(t, h, "Action=DescribeLocalGateways&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "DescribeLocalGatewaysResponse")
	assert.Contains(t, body, "<localGatewaySet>")
	assert.Contains(t, body, "<localGatewayId>"+lg.LocalGatewayID+"</localGatewayId>")
	assert.Contains(t, body, "<outpostArn>"+lg.OutpostArn+"</outpostArn>")
	assert.Contains(t, body, "<state>available</state>")
}

func TestHandler_DescribeLocalGateways_Empty(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=DescribeLocalGateways&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "DescribeLocalGatewaysResponse")
	assert.Contains(t, body, "<localGatewaySet></localGatewaySet>")
}

func TestHandler_DescribeLocalGatewayVirtualInterfacesAndGroups(t *testing.T) {
	t.Parallel()

	h, _, group := newLocalGatewayHandler(t)

	rec := postForm(t, h, "Action=DescribeLocalGatewayVirtualInterfaces&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DescribeLocalGatewayVirtualInterfacesResponse")
	assert.Contains(t, body, "<localGatewayVirtualInterfaceSet>")
	assert.Contains(t, body, "<vlan>42</vlan>")

	rec = postForm(t, h, "Action=DescribeLocalGatewayVirtualInterfaceGroups&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, "DescribeLocalGatewayVirtualInterfaceGroupsResponse")
	assert.Contains(t, body, "<localGatewayVirtualInterfaceGroupId>"+group.LocalGatewayVirtualInterfaceGroupID+
		"</localGatewayVirtualInterfaceGroupId>")
}

func TestHandler_LocalGatewayRouteTableLifecycle(t *testing.T) {
	t.Parallel()

	h, lg, _ := newLocalGatewayHandler(t)

	rec := postForm(
		t,
		h,
		"Action=CreateLocalGatewayRouteTable&Version=2016-11-15&LocalGatewayId="+lg.LocalGatewayID+
			"&Mode=direct-vpc-routing",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CreateLocalGatewayRouteTableResponse")
	assert.Contains(t, body, "<mode>direct-vpc-routing</mode>")

	rtID := extractXMLField(body, "localGatewayRouteTableId")
	require.NotEmpty(t, rtID)

	rec = postForm(t, h, "Action=DescribeLocalGatewayRouteTables&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<localGatewayRouteTableId>"+rtID+"</localGatewayRouteTableId>")

	rec = postForm(
		t,
		h,
		"Action=DeleteLocalGatewayRouteTable&Version=2016-11-15&LocalGatewayRouteTableId="+rtID,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteLocalGatewayRouteTableResponse")

	// Deleting again returns an error response (not-found).
	rec = postForm(
		t,
		h,
		"Action=DeleteLocalGatewayRouteTable&Version=2016-11-15&LocalGatewayRouteTableId="+rtID,
	)
	assert.NotEqual(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidLocalGatewayRouteTableID.NotFound")
}

func TestHandler_LocalGatewayRouteLifecycle(t *testing.T) {
	t.Parallel()

	h, lg, _ := newLocalGatewayHandler(t)

	rec := postForm(
		t,
		h,
		"Action=CreateLocalGatewayRouteTable&Version=2016-11-15&LocalGatewayId="+lg.LocalGatewayID+"&Mode=coip",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rtID := extractXMLField(rec.Body.String(), "localGatewayRouteTableId")
	require.NotEmpty(t, rtID)

	rec = postForm(
		t,
		h,
		"Action=CreateLocalGatewayRoute&Version=2016-11-15&LocalGatewayRouteTableId="+rtID+
			"&DestinationCidrBlock=10.0.0.0/24&NetworkInterfaceId=eni-abc123",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CreateLocalGatewayRouteResponse")
	assert.Contains(t, body, "<destinationCidrBlock>10.0.0.0/24</destinationCidrBlock>")
	assert.Contains(t, body, "<networkInterfaceId>eni-abc123</networkInterfaceId>")
	assert.Contains(t, body, "<type>static</type>")
	assert.Contains(t, body, "<state>active</state>")

	rec = postForm(
		t,
		h,
		"Action=SearchLocalGatewayRoutes&Version=2016-11-15&LocalGatewayRouteTableId="+rtID,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, "SearchLocalGatewayRoutesResponse")
	assert.Contains(t, body, "<routeSet>")
	assert.Contains(t, body, "10.0.0.0/24")

	rec = postForm(
		t,
		h,
		"Action=ModifyLocalGatewayRoute&Version=2016-11-15&LocalGatewayRouteTableId="+rtID+
			"&DestinationCidrBlock=10.0.0.0/24&NetworkInterfaceId=eni-xyz789",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<networkInterfaceId>eni-xyz789</networkInterfaceId>")

	rec = postForm(
		t,
		h,
		"Action=DeleteLocalGatewayRoute&Version=2016-11-15&LocalGatewayRouteTableId="+rtID+
			"&DestinationCidrBlock=10.0.0.0/24",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteLocalGatewayRouteResponse")
}

func TestHandler_LocalGatewayRouteTableVpcAssociationLifecycle(t *testing.T) {
	t.Parallel()

	h, lg, _ := newLocalGatewayHandler(t)

	rec := postForm(
		t,
		h,
		"Action=CreateLocalGatewayRouteTable&Version=2016-11-15&LocalGatewayId="+lg.LocalGatewayID+"&Mode=coip",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rtID := extractXMLField(rec.Body.String(), "localGatewayRouteTableId")
	require.NotEmpty(t, rtID)

	rec = postForm(t, h, "Action=CreateVpc&Version=2016-11-15&CidrBlock=10.9.0.0/16")
	require.Equal(t, http.StatusOK, rec.Code)
	vpcID := extractXMLField(rec.Body.String(), "vpcId")
	require.NotEmpty(t, vpcID)

	rec = postForm(
		t,
		h,
		"Action=CreateLocalGatewayRouteTableVpcAssociation&Version=2016-11-15&LocalGatewayRouteTableId="+
			rtID+"&VpcId="+vpcID,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CreateLocalGatewayRouteTableVpcAssociationResponse")
	assert.Contains(t, body, "<localGatewayRouteTableVpcAssociation>")
	assert.Contains(t, body, "<vpcId>"+vpcID+"</vpcId>")
	assert.Contains(t, body, "<state>associated</state>")

	assocID := extractXMLField(body, "localGatewayRouteTableVpcAssociationId")
	require.NotEmpty(t, assocID)

	rec = postForm(t, h, "Action=DescribeLocalGatewayRouteTableVpcAssociations&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), assocID)

	rec = postForm(
		t,
		h,
		"Action=DeleteLocalGatewayRouteTableVpcAssociation&Version=2016-11-15&"+
			"LocalGatewayRouteTableVpcAssociationId="+assocID,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<state>disassociated</state>")
}

func TestHandler_LocalGatewayRouteTableVifGroupAssociationLifecycle(t *testing.T) {
	t.Parallel()

	h, lg, group := newLocalGatewayHandler(t)

	rec := postForm(
		t,
		h,
		"Action=CreateLocalGatewayRouteTable&Version=2016-11-15&LocalGatewayId="+lg.LocalGatewayID+"&Mode=coip",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rtID := extractXMLField(rec.Body.String(), "localGatewayRouteTableId")
	require.NotEmpty(t, rtID)

	rec = postForm(
		t,
		h,
		"Action=CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation&Version=2016-11-15&"+
			"LocalGatewayRouteTableId="+rtID+"&LocalGatewayVirtualInterfaceGroupId="+
			group.LocalGatewayVirtualInterfaceGroupID,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociationResponse")
	assert.Contains(t, body, "<localGatewayRouteTableVirtualInterfaceGroupAssociation>")
	assert.Contains(t, body, "<state>associated</state>")

	assocID := extractXMLField(body, "localGatewayRouteTableVirtualInterfaceGroupAssociationId")
	require.NotEmpty(t, assocID)

	rec = postForm(
		t,
		h,
		"Action=DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations&Version=2016-11-15",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), assocID)

	rec = postForm(
		t,
		h,
		"Action=DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation&Version=2016-11-15&"+
			"LocalGatewayRouteTableVirtualInterfaceGroupAssociationId="+assocID,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<state>disassociated</state>")
}
