package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTGWPeripheralsHandler_PolicyTableLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	require.Equal(t, http.StatusOK, tgwRec.Code)
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	createRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayPolicyTable&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateTransitGatewayPolicyTableResponse")
	assert.Contains(t, createBody, "<state>available</state>")
	policyTableID := extractTag(t, createBody, "transitGatewayPolicyTableId")
	assert.Contains(t, policyTableID, "tgw-ptb-")

	descRec := postForm(t, h, "Action=DescribeTransitGatewayPolicyTables&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), policyTableID)

	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateTransitGatewayPolicyTable&Version=2016-11-15"+
			"&TransitGatewayPolicyTableId=%s&TransitGatewayAttachmentId=tgw-attach-1",
		policyTableID,
	))
	require.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assert.Contains(t, assocBody, "<AssociateTransitGatewayPolicyTableResponse")
	assert.Contains(t, assocBody, "<state>associated</state>")
	assert.Contains(t, assocBody, "<transitGatewayAttachmentId>tgw-attach-1</transitGatewayAttachmentId>")

	getAssocRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayPolicyTableAssociations&Version=2016-11-15"+
			"&TransitGatewayPolicyTableId=%s",
		policyTableID,
	))
	require.Equal(t, http.StatusOK, getAssocRec.Code)
	assert.Contains(t, getAssocRec.Body.String(), "tgw-attach-1")

	getEntriesRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayPolicyTableEntries&Version=2016-11-15&TransitGatewayPolicyTableId=%s",
		policyTableID,
	))
	require.Equal(t, http.StatusOK, getEntriesRec.Code)
	assert.Contains(t, getEntriesRec.Body.String(), "<GetTransitGatewayPolicyTableEntriesResponse")
	assert.NotContains(t, getEntriesRec.Body.String(), "<item>")

	disassocRec := postForm(t, h, fmt.Sprintf(
		"Action=DisassociateTransitGatewayPolicyTable&Version=2016-11-15"+
			"&TransitGatewayPolicyTableId=%s&TransitGatewayAttachmentId=tgw-attach-1",
		policyTableID,
	))
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "<state>disassociated</state>")

	deleteRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteTransitGatewayPolicyTable&Version=2016-11-15&TransitGatewayPolicyTableId=%s",
		policyTableID,
	))
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>deleted</state>")

	notFoundRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteTransitGatewayPolicyTable&Version=2016-11-15&TransitGatewayPolicyTableId=%s",
		policyTableID,
	))
	require.Equal(t, http.StatusBadRequest, notFoundRec.Code)
	assert.Contains(t, notFoundRec.Body.String(), "InvalidTransitGatewayPolicyTableId.NotFound")
}

func TestTGWPeripheralsHandler_RouteTableAnnouncementLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	rtRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	rtID := extractTag(t, rtRec.Body.String(), "transitGatewayRouteTableId")

	peerRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayPeeringAttachment&Version=2016-11-15"+
			"&TransitGatewayId=%s&PeerTransitGatewayId=tgw-peer-1",
		tgwID,
	))
	require.Equal(t, http.StatusOK, peerRec.Code)
	peerAttID := extractTag(t, peerRec.Body.String(), "transitGatewayAttachmentId")

	createRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayRouteTableAnnouncement&Version=2016-11-15"+
			"&TransitGatewayRouteTableId=%s&PeeringAttachmentId=%s",
		rtID, peerAttID,
	))
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateTransitGatewayRouteTableAnnouncementResponse")
	assert.Contains(t, createBody, "<announcementDirection>outgoing</announcementDirection>")
	annID := extractTag(t, createBody, "transitGatewayRouteTableAnnouncementId")
	assert.Contains(t, annID, "tgw-rtb-ann-")

	descRec := postForm(t, h, "Action=DescribeTransitGatewayRouteTableAnnouncements&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), annID)

	deleteRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteTransitGatewayRouteTableAnnouncement&Version=2016-11-15"+
			"&TransitGatewayRouteTableAnnouncementId=%s",
		annID,
	))
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>deleted</state>")
}

func TestTGWPeripheralsHandler_RouteTableAssociationsPropagationsSearchExport(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	rtRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayRouteTable&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	rtID := extractTag(t, rtRec.Body.String(), "transitGatewayRouteTableId")

	attRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayId=%s&VpcId=vpc-default",
		tgwID,
	))
	attID := extractTag(t, attRec.Body.String(), "transitGatewayAttachmentId")

	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateTransitGatewayRouteTable&Version=2016-11-15"+
			"&TransitGatewayRouteTableId=%s&TransitGatewayAttachmentId=%s",
		rtID, attID,
	))
	require.Equal(t, http.StatusOK, assocRec.Code)

	getAssocRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayRouteTableAssociations&Version=2016-11-15&TransitGatewayRouteTableId=%s",
		rtID,
	))
	require.Equal(t, http.StatusOK, getAssocRec.Code)
	assert.Contains(t, getAssocRec.Body.String(), "<GetTransitGatewayRouteTableAssociationsResponse")
	assert.Contains(t, getAssocRec.Body.String(), attID)

	getPropRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayRouteTablePropagations&Version=2016-11-15&TransitGatewayRouteTableId=%s",
		rtID,
	))
	require.Equal(t, http.StatusOK, getPropRec.Code)
	assert.Contains(t, getPropRec.Body.String(), "<GetTransitGatewayRouteTablePropagationsResponse")
	assert.NotContains(t, getPropRec.Body.String(), "<item>")

	getAttPropRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayAttachmentPropagations&Version=2016-11-15&TransitGatewayAttachmentId=%s",
		attID,
	))
	require.Equal(t, http.StatusOK, getAttPropRec.Code)
	assert.Contains(t, getAttPropRec.Body.String(), "<GetTransitGatewayAttachmentPropagationsResponse")

	routeRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayRoute&Version=2016-11-15"+
			"&TransitGatewayRouteTableId=%s&DestinationCidrBlock=10.0.0.0/24"+
			"&TransitGatewayAttachmentId=%s",
		rtID, attID,
	))
	require.Equal(t, http.StatusOK, routeRec.Code)

	searchRec := postForm(t, h, fmt.Sprintf(
		"Action=SearchTransitGatewayRoutes&Version=2016-11-15&TransitGatewayRouteTableId=%s"+
			"&Filter.1.Name=route-search.exact-match&Filter.1.Value.1=10.0.0.0/24",
		rtID,
	))
	require.Equal(t, http.StatusOK, searchRec.Code)
	assert.Contains(t, searchRec.Body.String(), "<SearchTransitGatewayRoutesResponse")
	assert.Contains(t, searchRec.Body.String(), "10.0.0.0/24")

	exportRec := postForm(t, h, fmt.Sprintf(
		"Action=ExportTransitGatewayRoutes&Version=2016-11-15"+
			"&TransitGatewayRouteTableId=%s&S3Bucket=my-bucket",
		rtID,
	))
	require.Equal(t, http.StatusOK, exportRec.Code)
	assert.Contains(t, exportRec.Body.String(), "s3://my-bucket/VPCTransitGateway/TransitGatewayRouteTables/")
}

func TestTGWPeripheralsHandler_ModifyVpcAttachment(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	attRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayId=%s&VpcId=vpc-default",
		tgwID,
	))
	require.Equal(t, http.StatusOK, attRec.Code)
	attID := extractTag(t, attRec.Body.String(), "transitGatewayAttachmentId")

	modifyRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyTransitGatewayVpcAttachment&Version=2016-11-15"+
			"&TransitGatewayAttachmentId=%s&AddSubnetIds.1=subnet-1&AddSubnetIds.2=subnet-2",
		attID,
	))
	require.Equal(t, http.StatusOK, modifyRec.Code)
	modifyBody := modifyRec.Body.String()
	assert.Contains(t, modifyBody, "<ModifyTransitGatewayVpcAttachmentResponse")
	assert.Contains(t, modifyBody, "subnet-1")
	assert.Contains(t, modifyBody, "subnet-2")
}

func TestTGWPeripheralsHandler_ModifyMeteringPolicyAndGetEntries(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	policyRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayMeteringPolicy&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	require.Equal(t, http.StatusOK, policyRec.Code)
	policyID := extractTag(t, policyRec.Body.String(), "transitGatewayMeteringPolicyId")

	modifyRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyTransitGatewayMeteringPolicy&Version=2016-11-15"+
			"&TransitGatewayMeteringPolicyId=%s&AddMiddleboxAttachmentIds.1=tgw-attach-1",
		policyID,
	))
	require.Equal(t, http.StatusOK, modifyRec.Code)
	assert.Contains(t, modifyRec.Body.String(), "tgw-attach-1")

	entriesRec := postForm(t, h, fmt.Sprintf(
		"Action=GetTransitGatewayMeteringPolicyEntries&Version=2016-11-15&TransitGatewayMeteringPolicyId=%s",
		policyID,
	))
	require.Equal(t, http.StatusOK, entriesRec.Code)
	assert.Contains(t, entriesRec.Body.String(), "<GetTransitGatewayMeteringPolicyEntriesResponse")
}

func TestTGWPeripheralsHandler_ModifyPrefixListReference(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(t, h, "Action=CreateTransitGatewayPrefixListReference&Version=2016-11-15"+
		"&TransitGatewayRouteTableId=tgw-rtb-111&PrefixListId=pl-abc123")
	require.Equal(t, http.StatusOK, createRec.Code)

	modifyRec := postForm(t, h, "Action=ModifyTransitGatewayPrefixListReference&Version=2016-11-15"+
		"&TransitGatewayRouteTableId=tgw-rtb-111&PrefixListId=pl-abc123"+
		"&TransitGatewayAttachmentId=tgw-attach-1&Blackhole=true")
	require.Equal(t, http.StatusOK, modifyRec.Code)
	modifyBody := modifyRec.Body.String()
	assert.Contains(t, modifyBody, "<ModifyTransitGatewayPrefixListReferenceResponse")
	assert.Contains(t, modifyBody, "<blackhole>true</blackhole>")
	assert.Contains(t, modifyBody, "<transitGatewayAttachmentId>tgw-attach-1</transitGatewayAttachmentId>")
}

func TestTGWPeripheralsHandler_RejectOps(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tgwRec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15")
	tgwID := extractTag(t, tgwRec.Body.String(), "transitGatewayId")

	attRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayId=%s&VpcId=vpc-default",
		tgwID,
	))
	attID := extractTag(t, attRec.Body.String(), "transitGatewayAttachmentId")

	rejectVpcRec := postForm(t, h, fmt.Sprintf(
		"Action=RejectTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayAttachmentId=%s",
		attID,
	))
	require.Equal(t, http.StatusOK, rejectVpcRec.Code)
	assert.Contains(t, rejectVpcRec.Body.String(), "<state>rejected</state>")

	peerRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayPeeringAttachment&Version=2016-11-15"+
			"&TransitGatewayId=%s&PeerTransitGatewayId=tgw-peer-1",
		tgwID,
	))
	peerAttID := extractTag(t, peerRec.Body.String(), "transitGatewayAttachmentId")

	rejectPeerRec := postForm(t, h, fmt.Sprintf(
		"Action=RejectTransitGatewayPeeringAttachment&Version=2016-11-15&TransitGatewayAttachmentId=%s",
		peerAttID,
	))
	require.Equal(t, http.StatusOK, rejectPeerRec.Code)
	assert.Contains(t, rejectPeerRec.Body.String(), "<state>rejected</state>")

	domainRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTransitGatewayMulticastDomain&Version=2016-11-15&TransitGatewayId=%s",
		tgwID,
	))
	domainID := extractTag(t, domainRec.Body.String(), "transitGatewayMulticastDomainId")

	_ = postForm(t, h, fmt.Sprintf(
		"Action=AssociateTransitGatewayMulticastDomain&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s&TransitGatewayAttachmentId=tgw-attach-x&SubnetIds.1=subnet-1",
		domainID,
	))

	rejectMcastRec := postForm(t, h, fmt.Sprintf(
		"Action=RejectTransitGatewayMulticastDomainAssociations&Version=2016-11-15"+
			"&TransitGatewayMulticastDomainId=%s&TransitGatewayAttachmentId=tgw-attach-x&SubnetIds.1=subnet-1",
		domainID,
	))
	require.Equal(t, http.StatusOK, rejectMcastRec.Code)
	assert.Contains(t, rejectMcastRec.Body.String(), "<state>rejected</state>")
}

func TestTGWPeripheralsHandler_NotFoundErrors(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tests := []struct {
		name       string
		body       string
		wantErrMsg string
	}{
		{
			name:       "GetTransitGatewayPolicyTableEntries unknown table",
			body:       "Action=GetTransitGatewayPolicyTableEntries&Version=2016-11-15&TransitGatewayPolicyTableId=tgw-ptb-x",
			wantErrMsg: "InvalidTransitGatewayPolicyTableId.NotFound",
		},
		{
			name: "DeleteTransitGatewayRouteTableAnnouncement unknown announcement",
			body: "Action=DeleteTransitGatewayRouteTableAnnouncement&Version=2016-11-15" +
				"&TransitGatewayRouteTableAnnouncementId=tgw-rtb-ann-x",
			wantErrMsg: "InvalidTransitGatewayRouteTableAnnouncementId.NotFound",
		},
		{
			name:       "GetTransitGatewayRouteTableAssociations unknown route table",
			body:       "Action=GetTransitGatewayRouteTableAssociations&Version=2016-11-15&TransitGatewayRouteTableId=tgw-rtb-x",
			wantErrMsg: "InvalidTransitGatewayRouteTableId.NotFound",
		},
		{
			name:       "ModifyTransitGatewayVpcAttachment unknown attachment",
			body:       "Action=ModifyTransitGatewayVpcAttachment&Version=2016-11-15&TransitGatewayAttachmentId=tgw-attach-x",
			wantErrMsg: "InvalidTransitGatewayAttachmentID.NotFound",
		},
		{
			name: "ModifyTransitGatewayMeteringPolicy unknown policy",
			body: "Action=ModifyTransitGatewayMeteringPolicy&Version=2016-11-15" +
				"&TransitGatewayMeteringPolicyId=tgw-metering-policy-x",
			wantErrMsg: "InvalidTransitGatewayMeteringPolicyId.NotFound",
		},
		{
			name: "ModifyTransitGatewayPrefixListReference unknown reference",
			body: "Action=ModifyTransitGatewayPrefixListReference&Version=2016-11-15" +
				"&TransitGatewayRouteTableId=tgw-rtb-x&PrefixListId=pl-x",
			wantErrMsg: "InvalidTransitGatewayPrefixListReferenceId.NotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
		})
	}
}
