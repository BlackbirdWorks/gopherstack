package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_TransitGatewayAndLegacyTasks covers ec2's last tier of uncovered ops
// (gopherstack-n3zi): the Transit Gateway peripheral family
// (peering/VPC/Connect/multicast/client-vpn attachment accept-reject-delete,
// policy tables, route table announcements/propagation/routes, metering
// policies, prefix list references), IPAM prefix-list-resolver/BYOASN/
// resource-discovery, legacy bundle/conversion/export/import tasks, FPGA
// images, and the remaining singletons. Each row gets its own fresh
// handler+backend.
func TestRealClient_TransitGatewayAndLegacyTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client)
		name string
	}{
		{runTGWAttachmentAcceptReject, "tgw_attachment_accept_reject"},
		{runTGWPolicyTableFamily, "tgw_policy_table_family"},
		{runTGWRouteTableFamily, "tgw_route_table_family"},
		{runTGWRouteTableAnnouncements, "tgw_route_table_announcements"},
		{runTGWPrefixListAndMeteringPolicy, "tgw_prefix_list_and_metering_policy"},
		{runTGWConnectAndClientVpn, "tgw_connect_and_client_vpn"},
		{runTGWMulticastAcceptRejectAndGroups, "tgw_multicast_accept_reject_and_groups"},
		{runIpamPrefixListResolverFamily, "ipam_prefix_list_resolver_family"},
		{runIpamByoasnAndDiscoveryFamily, "ipam_byoasn_and_discovery_family"},
		{runLegacyBundleConversionExportImport, "legacy_bundle_conversion_export_import"},
		{runFpgaImageFamily, "fpga_image_family"},
		{runSingletonsA, "singletons_a"},
		{runSingletonsB, "singletons_b"},
		{runSingletonsC, "singletons_c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			h := ec2.NewHandler(backend)
			client := newTestEC2Client(t, h)
			tt.run(t, backend, client)
		})
	}
}

// ---- shared setup helpers ----

// setupTGWWithTwoVPCAttachments creates a transit gateway with two VPC
// attachments (each with its own VPC/subnet), matching this slice's task
// instruction to build the shared TGW setup once.
func setupTGWWithTwoVPCAttachments(
	t *testing.T, backend *ec2.InMemoryBackend,
) (*ec2.TransitGateway, *ec2.TransitGatewayVpcAttachment, *ec2.TransitGatewayVpcAttachment) {
	t.Helper()

	tgw, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "slice26-tgw"})
	require.NoError(t, err)

	vpc1, err := backend.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)
	subnet1, err := backend.CreateSubnet(vpc1.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)
	att1, err := backend.CreateTransitGatewayVpcAttachment(tgw.ID, vpc1.ID, []string{subnet1.ID}, nil)
	require.NoError(t, err)

	vpc2, err := backend.CreateVpc("10.1.0.0/16", "default")
	require.NoError(t, err)
	subnet2, err := backend.CreateSubnet(vpc2.ID, "10.1.1.0/24", "us-east-1a")
	require.NoError(t, err)
	att2, err := backend.CreateTransitGatewayVpcAttachment(tgw.ID, vpc2.ID, []string{subnet2.ID}, nil)
	require.NoError(t, err)

	return tgw, att1, att2
}

// setupInstanceWithVolume launches an instance and attaches a fresh EBS
// volume to it, matching this slice's task instruction to build the shared
// instance+volume setup once.
func setupInstanceWithVolume(t *testing.T, backend *ec2.InMemoryBackend) (string, string) {
	t.Helper()

	instances, err := backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	vol, err := backend.CreateVolume("us-east-1a", "", 8, "")
	require.NoError(t, err)
	_, err = backend.AttachVolume(vol.ID, instances[0].ID, "/dev/xvdf")
	require.NoError(t, err)

	return instances[0].ID, vol.ID
}

func smithyErrCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

// ---- TGW attachment accept/reject ----

// runTGWAttachmentAcceptReject covers AcceptTransitGatewayVpcAttachment,
// RejectTransitGatewayVpcAttachment, AcceptTransitGatewayPeeringAttachment,
// RejectTransitGatewayPeeringAttachment, DeleteTransitGatewayPeeringAttachment.
func runTGWAttachmentAcceptReject(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	_, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	acceptOut, err := client.AcceptTransitGatewayVpcAttachment(
		t.Context(),
		&ec2sdk.AcceptTransitGatewayVpcAttachmentInput{
			TransitGatewayAttachmentId: aws.String(att1.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, acceptOut.TransitGatewayVpcAttachment)
	assert.Equal(
		t, att1.TransitGatewayAttachmentID,
		aws.ToString(acceptOut.TransitGatewayVpcAttachment.TransitGatewayAttachmentId),
	)
	assert.Equal(t, types.TransitGatewayAttachmentStateAvailable, acceptOut.TransitGatewayVpcAttachment.State)

	rejectOut, err := client.RejectTransitGatewayVpcAttachment(
		t.Context(),
		&ec2sdk.RejectTransitGatewayVpcAttachmentInput{
			TransitGatewayAttachmentId: aws.String(att2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, rejectOut.TransitGatewayVpcAttachment)
	assert.Equal(t, types.TransitGatewayAttachmentStateRejected, rejectOut.TransitGatewayVpcAttachment.State)

	peerTGW, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "peer-tgw"})
	require.NoError(t, err)

	peering1, err := backend.CreateTransitGatewayPeeringAttachment(
		peerTGW.ID, peerTGW.ID, backend.AccountID, backend.Region,
	)
	require.NoError(t, err)

	acceptPeerOut, err := client.AcceptTransitGatewayPeeringAttachment(
		t.Context(), &ec2sdk.AcceptTransitGatewayPeeringAttachmentInput{
			TransitGatewayAttachmentId: aws.String(peering1.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, acceptPeerOut.TransitGatewayPeeringAttachment)
	assert.Equal(
		t, peering1.TransitGatewayAttachmentID,
		aws.ToString(acceptPeerOut.TransitGatewayPeeringAttachment.TransitGatewayAttachmentId),
	)
	assert.Equal(
		t,
		types.TransitGatewayAttachmentStateAvailable,
		acceptPeerOut.TransitGatewayPeeringAttachment.State,
	)

	peering2, err := backend.CreateTransitGatewayPeeringAttachment(
		peerTGW.ID, peerTGW.ID, backend.AccountID, backend.Region,
	)
	require.NoError(t, err)

	rejectPeerOut, err := client.RejectTransitGatewayPeeringAttachment(
		t.Context(), &ec2sdk.RejectTransitGatewayPeeringAttachmentInput{
			TransitGatewayAttachmentId: aws.String(peering2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, rejectPeerOut.TransitGatewayPeeringAttachment)
	assert.Equal(
		t,
		types.TransitGatewayAttachmentStateRejected,
		rejectPeerOut.TransitGatewayPeeringAttachment.State,
	)

	deleteOut, err := client.DeleteTransitGatewayPeeringAttachment(
		t.Context(), &ec2sdk.DeleteTransitGatewayPeeringAttachmentInput{
			TransitGatewayAttachmentId: aws.String(peering2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deleteOut.TransitGatewayPeeringAttachment)
	assert.Equal(
		t, peering2.TransitGatewayAttachmentID,
		aws.ToString(deleteOut.TransitGatewayPeeringAttachment.TransitGatewayAttachmentId),
	)
}

// ---- TGW policy table family ----

// runTGWPolicyTableFamily covers ModifyTransitGatewayVpcAttachment,
// CreateTransitGatewayPolicyTable, AssociateTransitGatewayPolicyTable,
// GetTransitGatewayPolicyTableAssociations, CreateTransitGatewayPolicyTableEntry,
// GetTransitGatewayPolicyTableEntries, ModifyTransitGatewayPolicyTableEntry,
// DeleteTransitGatewayPolicyTableEntry, DisassociateTransitGatewayPolicyTable,
// DeleteTransitGatewayPolicyTable.
func runTGWPolicyTableFamily(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	tgw, att1, _ := setupTGWWithTwoVPCAttachments(t, backend)

	vpc3, err := backend.CreateVpc("10.9.0.0/16", "default")
	require.NoError(t, err)
	subnet3, err := backend.CreateSubnet(vpc3.ID, "10.9.1.0/24", "us-east-1a")
	require.NoError(t, err)

	modOut, err := client.ModifyTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.ModifyTransitGatewayVpcAttachmentInput{
			TransitGatewayAttachmentId: aws.String(att1.TransitGatewayAttachmentID),
			AddSubnetIds:               []string{subnet3.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modOut.TransitGatewayVpcAttachment)
	assert.Contains(t, modOut.TransitGatewayVpcAttachment.SubnetIds, subnet3.ID)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	createPTOut, err := client.CreateTransitGatewayPolicyTable(
		t.Context(), &ec2sdk.CreateTransitGatewayPolicyTableInput{
			TransitGatewayId: aws.String(tgw.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createPTOut.TransitGatewayPolicyTable)
	policyTableID := aws.ToString(createPTOut.TransitGatewayPolicyTable.TransitGatewayPolicyTableId)
	require.NotEmpty(t, policyTableID)
	assert.Equal(t, tgw.ID, aws.ToString(createPTOut.TransitGatewayPolicyTable.TransitGatewayId))

	assocOut, err := client.AssociateTransitGatewayPolicyTable(
		t.Context(), &ec2sdk.AssociateTransitGatewayPolicyTableInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
			TransitGatewayAttachmentId:  aws.String(att1.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, assocOut.Association)
	assert.Equal(t, policyTableID, aws.ToString(assocOut.Association.TransitGatewayPolicyTableId))
	assert.Equal(
		t,
		att1.TransitGatewayAttachmentID,
		aws.ToString(assocOut.Association.TransitGatewayAttachmentId),
	)

	getAssocOut, err := client.GetTransitGatewayPolicyTableAssociations(
		t.Context(), &ec2sdk.GetTransitGatewayPolicyTableAssociationsInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getAssocOut.Associations, 1)
	assert.Equal(
		t,
		att1.TransitGatewayAttachmentID,
		aws.ToString(getAssocOut.Associations[0].TransitGatewayAttachmentId),
	)

	createEntryOut, err := client.CreateTransitGatewayPolicyTableEntry(
		t.Context(), &ec2sdk.CreateTransitGatewayPolicyTableEntryInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
			PolicyRuleNumber:            aws.String("100"),
			TargetRouteTableId:          aws.String(rt.RouteTableID),
			PolicyRule: &types.TransitGatewayRequestPolicyRule{
				SourceCidrBlock:      aws.String("10.0.0.0/16"),
				DestinationCidrBlock: aws.String("10.1.0.0/16"),
				Protocol:             aws.String("6"),
				MetaData: &types.TransitGatewayRequestPolicyRuleMetaData{
					MetaDataKey:   aws.String("env"),
					MetaDataValue: aws.String("prod"),
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createEntryOut.TransitGatewayPolicyTableEntry)
	entry := createEntryOut.TransitGatewayPolicyTableEntry
	assert.Equal(t, "100", aws.ToString(entry.PolicyRuleNumber))
	assert.Equal(t, rt.RouteTableID, aws.ToString(entry.TargetRouteTableId))
	require.NotNil(t, entry.PolicyRule)
	assert.Equal(t, "10.0.0.0/16", aws.ToString(entry.PolicyRule.SourceCidrBlock))
	require.NotNil(t, entry.PolicyRule.MetaData)
	assert.Equal(t, "env", aws.ToString(entry.PolicyRule.MetaData.MetaDataKey))

	getEntriesOut, err := client.GetTransitGatewayPolicyTableEntries(
		t.Context(), &ec2sdk.GetTransitGatewayPolicyTableEntriesInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getEntriesOut.TransitGatewayPolicyTableEntries, 1)
	assert.Equal(t, "100", aws.ToString(getEntriesOut.TransitGatewayPolicyTableEntries[0].PolicyRuleNumber))

	modEntryOut, err := client.ModifyTransitGatewayPolicyTableEntry(
		t.Context(), &ec2sdk.ModifyTransitGatewayPolicyTableEntryInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
			PolicyRuleNumber:            aws.String("100"),
			PolicyRule: &types.TransitGatewayRequestPolicyRule{
				SourceCidrBlock: aws.String("10.2.0.0/16"),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modEntryOut.TransitGatewayPolicyTableEntry)
	require.NotNil(t, modEntryOut.TransitGatewayPolicyTableEntry.PolicyRule)
	assert.Equal(
		t,
		"10.2.0.0/16",
		aws.ToString(modEntryOut.TransitGatewayPolicyTableEntry.PolicyRule.SourceCidrBlock),
	)

	delEntryOut, err := client.DeleteTransitGatewayPolicyTableEntry(
		t.Context(), &ec2sdk.DeleteTransitGatewayPolicyTableEntryInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
			PolicyRuleNumber:            aws.String("100"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delEntryOut.TransitGatewayPolicyTableEntry)

	disassocOut, err := client.DisassociateTransitGatewayPolicyTable(
		t.Context(), &ec2sdk.DisassociateTransitGatewayPolicyTableInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
			TransitGatewayAttachmentId:  aws.String(att1.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disassocOut.Association)
	assert.Equal(t, types.TransitGatewayAssociationStateDisassociated, disassocOut.Association.State)

	delPTOut, err := client.DeleteTransitGatewayPolicyTable(
		t.Context(), &ec2sdk.DeleteTransitGatewayPolicyTableInput{
			TransitGatewayPolicyTableId: aws.String(policyTableID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delPTOut.TransitGatewayPolicyTable)
}

// ---- TGW route table family ----

// runTGWRouteTableFamily covers GetTransitGatewayRouteTableAssociations,
// DisassociateTransitGatewayRouteTable, EnableTransitGatewayRouteTablePropagation,
// GetTransitGatewayRouteTablePropagations, GetTransitGatewayAttachmentPropagations,
// DisableTransitGatewayRouteTablePropagation, ReplaceTransitGatewayRoute,
// SearchTransitGatewayRoutes, ExportTransitGatewayRoutes, DeleteTransitGatewayRoute.
func runTGWRouteTableFamily(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	_, err = backend.AssociateTransitGatewayRouteTable(rt.RouteTableID, att1.TransitGatewayAttachmentID)
	require.NoError(t, err)

	getAssocOut, err := client.GetTransitGatewayRouteTableAssociations(
		t.Context(), &ec2sdk.GetTransitGatewayRouteTableAssociationsInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getAssocOut.Associations, 1)
	assert.Equal(
		t,
		att1.TransitGatewayAttachmentID,
		aws.ToString(getAssocOut.Associations[0].TransitGatewayAttachmentId),
	)

	disassocOut, err := client.DisassociateTransitGatewayRouteTable(
		t.Context(), &ec2sdk.DisassociateTransitGatewayRouteTableInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			TransitGatewayAttachmentId: aws.String(att1.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disassocOut.Association)

	enableOut, err := client.EnableTransitGatewayRouteTablePropagation(
		t.Context(), &ec2sdk.EnableTransitGatewayRouteTablePropagationInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			TransitGatewayAttachmentId: aws.String(att2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, enableOut.Propagation)
	assert.Equal(
		t,
		att2.TransitGatewayAttachmentID,
		aws.ToString(enableOut.Propagation.TransitGatewayAttachmentId),
	)

	getPropOut, err := client.GetTransitGatewayRouteTablePropagations(
		t.Context(), &ec2sdk.GetTransitGatewayRouteTablePropagationsInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getPropOut.TransitGatewayRouteTablePropagations, 1)
	assert.Equal(
		t, att2.TransitGatewayAttachmentID,
		aws.ToString(getPropOut.TransitGatewayRouteTablePropagations[0].TransitGatewayAttachmentId),
	)

	getAttPropOut, err := client.GetTransitGatewayAttachmentPropagations(
		t.Context(), &ec2sdk.GetTransitGatewayAttachmentPropagationsInput{
			TransitGatewayAttachmentId: aws.String(att2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getAttPropOut.TransitGatewayAttachmentPropagations, 1)
	assert.Equal(
		t,
		rt.RouteTableID,
		aws.ToString(getAttPropOut.TransitGatewayAttachmentPropagations[0].TransitGatewayRouteTableId),
	)

	disableOut, err := client.DisableTransitGatewayRouteTablePropagation(
		t.Context(), &ec2sdk.DisableTransitGatewayRouteTablePropagationInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			TransitGatewayAttachmentId: aws.String(att2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disableOut.Propagation)

	_, err = backend.CreateTransitGatewayRoute(
		rt.RouteTableID,
		"10.5.0.0/16",
		att2.TransitGatewayAttachmentID,
		false,
	)
	require.NoError(t, err)

	replaceOut, err := client.ReplaceTransitGatewayRoute(
		t.Context(), &ec2sdk.ReplaceTransitGatewayRouteInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			DestinationCidrBlock:       aws.String("10.5.0.0/16"),
			Blackhole:                  aws.Bool(true),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, replaceOut.Route)
	assert.Equal(t, types.TransitGatewayRouteStateBlackhole, replaceOut.Route.State)

	searchOut, err := client.SearchTransitGatewayRoutes(
		t.Context(), &ec2sdk.SearchTransitGatewayRoutesInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			Filters: []types.Filter{
				{Name: aws.String("state"), Values: []string{"blackhole"}},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, searchOut.Routes, 1)
	assert.Equal(t, "10.5.0.0/16", aws.ToString(searchOut.Routes[0].DestinationCidrBlock))

	exportOut, err := client.ExportTransitGatewayRoutes(
		t.Context(), &ec2sdk.ExportTransitGatewayRoutesInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			S3Bucket:                   aws.String("my-export-bucket"),
		},
	)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(exportOut.S3Location), "my-export-bucket")

	deleteRouteOut, err := client.DeleteTransitGatewayRoute(
		t.Context(), &ec2sdk.DeleteTransitGatewayRouteInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			DestinationCidrBlock:       aws.String("10.5.0.0/16"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deleteRouteOut.Route)
}

// ---- TGW route table announcements ----

// runTGWRouteTableAnnouncements covers CreateTransitGatewayRouteTableAnnouncement,
// DescribeTransitGatewayRouteTableAnnouncements, DeleteTransitGatewayRouteTableAnnouncement.
func runTGWRouteTableAnnouncements(t *testing.T, backend *ec2.InMemoryBackend, _ *ec2sdk.Client) {
	t.Helper()

	client := newTestEC2Client(t, ec2.NewHandler(backend))

	tgw, _, _ := setupTGWWithTwoVPCAttachments(t, backend)
	peerTGW, err := backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "peer-tgw"})
	require.NoError(t, err)

	peering, err := backend.CreateTransitGatewayPeeringAttachment(
		tgw.ID,
		peerTGW.ID,
		backend.AccountID,
		backend.Region,
	)
	require.NoError(t, err)
	_, err = backend.AcceptTransitGatewayPeeringAttachment(peering.TransitGatewayAttachmentID)
	require.NoError(t, err)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	createOut, err := client.CreateTransitGatewayRouteTableAnnouncement(
		t.Context(), &ec2sdk.CreateTransitGatewayRouteTableAnnouncementInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			PeeringAttachmentId:        aws.String(peering.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createOut.TransitGatewayRouteTableAnnouncement)
	annID := aws.ToString(
		createOut.TransitGatewayRouteTableAnnouncement.TransitGatewayRouteTableAnnouncementId,
	)
	require.NotEmpty(t, annID)
	assert.Equal(
		t,
		peering.TransitGatewayAttachmentID,
		aws.ToString(createOut.TransitGatewayRouteTableAnnouncement.PeeringAttachmentId),
	)

	describeOut, err := client.DescribeTransitGatewayRouteTableAnnouncements(
		t.Context(), &ec2sdk.DescribeTransitGatewayRouteTableAnnouncementsInput{
			TransitGatewayRouteTableAnnouncementIds: []string{annID},
		},
	)
	require.NoError(t, err)
	require.Len(t, describeOut.TransitGatewayRouteTableAnnouncements, 1)
	assert.Equal(
		t,
		annID,
		aws.ToString(
			describeOut.TransitGatewayRouteTableAnnouncements[0].TransitGatewayRouteTableAnnouncementId,
		),
	)

	deleteOut, err := client.DeleteTransitGatewayRouteTableAnnouncement(
		t.Context(), &ec2sdk.DeleteTransitGatewayRouteTableAnnouncementInput{
			TransitGatewayRouteTableAnnouncementId: aws.String(annID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deleteOut.TransitGatewayRouteTableAnnouncement)
}

// ---- TGW prefix list references and metering policy ----

// runTGWPrefixListAndMeteringPolicy covers CreateTransitGatewayPrefixListReference,
// GetTransitGatewayPrefixListReferences, ModifyTransitGatewayPrefixListReference,
// CreateTransitGatewayMeteringPolicyEntry, GetTransitGatewayMeteringPolicyEntries,
// DeleteTransitGatewayMeteringPolicyEntry, DeleteTransitGatewayMeteringPolicy.
func runTGWPrefixListAndMeteringPolicy(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)
	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	pl, err := backend.CreateManagedPrefixList("slice26-pl", "IPv4", 10, nil)
	require.NoError(t, err)

	createRefOut, err := client.CreateTransitGatewayPrefixListReference(
		t.Context(), &ec2sdk.CreateTransitGatewayPrefixListReferenceInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			PrefixListId:               aws.String(pl.PrefixListID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createRefOut.TransitGatewayPrefixListReference)
	assert.Equal(
		t,
		pl.PrefixListID,
		aws.ToString(createRefOut.TransitGatewayPrefixListReference.PrefixListId),
	)

	getRefsOut, err := client.GetTransitGatewayPrefixListReferences(
		t.Context(), &ec2sdk.GetTransitGatewayPrefixListReferencesInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getRefsOut.TransitGatewayPrefixListReferences, 1)
	assert.Equal(
		t,
		pl.PrefixListID,
		aws.ToString(getRefsOut.TransitGatewayPrefixListReferences[0].PrefixListId),
	)

	modRefOut, err := client.ModifyTransitGatewayPrefixListReference(
		t.Context(), &ec2sdk.ModifyTransitGatewayPrefixListReferenceInput{
			TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
			PrefixListId:               aws.String(pl.PrefixListID),
			TransitGatewayAttachmentId: aws.String(att1.TransitGatewayAttachmentID),
			Blackhole:                  aws.Bool(true),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modRefOut.TransitGatewayPrefixListReference)
	assert.True(t, aws.ToBool(modRefOut.TransitGatewayPrefixListReference.Blackhole))
	require.NotNil(t, modRefOut.TransitGatewayPrefixListReference.TransitGatewayAttachment)
	assert.Equal(
		t,
		att1.TransitGatewayAttachmentID,
		aws.ToString(
			modRefOut.TransitGatewayPrefixListReference.TransitGatewayAttachment.TransitGatewayAttachmentId,
		),
	)

	policy, err := backend.CreateTransitGatewayMeteringPolicy(
		tgw.ID,
		[]string{att1.TransitGatewayAttachmentID},
		nil,
	)
	require.NoError(t, err)

	createEntryOut, err := client.CreateTransitGatewayMeteringPolicyEntry(
		t.Context(), &ec2sdk.CreateTransitGatewayMeteringPolicyEntryInput{
			TransitGatewayMeteringPolicyId:        aws.String(policy.ID),
			PolicyRuleNumber:                      aws.Int32(50),
			MeteredAccount:                        types.TransitGatewayMeteringPayerTypeSourceAttachmentOwner,
			SourceCidrBlock:                       aws.String("10.0.0.0/16"),
			SourceTransitGatewayAttachmentId:      aws.String(att1.TransitGatewayAttachmentID),
			DestinationCidrBlock:                  aws.String("10.1.0.0/16"),
			DestinationTransitGatewayAttachmentId: aws.String(att2.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createEntryOut.TransitGatewayMeteringPolicyEntry)
	entry := createEntryOut.TransitGatewayMeteringPolicyEntry
	assert.Equal(t, "50", aws.ToString(entry.PolicyRuleNumber))
	assert.Equal(
		t, string(types.TransitGatewayMeteringPayerTypeSourceAttachmentOwner), string(entry.MeteredAccount),
	)
	require.NotNil(t, entry.MeteringPolicyRule)
	assert.Equal(t, "10.0.0.0/16", aws.ToString(entry.MeteringPolicyRule.SourceCidrBlock))

	getEntriesOut, err := client.GetTransitGatewayMeteringPolicyEntries(
		t.Context(), &ec2sdk.GetTransitGatewayMeteringPolicyEntriesInput{
			TransitGatewayMeteringPolicyId: aws.String(policy.ID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getEntriesOut.TransitGatewayMeteringPolicyEntries, 1)

	delEntryOut, err := client.DeleteTransitGatewayMeteringPolicyEntry(
		t.Context(), &ec2sdk.DeleteTransitGatewayMeteringPolicyEntryInput{
			TransitGatewayMeteringPolicyId: aws.String(policy.ID),
			PolicyRuleNumber:               aws.Int32(50),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delEntryOut.TransitGatewayMeteringPolicyEntry)

	delPolicyOut, err := client.DeleteTransitGatewayMeteringPolicy(
		t.Context(), &ec2sdk.DeleteTransitGatewayMeteringPolicyInput{
			TransitGatewayMeteringPolicyId: aws.String(policy.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delPolicyOut.TransitGatewayMeteringPolicy)
}

// ---- TGW Connect and Client VPN attachments ----

// runTGWConnectAndClientVpn covers DeleteTransitGatewayConnect,
// AcceptTransitGatewayClientVpnAttachment, RejectTransitGatewayClientVpnAttachment,
// DeleteTransitGatewayClientVpnAttachment.
func runTGWConnectAndClientVpn(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	tgw, att1, _ := setupTGWWithTwoVPCAttachments(t, backend)

	conn, err := backend.CreateTransitGatewayConnect(att1.TransitGatewayAttachmentID, tgw.ID)
	require.NoError(t, err)

	delConnOut, err := client.DeleteTransitGatewayConnect(
		t.Context(), &ec2sdk.DeleteTransitGatewayConnectInput{
			TransitGatewayAttachmentId: aws.String(conn.TransitGatewayAttachmentID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delConnOut.TransitGatewayConnect)
	assert.Equal(
		t,
		conn.TransitGatewayAttachmentID,
		aws.ToString(delConnOut.TransitGatewayConnect.TransitGatewayAttachmentId),
	)

	ep1, err := backend.CreateClientVpnEndpointWithOptions(
		"10.20.0.0/22", "cvpn-1", nil, ec2.ClientVpnEndpointOptions{TransitGatewayID: tgw.ID},
	)
	require.NoError(t, err)
	cvpnAtt1 := findClientVpnAttachment(t, backend, ep1.ClientVpnEndpointID)

	acceptCvpnOut, err := client.AcceptTransitGatewayClientVpnAttachment(
		t.Context(), &ec2sdk.AcceptTransitGatewayClientVpnAttachmentInput{
			TransitGatewayAttachmentId: aws.String(cvpnAtt1),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, acceptCvpnOut.TransitGatewayClientVpnAttachment)
	assert.Equal(
		t,
		types.TransitGatewayAttachmentStatusTypeAvailable,
		acceptCvpnOut.TransitGatewayClientVpnAttachment.State,
	)

	ep2, err := backend.CreateClientVpnEndpointWithOptions(
		"10.21.0.0/22", "cvpn-2", nil, ec2.ClientVpnEndpointOptions{TransitGatewayID: tgw.ID},
	)
	require.NoError(t, err)
	cvpnAtt2 := findClientVpnAttachment(t, backend, ep2.ClientVpnEndpointID)

	rejectCvpnOut, err := client.RejectTransitGatewayClientVpnAttachment(
		t.Context(), &ec2sdk.RejectTransitGatewayClientVpnAttachmentInput{
			TransitGatewayAttachmentId: aws.String(cvpnAtt2),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, rejectCvpnOut.TransitGatewayClientVpnAttachment)
	assert.Equal(
		t,
		types.TransitGatewayAttachmentStatusTypeRejected,
		rejectCvpnOut.TransitGatewayClientVpnAttachment.State,
	)

	deleteCvpnOut, err := client.DeleteTransitGatewayClientVpnAttachment(
		t.Context(), &ec2sdk.DeleteTransitGatewayClientVpnAttachmentInput{
			TransitGatewayAttachmentId: aws.String(cvpnAtt2),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deleteCvpnOut.TransitGatewayClientVpnAttachment)
}

// findClientVpnAttachment locates the TGW Client VPN attachment ID implicitly
// created for a Client VPN endpoint with TransitGatewayConfiguration set.
func findClientVpnAttachment(t *testing.T, backend *ec2.InMemoryBackend, clientVpnEndpointID string) string {
	t.Helper()

	for _, att := range backend.DescribeTransitGatewayAttachments(nil) {
		if att.ResourceID == clientVpnEndpointID {
			return att.TransitGatewayAttachmentID
		}
	}

	t.Fatalf("no TGW attachment found for client VPN endpoint %s", clientVpnEndpointID)

	return ""
}

// ---- TGW multicast accept/reject and group members/sources ----

// runTGWMulticastAcceptRejectAndGroups covers
// AcceptTransitGatewayMulticastDomainAssociations,
// RejectTransitGatewayMulticastDomainAssociations,
// RegisterTransitGatewayMulticastGroupSources,
// DeregisterTransitGatewayMulticastGroupSources,
// DeregisterTransitGatewayMulticastGroupMembers.
func runTGWMulticastAcceptRejectAndGroups(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	tgw, att1, att2 := setupTGWWithTwoVPCAttachments(t, backend)

	domain, err := backend.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	subnetA, err := backend.CreateSubnet(att1.VpcID, "10.0.2.0/24", "us-east-1a")
	require.NoError(t, err)

	acceptOut, err := client.AcceptTransitGatewayMulticastDomainAssociations(
		t.Context(), &ec2sdk.AcceptTransitGatewayMulticastDomainAssociationsInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
			TransitGatewayAttachmentId:      aws.String(att1.TransitGatewayAttachmentID),
			SubnetIds:                       []string{subnetA.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, acceptOut.Associations)
	assert.Equal(t, domain.ID, aws.ToString(acceptOut.Associations.TransitGatewayMulticastDomainId))
	assert.Equal(
		t,
		att1.TransitGatewayAttachmentID,
		aws.ToString(acceptOut.Associations.TransitGatewayAttachmentId),
	)
	require.Len(t, acceptOut.Associations.Subnets, 1)
	assert.Equal(t, subnetA.ID, aws.ToString(acceptOut.Associations.Subnets[0].SubnetId))
	assert.Equal(
		t,
		types.TransitGatewayMulitcastDomainAssociationStateAssociated,
		acceptOut.Associations.Subnets[0].State,
	)

	subnetB, err := backend.CreateSubnet(att2.VpcID, "10.1.2.0/24", "us-east-1a")
	require.NoError(t, err)

	rejectOut, err := client.RejectTransitGatewayMulticastDomainAssociations(
		t.Context(), &ec2sdk.RejectTransitGatewayMulticastDomainAssociationsInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
			TransitGatewayAttachmentId:      aws.String(att2.TransitGatewayAttachmentID),
			SubnetIds:                       []string{subnetB.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, rejectOut.Associations)
	assert.Equal(
		t,
		att2.TransitGatewayAttachmentID,
		aws.ToString(rejectOut.Associations.TransitGatewayAttachmentId),
	)
	require.Len(t, rejectOut.Associations.Subnets, 1)
	assert.Equal(t, subnetB.ID, aws.ToString(rejectOut.Associations.Subnets[0].SubnetId))

	eni, err := backend.CreateNetworkInterface(subnetA.ID, "multicast-eni")
	require.NoError(t, err)

	registerSrcOut, err := client.RegisterTransitGatewayMulticastGroupSources(
		t.Context(), &ec2sdk.RegisterTransitGatewayMulticastGroupSourcesInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
			GroupIpAddress:                  aws.String("224.0.1.0"),
			NetworkInterfaceIds:             []string{eni.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, registerSrcOut.RegisteredMulticastGroupSources)
	assert.Contains(t, registerSrcOut.RegisteredMulticastGroupSources.RegisteredNetworkInterfaceIds, eni.ID)
	assert.Equal(t, "224.0.1.0", aws.ToString(registerSrcOut.RegisteredMulticastGroupSources.GroupIpAddress))

	deregisterSrcOut, err := client.DeregisterTransitGatewayMulticastGroupSources(
		t.Context(), &ec2sdk.DeregisterTransitGatewayMulticastGroupSourcesInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
			GroupIpAddress:                  aws.String("224.0.1.0"),
			NetworkInterfaceIds:             []string{eni.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deregisterSrcOut.DeregisteredMulticastGroupSources)
	assert.Contains(
		t,
		deregisterSrcOut.DeregisteredMulticastGroupSources.DeregisteredNetworkInterfaceIds,
		eni.ID,
	)

	_, err = backend.RegisterTransitGatewayMulticastGroupMembers(domain.ID, "224.0.2.0", []string{eni.ID})
	require.NoError(t, err)

	deregisterMemOut, err := client.DeregisterTransitGatewayMulticastGroupMembers(
		t.Context(), &ec2sdk.DeregisterTransitGatewayMulticastGroupMembersInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
			GroupIpAddress:                  aws.String("224.0.2.0"),
			NetworkInterfaceIds:             []string{eni.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deregisterMemOut.DeregisteredMulticastGroupMembers)
	assert.Contains(
		t,
		deregisterMemOut.DeregisteredMulticastGroupMembers.DeregisteredNetworkInterfaceIds,
		eni.ID,
	)
}

// ---- IPAM prefix list resolver family ----

// runIpamPrefixListResolverFamily covers CreateIpamPrefixListResolver,
// DescribeIpamPrefixListResolvers, ModifyIpamPrefixListResolver,
// GetIpamPrefixListResolverRules, GetIpamPrefixListResolverVersions,
// GetIpamPrefixListResolverVersionEntries, CreateIpamPrefixListResolverTarget,
// DescribeIpamPrefixListResolverTargets, ModifyIpamPrefixListResolverTarget,
// DeleteIpamPrefixListResolverTarget, DeleteIpamPrefixListResolver.
func runIpamPrefixListResolverFamily(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	ipam, err := backend.CreateIpam()
	require.NoError(t, err)

	pl, err := backend.CreateManagedPrefixList("resolver-target-pl", "IPv4", 10, nil)
	require.NoError(t, err)

	createOut, err := client.CreateIpamPrefixListResolver(
		t.Context(), &ec2sdk.CreateIpamPrefixListResolverInput{
			IpamId:        aws.String(ipam.IpamID),
			AddressFamily: types.AddressFamilyIpv4,
			Description:   aws.String("slice26 resolver"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createOut.IpamPrefixListResolver)
	resolverID := aws.ToString(createOut.IpamPrefixListResolver.IpamPrefixListResolverId)
	require.NotEmpty(t, resolverID)
	assert.Equal(t, ipam.IpamARN, aws.ToString(createOut.IpamPrefixListResolver.IpamArn))
	assert.Equal(t, "slice26 resolver", aws.ToString(createOut.IpamPrefixListResolver.Description))

	describeOut, err := client.DescribeIpamPrefixListResolvers(
		t.Context(), &ec2sdk.DescribeIpamPrefixListResolversInput{
			IpamPrefixListResolverIds: []string{resolverID},
		},
	)
	require.NoError(t, err)
	require.Len(t, describeOut.IpamPrefixListResolvers, 1)
	assert.Equal(t, resolverID, aws.ToString(describeOut.IpamPrefixListResolvers[0].IpamPrefixListResolverId))

	modOut, err := client.ModifyIpamPrefixListResolver(
		t.Context(), &ec2sdk.ModifyIpamPrefixListResolverInput{
			IpamPrefixListResolverId: aws.String(resolverID),
			Description:              aws.String("updated description"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modOut.IpamPrefixListResolver)
	assert.Equal(t, "updated description", aws.ToString(modOut.IpamPrefixListResolver.Description))

	rulesOut, err := client.GetIpamPrefixListResolverRules(
		t.Context(), &ec2sdk.GetIpamPrefixListResolverRulesInput{
			IpamPrefixListResolverId: aws.String(resolverID),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, rulesOut.Rules)

	versionsOut, err := client.GetIpamPrefixListResolverVersions(
		t.Context(), &ec2sdk.GetIpamPrefixListResolverVersionsInput{
			IpamPrefixListResolverId: aws.String(resolverID),
		},
	)
	require.NoError(t, err)
	require.Len(t, versionsOut.IpamPrefixListResolverVersions, 1)
	assert.Equal(t, int64(1), aws.ToInt64(versionsOut.IpamPrefixListResolverVersions[0].Version))

	entriesOut, err := client.GetIpamPrefixListResolverVersionEntries(
		t.Context(), &ec2sdk.GetIpamPrefixListResolverVersionEntriesInput{
			IpamPrefixListResolverId:      aws.String(resolverID),
			IpamPrefixListResolverVersion: aws.Int64(1),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, entriesOut.Entries)

	createTargetOut, err := client.CreateIpamPrefixListResolverTarget(
		t.Context(), &ec2sdk.CreateIpamPrefixListResolverTargetInput{
			IpamPrefixListResolverId: aws.String(resolverID),
			PrefixListId:             aws.String(pl.PrefixListID),
			PrefixListRegion:         aws.String(backend.Region),
			TrackLatestVersion:       aws.Bool(true),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createTargetOut.IpamPrefixListResolverTarget)
	targetID := aws.ToString(createTargetOut.IpamPrefixListResolverTarget.IpamPrefixListResolverTargetId)
	require.NotEmpty(t, targetID)
	assert.Equal(t, pl.PrefixListID, aws.ToString(createTargetOut.IpamPrefixListResolverTarget.PrefixListId))
	assert.True(t, aws.ToBool(createTargetOut.IpamPrefixListResolverTarget.TrackLatestVersion))

	describeTargetsOut, err := client.DescribeIpamPrefixListResolverTargets(
		t.Context(), &ec2sdk.DescribeIpamPrefixListResolverTargetsInput{
			IpamPrefixListResolverId:        aws.String(resolverID),
			IpamPrefixListResolverTargetIds: []string{targetID},
		},
	)
	require.NoError(t, err)
	require.Len(t, describeTargetsOut.IpamPrefixListResolverTargets, 1)

	modTargetOut, err := client.ModifyIpamPrefixListResolverTarget(
		t.Context(), &ec2sdk.ModifyIpamPrefixListResolverTargetInput{
			IpamPrefixListResolverTargetId: aws.String(targetID),
			DesiredVersion:                 aws.Int64(1),
			TrackLatestVersion:             aws.Bool(false),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modTargetOut.IpamPrefixListResolverTarget)
	assert.False(t, aws.ToBool(modTargetOut.IpamPrefixListResolverTarget.TrackLatestVersion))
	assert.Equal(t, int64(1), aws.ToInt64(modTargetOut.IpamPrefixListResolverTarget.DesiredVersion))

	delTargetOut, err := client.DeleteIpamPrefixListResolverTarget(
		t.Context(), &ec2sdk.DeleteIpamPrefixListResolverTargetInput{
			IpamPrefixListResolverTargetId: aws.String(targetID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delTargetOut.IpamPrefixListResolverTarget)

	delResolverOut, err := client.DeleteIpamPrefixListResolver(
		t.Context(), &ec2sdk.DeleteIpamPrefixListResolverInput{
			IpamPrefixListResolverId: aws.String(resolverID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delResolverOut.IpamPrefixListResolver)
}

// ---- IPAM BYOASN and resource discovery family ----

// runIpamByoasnAndDiscoveryFamily covers ProvisionIpamByoasn,
// DeprovisionIpamByoasn, DescribeIpamByoasn, AssociateIpamByoasn,
// DisassociateIpamByoasn, DescribeIpamResourceDiscoveries,
// DescribeIpamResourceDiscoveryAssociations, GetIpamResourceCidrs,
// ModifyIpamResourceCidr, DescribeIpamPoolAllocations, ModifyIpamPoolAllocation.
func runIpamByoasnAndDiscoveryFamily(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	ipam, err := backend.CreateIpam()
	require.NoError(t, err)

	provisionOut, err := client.ProvisionIpamByoasn(
		t.Context(), &ec2sdk.ProvisionIpamByoasnInput{
			IpamId: aws.String(ipam.IpamID),
			Asn:    aws.String("64512"),
			AsnAuthorizationContext: &types.AsnAuthorizationContext{
				Message:   aws.String("test-message"),
				Signature: aws.String("test-signature"),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, provisionOut.Byoasn)
	assert.Equal(t, "64512", aws.ToString(provisionOut.Byoasn.Asn))
	assert.Equal(t, ipam.IpamID, aws.ToString(provisionOut.Byoasn.IpamId))

	describeByoasnOut, err := client.DescribeIpamByoasn(t.Context(), &ec2sdk.DescribeIpamByoasnInput{})
	require.NoError(t, err)
	require.Len(t, describeByoasnOut.Byoasns, 1)
	assert.Equal(t, "64512", aws.ToString(describeByoasnOut.Byoasns[0].Asn))

	associateOut, err := client.AssociateIpamByoasn(
		t.Context(), &ec2sdk.AssociateIpamByoasnInput{
			Asn:  aws.String("64512"),
			Cidr: aws.String("203.0.113.0/24"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, associateOut.AsnAssociation)
	assert.Equal(t, "64512", aws.ToString(associateOut.AsnAssociation.Asn))
	assert.Equal(t, "203.0.113.0/24", aws.ToString(associateOut.AsnAssociation.Cidr))

	disassociateOut, err := client.DisassociateIpamByoasn(
		t.Context(), &ec2sdk.DisassociateIpamByoasnInput{
			Asn:  aws.String("64512"),
			Cidr: aws.String("203.0.113.0/24"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disassociateOut.AsnAssociation)

	deprovisionOut, err := client.DeprovisionIpamByoasn(
		t.Context(), &ec2sdk.DeprovisionIpamByoasnInput{
			IpamId: aws.String(ipam.IpamID),
			Asn:    aws.String("64512"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deprovisionOut.Byoasn)

	describeDiscoveriesOut, err := client.DescribeIpamResourceDiscoveries(
		t.Context(), &ec2sdk.DescribeIpamResourceDiscoveriesInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, describeDiscoveriesOut.IpamResourceDiscoveries)

	describeAssocOut, err := client.DescribeIpamResourceDiscoveryAssociations(
		t.Context(), &ec2sdk.DescribeIpamResourceDiscoveryAssociationsInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, describeAssocOut.IpamResourceDiscoveryAssociations)

	pool, err := backend.CreateIpamPool(ipam.IpamID, "ipv4", "", "10.30.0.0/16")
	require.NoError(t, err)

	vpc, err := backend.CreateVpc("10.30.1.0/24", "default")
	require.NoError(t, err)

	alloc, err := backend.AllocateIpamPoolCidr(
		pool.IpamPoolID, "10.30.1.0/24", 0,
		ec2.IpamAllocationOptions{ResourceID: vpc.ID, ResourceType: "vpc", ResourceOwner: backend.AccountID},
	)
	require.NoError(t, err)

	getCidrsOut, err := client.GetIpamResourceCidrs(
		t.Context(), &ec2sdk.GetIpamResourceCidrsInput{
			IpamScopeId: aws.String(pool.IpamScopeID),
			IpamPoolId:  aws.String(pool.IpamPoolID),
		},
	)
	require.NoError(t, err)
	require.Len(t, getCidrsOut.IpamResourceCidrs, 1)
	assert.Equal(t, vpc.ID, aws.ToString(getCidrsOut.IpamResourceCidrs[0].ResourceId))

	modCidrOut, err := client.ModifyIpamResourceCidr(
		t.Context(), &ec2sdk.ModifyIpamResourceCidrInput{
			CurrentIpamScopeId: aws.String(pool.IpamScopeID),
			ResourceId:         aws.String(vpc.ID),
			ResourceCidr:       aws.String("10.30.1.0/24"),
			ResourceRegion:     aws.String(backend.Region),
			Monitored:          aws.Bool(false),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modCidrOut.IpamResourceCidr)
	assert.Equal(t, types.IpamManagementStateUnmanaged, modCidrOut.IpamResourceCidr.ManagementState)

	describeAllocOut, err := client.DescribeIpamPoolAllocations(
		t.Context(), &ec2sdk.DescribeIpamPoolAllocationsInput{
			IpamPoolAllocationIds: []string{alloc.IpamPoolAllocationID},
		},
	)
	require.NoError(t, err)
	require.Len(t, describeAllocOut.IpamPoolAllocations, 1)
	assert.Equal(
		t,
		alloc.IpamPoolAllocationID,
		aws.ToString(describeAllocOut.IpamPoolAllocations[0].IpamPoolAllocationId),
	)

	modAllocOut, err := client.ModifyIpamPoolAllocation(
		t.Context(), &ec2sdk.ModifyIpamPoolAllocationInput{
			IpamPoolAllocationId: aws.String(alloc.IpamPoolAllocationID),
			Description:          aws.String("updated allocation"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modAllocOut.IpamPoolAllocation)
	assert.Equal(t, "updated allocation", aws.ToString(modAllocOut.IpamPoolAllocation.Description))
}

// ---- legacy bundle/conversion/export/import task family ----

// runLegacyBundleConversionExportImport covers BundleInstance,
// CancelBundleTask, CancelConversionTask, DescribeConversionTasks,
// CancelExportTask, ExportImage, ImportInstance, ImportVolume,
// CancelImportTask.
func runLegacyBundleConversionExportImport(
	t *testing.T,
	backend *ec2.InMemoryBackend,
	client *ec2sdk.Client,
) {
	t.Helper()

	instanceID, _ := setupInstanceWithVolume(t, backend)

	bundleOut, err := client.BundleInstance(
		t.Context(), &ec2sdk.BundleInstanceInput{
			InstanceId: aws.String(instanceID),
			Storage: &types.Storage{
				S3: &types.S3Storage{
					Bucket: aws.String("bundle-bucket"),
					Prefix: aws.String("bundle-prefix"),
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, bundleOut.BundleTask)
	bundleID := aws.ToString(bundleOut.BundleTask.BundleId)
	require.NotEmpty(t, bundleID)
	assert.Equal(t, instanceID, aws.ToString(bundleOut.BundleTask.InstanceId))

	cancelBundleOut, err := client.CancelBundleTask(
		t.Context(), &ec2sdk.CancelBundleTaskInput{BundleId: aws.String(bundleID)},
	)
	require.NoError(t, err)
	require.NotNil(t, cancelBundleOut.BundleTask)
	assert.Equal(t, bundleID, aws.ToString(cancelBundleOut.BundleTask.BundleId))

	// DescribeConversionTasks settles an active task to "completed" as a side
	// effect of being described (settleConversionTask), so a task must be
	// cancelled BEFORE it is ever described, or the cancel call sees a
	// terminal state -- use two separate tasks to exercise both ops cleanly.
	cancelConvTask, err := backend.ImportInstance("test-cancel", "Linux/UNIX", "", "VMDK", 1024, 8)
	require.NoError(t, err)

	cancelConvOut, err := client.CancelConversionTask(
		t.Context(),
		&ec2sdk.CancelConversionTaskInput{ConversionTaskId: aws.String(cancelConvTask.ConversionTaskID)},
	)
	require.NoError(t, err)
	require.NotNil(t, cancelConvOut)

	convTask, err := backend.ImportInstance("test-describe", "Linux/UNIX", "", "VMDK", 1024, 8)
	require.NoError(t, err)

	describeConvOut, err := client.DescribeConversionTasks(
		t.Context(), &ec2sdk.DescribeConversionTasksInput{
			ConversionTaskIds: []string{convTask.ConversionTaskID},
		},
	)
	require.NoError(t, err)
	require.Len(t, describeConvOut.ConversionTasks, 1)
	assert.Equal(
		t,
		convTask.ConversionTaskID,
		aws.ToString(describeConvOut.ConversionTasks[0].ConversionTaskId),
	)

	exportTask, err := backend.CreateInstanceExportTask(
		instanceID,
		"test-export",
		"citrix",
		"VMDK",
		"ova",
		"export-bucket",
		"",
	)
	require.NoError(t, err)

	cancelExportOut, err := client.CancelExportTask(
		t.Context(), &ec2sdk.CancelExportTaskInput{ExportTaskId: aws.String(exportTask.ExportTaskID)},
	)
	require.NoError(t, err)
	require.NotNil(t, cancelExportOut)

	img, err := backend.RegisterImage("slice26-ami", "test image", "x86_64")
	require.NoError(t, err)

	exportImageOut, err := client.ExportImage(
		t.Context(), &ec2sdk.ExportImageInput{
			ImageId:         aws.String(img.ImageID),
			DiskImageFormat: types.DiskImageFormatVmdk,
			S3ExportLocation: &types.ExportTaskS3LocationRequest{
				S3Bucket: aws.String("export-image-bucket"),
				S3Prefix: aws.String("export-image-prefix"),
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, img.ImageID, aws.ToString(exportImageOut.ImageId))
	assert.NotEmpty(t, aws.ToString(exportImageOut.ExportImageTaskId))

	importInstanceOut, err := client.ImportInstance(
		t.Context(), &ec2sdk.ImportInstanceInput{
			Platform: types.PlatformValuesWindows,
			LaunchSpecification: &types.ImportInstanceLaunchSpecification{
				Architecture: types.ArchitectureValuesX8664,
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, importInstanceOut.ConversionTask)
	assert.Equal(t, types.ConversionTaskStateActive, importInstanceOut.ConversionTask.State)

	importVolumeOut, err := client.ImportVolume(
		t.Context(), &ec2sdk.ImportVolumeInput{
			AvailabilityZone: aws.String("us-east-1a"),
			Image: &types.DiskImageDetail{
				Format:            types.DiskImageFormatVmdk,
				Bytes:             aws.Int64(1024),
				ImportManifestUrl: aws.String("https://example.com/manifest"),
			},
			Volume: &types.VolumeDetail{
				Size: aws.Int64(10),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, importVolumeOut.ConversionTask)
	assert.Equal(t, types.ConversionTaskStateActive, importVolumeOut.ConversionTask.State)

	// CancelImportTask: ImportImage/ImportSnapshot settle to "completed"
	// synchronously in this backend (pinned by
	// TestBackend_CancelImportTask_AlreadyCompletedFails), so a real client's
	// happy path can never succeed -- covering it means asserting the correct
	// wire-level IncorrectState error instead of a fabricated success.
	importImageTask, err := backend.ImportImage("cancel-me", "x86_64", "Linux/UNIX", false, "")
	require.NoError(t, err)

	_, err = client.CancelImportTask(
		t.Context(), &ec2sdk.CancelImportTaskInput{ImportTaskId: aws.String(importImageTask.ImportTaskID)},
	)
	require.Error(t, err)
	assert.Equal(t, "IncorrectState", smithyErrCode(t, err))
}

// ---- FPGA image family ----

// runFpgaImageFamily covers CopyFpgaImage, DescribeFpgaImageAttribute,
// ModifyFpgaImageAttribute, ResetFpgaImageAttribute, DeleteFpgaImage.
func runFpgaImageFamily(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	src, err := backend.CreateFpgaImage("source-afi", "source description")
	require.NoError(t, err)

	copyOut, err := client.CopyFpgaImage(
		t.Context(), &ec2sdk.CopyFpgaImageInput{
			SourceFpgaImageId: aws.String(src.FpgaImageID),
			SourceRegion:      aws.String(backend.Region),
			Name:              aws.String("copied-afi"),
		},
	)
	require.NoError(t, err)
	newImageID := aws.ToString(copyOut.FpgaImageId)
	require.NotEmpty(t, newImageID)
	require.NotEqual(t, src.FpgaImageID, newImageID)

	modOut, err := client.ModifyFpgaImageAttribute(
		t.Context(), &ec2sdk.ModifyFpgaImageAttributeInput{
			FpgaImageId: aws.String(newImageID),
			Attribute:   types.FpgaImageAttributeNameLoadPermission,
			LoadPermission: &types.LoadPermissionModifications{
				Add: []types.LoadPermissionRequest{
					{UserId: aws.String("123456789012")},
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modOut.FpgaImageAttribute)
	require.Len(t, modOut.FpgaImageAttribute.LoadPermissions, 1)
	assert.Equal(t, "123456789012", aws.ToString(modOut.FpgaImageAttribute.LoadPermissions[0].UserId))

	descAttrOut, err := client.DescribeFpgaImageAttribute(
		t.Context(), &ec2sdk.DescribeFpgaImageAttributeInput{
			FpgaImageId: aws.String(newImageID),
			Attribute:   types.FpgaImageAttributeNameLoadPermission,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, descAttrOut.FpgaImageAttribute)
	require.Len(t, descAttrOut.FpgaImageAttribute.LoadPermissions, 1)
	assert.Equal(t, "123456789012", aws.ToString(descAttrOut.FpgaImageAttribute.LoadPermissions[0].UserId))

	resetOut, err := client.ResetFpgaImageAttribute(
		t.Context(), &ec2sdk.ResetFpgaImageAttributeInput{
			FpgaImageId: aws.String(newImageID),
			Attribute:   types.ResetFpgaImageAttributeNameLoadPermission,
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(resetOut.Return))

	deleteOut, err := client.DeleteFpgaImage(
		t.Context(), &ec2sdk.DeleteFpgaImageInput{FpgaImageId: aws.String(newImageID)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(deleteOut.Return))
}

// ---- singletons A ----

// runSingletonsA covers DescribeAwsNetworkPerformanceMetricSubscriptions,
// EnableAwsNetworkPerformanceMetricSubscription,
// DisableAwsNetworkPerformanceMetricSubscription, GetAwsNetworkPerformanceData,
// GetManagedResourceVisibility, ModifyManagedResourceVisibility,
// GetDeclarativePoliciesReportSummary, CancelDeclarativePoliciesReport,
// GetFlowLogsIntegrationTemplate, EnableReachabilityAnalyzerOrganizationSharing.
func runSingletonsA(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	enableOut, err := client.EnableAwsNetworkPerformanceMetricSubscription(
		t.Context(), &ec2sdk.EnableAwsNetworkPerformanceMetricSubscriptionInput{
			Source:      aws.String("us-east-1"),
			Destination: aws.String("eu-west-1"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enableOut.Output))

	describeSubsOut, err := client.DescribeAwsNetworkPerformanceMetricSubscriptions(
		t.Context(), &ec2sdk.DescribeAwsNetworkPerformanceMetricSubscriptionsInput{},
	)
	require.NoError(t, err)
	require.Len(t, describeSubsOut.Subscriptions, 1)
	assert.Equal(t, "us-east-1", aws.ToString(describeSubsOut.Subscriptions[0].Source))

	dataOut, err := client.GetAwsNetworkPerformanceData(
		t.Context(), &ec2sdk.GetAwsNetworkPerformanceDataInput{
			DataQueries: []types.DataQuery{
				{Id: aws.String("q1"), Source: aws.String("us-east-1"), Destination: aws.String("eu-west-1")},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, dataOut.DataResponses, 1)
	assert.Equal(t, "q1", aws.ToString(dataOut.DataResponses[0].Id))
	require.Len(t, dataOut.DataResponses[0].MetricPoints, 1)

	disableOut, err := client.DisableAwsNetworkPerformanceMetricSubscription(
		t.Context(), &ec2sdk.DisableAwsNetworkPerformanceMetricSubscriptionInput{
			Source:      aws.String("us-east-1"),
			Destination: aws.String("eu-west-1"),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disableOut.Output))

	getVisOut, err := client.GetManagedResourceVisibility(
		t.Context(), &ec2sdk.GetManagedResourceVisibilityInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, getVisOut.Visibility)
	assert.NotEmpty(t, string(getVisOut.Visibility.DefaultVisibility))

	modVisOut, err := client.ModifyManagedResourceVisibility(
		t.Context(), &ec2sdk.ModifyManagedResourceVisibilityInput{
			DefaultVisibility: types.ManagedResourceDefaultVisibilityVisible,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modVisOut.Visibility)
	assert.Equal(t, types.ManagedResourceDefaultVisibilityVisible, modVisOut.Visibility.DefaultVisibility)

	report, err := backend.StartDeclarativePoliciesReport("dp-bucket", "111111111111", "", nil)
	require.NoError(t, err)

	summaryOut, err := client.GetDeclarativePoliciesReportSummary(
		t.Context(), &ec2sdk.GetDeclarativePoliciesReportSummaryInput{ReportId: aws.String(report.ReportID)},
	)
	require.NoError(t, err)
	assert.Equal(t, report.ReportID, aws.ToString(summaryOut.ReportId))

	report2, err := backend.StartDeclarativePoliciesReport("dp-bucket", "111111111111", "", nil)
	require.NoError(t, err)

	cancelOut, err := client.CancelDeclarativePoliciesReport(
		t.Context(), &ec2sdk.CancelDeclarativePoliciesReportInput{ReportId: aws.String(report2.ReportID)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(cancelOut.Return))

	vpc, err := backend.CreateVpc("10.40.0.0/16", "default")
	require.NoError(t, err)
	flowLogs, err := backend.CreateFlowLogs(
		[]string{vpc.ID},
		"ALL",
		"s3",
		"arn:aws:s3:::flow-log-bucket",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, flowLogs, 1)

	tmplOut, err := client.GetFlowLogsIntegrationTemplate(
		t.Context(), &ec2sdk.GetFlowLogsIntegrationTemplateInput{
			FlowLogId:                      aws.String(flowLogs[0].FlowLogID),
			ConfigDeliveryS3DestinationArn: aws.String("arn:aws:s3:::flow-log-config-bucket"),
			IntegrateServices: &types.IntegrateServices{
				AthenaIntegrations: []types.AthenaIntegration{
					{
						IntegrationResultS3DestinationArn: aws.String("arn:aws:s3:::flow-log-results-bucket"),
						PartitionLoadFrequency:            types.PartitionLoadFrequencyNone,
					},
				},
			},
		},
	)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(tmplOut.Result), "flow-log-results-bucket")

	reachOut, err := client.EnableReachabilityAnalyzerOrganizationSharing(
		t.Context(), &ec2sdk.EnableReachabilityAnalyzerOrganizationSharingInput{},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(reachOut.ReturnValue))
}

// ---- singletons B ----

// runSingletonsB covers DeleteSecondaryNetwork, DeleteSecondarySubnet,
// DescribeOutpostLags, AssociateTrunkInterface, DisassociateTrunkInterface,
// AssociateEnclaveCertificateIamRole, GetAssociatedEnclaveCertificateIamRoles,
// DisassociateEnclaveCertificateIamRole, DisableInstanceSqlHaStandbyDetections,
// CreateSpotDatafeedSubscription, DescribeSpotDatafeedSubscription,
// DeleteSpotDatafeedSubscription, ModifyCapacityReservation,
// CancelCapacityReservation, ModifyFleet, ModifyLaunchTemplate,
// DescribeIpv6Pools, GetAssociatedIpv6PoolCidrs, GetImageAncestry,
// CancelImageLaunchPermission, RestoreImageFromRecycleBin,
// AttachImageWatermark/DetachImageWatermark (setup only, not this slice's
// target -- both already typed-covered), ConfirmProductInstance.
func runSingletonsB(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	secNet, err := backend.CreateSecondaryNetwork("172.16.0.0/16", "rdma", nil)
	require.NoError(t, err)
	secSubnet, err := backend.CreateSecondarySubnet(
		"172.16.1.0/24",
		secNet.SecondaryNetworkID,
		"us-east-1a",
		"",
		nil,
	)
	require.NoError(t, err)

	delSubnetOut, err := client.DeleteSecondarySubnet(
		t.Context(),
		&ec2sdk.DeleteSecondarySubnetInput{SecondarySubnetId: aws.String(secSubnet.SecondarySubnetID)},
	)
	require.NoError(t, err)
	require.NotNil(t, delSubnetOut.SecondarySubnet)

	delNetOut, err := client.DeleteSecondaryNetwork(
		t.Context(),
		&ec2sdk.DeleteSecondaryNetworkInput{SecondaryNetworkId: aws.String(secNet.SecondaryNetworkID)},
	)
	require.NoError(t, err)
	require.NotNil(t, delNetOut.SecondaryNetwork)

	lag, err := backend.SeedOutpostLag(
		ec2.OutpostLag{OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-test"},
	)
	require.NoError(t, err)

	lagsOut, err := client.DescribeOutpostLags(
		t.Context(), &ec2sdk.DescribeOutpostLagsInput{OutpostLagIds: []string{lag.OutpostLagID}},
	)
	require.NoError(t, err)
	require.Len(t, lagsOut.OutpostLags, 1)
	assert.Equal(t, lag.OutpostLagID, aws.ToString(lagsOut.OutpostLags[0].OutpostLagId))

	vpc, err := backend.CreateVpc("10.41.0.0/16", "default")
	require.NoError(t, err)
	subnet, err := backend.CreateSubnet(vpc.ID, "10.41.1.0/24", "us-east-1a")
	require.NoError(t, err)
	branchENI, err := backend.CreateNetworkInterface(subnet.ID, "branch")
	require.NoError(t, err)
	trunkENI, err := backend.CreateNetworkInterface(subnet.ID, "trunk")
	require.NoError(t, err)

	assocTrunkOut, err := client.AssociateTrunkInterface(
		t.Context(), &ec2sdk.AssociateTrunkInterfaceInput{
			BranchInterfaceId: aws.String(branchENI.ID),
			TrunkInterfaceId:  aws.String(trunkENI.ID),
			VlanId:            aws.Int32(42),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, assocTrunkOut.InterfaceAssociation)
	trunkAssocID := aws.ToString(assocTrunkOut.InterfaceAssociation.AssociationId)
	require.NotEmpty(t, trunkAssocID)

	disassocTrunkOut, err := client.DisassociateTrunkInterface(
		t.Context(), &ec2sdk.DisassociateTrunkInterfaceInput{AssociationId: aws.String(trunkAssocID)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disassocTrunkOut.Return))

	certArn := "arn:aws:acm:us-east-1:000000000000:certificate/slice26-cert"
	roleArn := "arn:aws:iam::000000000000:role/slice26-role"

	assocCertOut, err := client.AssociateEnclaveCertificateIamRole(
		t.Context(), &ec2sdk.AssociateEnclaveCertificateIamRoleInput{
			CertificateArn: aws.String(certArn),
			RoleArn:        aws.String(roleArn),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(assocCertOut.CertificateS3BucketName))

	getCertRolesOut, err := client.GetAssociatedEnclaveCertificateIamRoles(
		t.Context(),
		&ec2sdk.GetAssociatedEnclaveCertificateIamRolesInput{CertificateArn: aws.String(certArn)},
	)
	require.NoError(t, err)
	require.Len(t, getCertRolesOut.AssociatedRoles, 1)
	assert.Equal(t, roleArn, aws.ToString(getCertRolesOut.AssociatedRoles[0].AssociatedRoleArn))

	disassocCertOut, err := client.DisassociateEnclaveCertificateIamRole(
		t.Context(), &ec2sdk.DisassociateEnclaveCertificateIamRoleInput{
			CertificateArn: aws.String(certArn),
			RoleArn:        aws.String(roleArn),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(disassocCertOut.Return))

	instanceID, volumeID := setupInstanceWithVolume(t, backend)
	vols := backend.DescribeVolumes([]string{volumeID})
	require.Len(t, vols, 1)
	require.NotNil(t, vols[0].Attachment)
	assert.Equal(t, instanceID, vols[0].Attachment.InstanceID)

	_, err = backend.EnableInstanceSQLHaStandbyDetections([]string{instanceID}, "creds")
	require.NoError(t, err)

	disableSQLHaOut, err := client.DisableInstanceSqlHaStandbyDetections(
		t.Context(), &ec2sdk.DisableInstanceSqlHaStandbyDetectionsInput{InstanceIds: []string{instanceID}},
	)
	require.NoError(t, err)
	require.Len(t, disableSQLHaOut.Instances, 1)
	assert.Equal(t, instanceID, aws.ToString(disableSQLHaOut.Instances[0].InstanceId))

	createDatafeedOut, err := client.CreateSpotDatafeedSubscription(
		t.Context(), &ec2sdk.CreateSpotDatafeedSubscriptionInput{Bucket: aws.String("spot-datafeed-bucket")},
	)
	require.NoError(t, err)
	require.NotNil(t, createDatafeedOut.SpotDatafeedSubscription)
	assert.Equal(t, "spot-datafeed-bucket", aws.ToString(createDatafeedOut.SpotDatafeedSubscription.Bucket))

	describeDatafeedOut, err := client.DescribeSpotDatafeedSubscription(
		t.Context(), &ec2sdk.DescribeSpotDatafeedSubscriptionInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, describeDatafeedOut.SpotDatafeedSubscription)
	assert.Equal(t, "spot-datafeed-bucket", aws.ToString(describeDatafeedOut.SpotDatafeedSubscription.Bucket))

	_, err = client.DeleteSpotDatafeedSubscription(
		t.Context(), &ec2sdk.DeleteSpotDatafeedSubscriptionInput{},
	)
	require.NoError(t, err)

	cr, err := backend.CreateCapacityReservation("t3.micro", "us-east-1a", 2, nil)
	require.NoError(t, err)

	modCROut, err := client.ModifyCapacityReservation(
		t.Context(), &ec2sdk.ModifyCapacityReservationInput{
			CapacityReservationId: aws.String(cr.CapacityReservationID),
			InstanceCount:         aws.Int32(5),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modCROut.Return))

	cancelCROut, err := client.CancelCapacityReservation(
		t.Context(),
		&ec2sdk.CancelCapacityReservationInput{CapacityReservationId: aws.String(cr.CapacityReservationID)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(cancelCROut.Return))

	fleet, _, err := backend.CreateFleet(ec2.FleetCreateInput{TotalTargetCapacity: 1})
	require.NoError(t, err)

	modFleetOut, err := client.ModifyFleet(
		t.Context(), &ec2sdk.ModifyFleetInput{
			FleetId: aws.String(fleet.FleetID),
			TargetCapacitySpecification: &types.TargetCapacitySpecificationRequest{
				TotalTargetCapacity: aws.Int32(3),
			},
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modFleetOut.Return))

	lt, err := backend.CreateLaunchTemplate("slice26-lt", "ami-test", "t3.micro", nil)
	require.NoError(t, err)
	_, err = backend.CreateLaunchTemplateVersion(lt.ID, "ami-test2", "t3.small")
	require.NoError(t, err)

	modLTOut, err := client.ModifyLaunchTemplate(
		t.Context(), &ec2sdk.ModifyLaunchTemplateInput{
			LaunchTemplateId: aws.String(lt.ID),
			DefaultVersion:   aws.String("2"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modLTOut.LaunchTemplate)
	assert.Equal(t, int64(2), aws.ToInt64(modLTOut.LaunchTemplate.DefaultVersionNumber))

	ipv6Pool := backend.CreateIpv6Pool("slice26 ipv6 pool", []string{"2001:db8::/32"})

	poolsOut, err := client.DescribeIpv6Pools(
		t.Context(), &ec2sdk.DescribeIpv6PoolsInput{PoolIds: []string{ipv6Pool.PoolID}},
	)
	require.NoError(t, err)
	require.Len(t, poolsOut.Ipv6Pools, 1)
	assert.Equal(t, ipv6Pool.PoolID, aws.ToString(poolsOut.Ipv6Pools[0].PoolId))

	assocCidrsOut, err := client.GetAssociatedIpv6PoolCidrs(
		t.Context(), &ec2sdk.GetAssociatedIpv6PoolCidrsInput{PoolId: aws.String(ipv6Pool.PoolID)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, assocCidrsOut.Ipv6CidrAssociations)

	img, err := backend.RegisterImage("slice26-ancestry-ami", "test", "x86_64")
	require.NoError(t, err)

	ancestryOut, err := client.GetImageAncestry(
		t.Context(), &ec2sdk.GetImageAncestryInput{ImageId: aws.String(img.ImageID)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, ancestryOut.ImageAncestryEntries)
	assert.Equal(t, img.ImageID, aws.ToString(ancestryOut.ImageAncestryEntries[0].ImageId))

	_, err = client.CancelImageLaunchPermission(
		t.Context(), &ec2sdk.CancelImageLaunchPermissionInput{ImageId: aws.String(img.ImageID)},
	)
	require.NoError(t, err)

	// RestoreImageFromRecycleBin: nothing ever puts an image into this
	// backend's recycle bin (see the backend's own doc comment on
	// RestoreImageFromRecycleBin), so the happy path is structurally
	// unreachable -- covering it means asserting the correct NotFound error.
	_, err = client.RestoreImageFromRecycleBin(
		t.Context(), &ec2sdk.RestoreImageFromRecycleBinInput{ImageId: aws.String("ami-doesnotexist")},
	)
	require.Error(t, err)
	assert.Equal(t, "InvalidAMIID.NotFound", smithyErrCode(t, err))

	confirmOut, err := client.ConfirmProductInstance(
		t.Context(), &ec2sdk.ConfirmProductInstanceInput{
			InstanceId:  aws.String(instanceID),
			ProductCode: aws.String("prod-code-1"),
		},
	)
	require.NoError(t, err)
	assert.False(t, aws.ToBool(confirmOut.Return))
	assert.Nil(t, confirmOut.OwnerId)
}

// ---- singletons C ----

// runSingletonsC covers the 15 ops the census's second pass (after this
// slice's other fixes) revealed were still genuinely uncovered: image
// watermarks, store/restore image tasks, image usage report deletion, TGW
// multicast domain delete/disassociate, TGW route table delete, TGW prefix
// list reference delete, IPAM resource discovery modify, and the four
// IPAM discovery Get* ops (always-empty by design -- this mock implements
// no live discovery pipeline).
func runSingletonsC(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	img, err := backend.RegisterImage("slice26-watermark-ami", "test", "x86_64")
	require.NoError(t, err)

	attachOut, err := client.AttachImageWatermark(
		t.Context(), &ec2sdk.AttachImageWatermarkInput{
			ImageId:       aws.String(img.ImageID),
			WatermarkName: aws.String("approved"),
		},
	)
	require.NoError(t, err)
	watermarkKey := aws.ToString(attachOut.WatermarkKey)
	require.NotEmpty(t, watermarkKey)
	assert.Contains(t, watermarkKey, "approved")

	detachOut, err := client.DetachImageWatermark(
		t.Context(), &ec2sdk.DetachImageWatermarkInput{
			ImageId:      aws.String(img.ImageID),
			WatermarkKey: aws.String(watermarkKey),
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(detachOut.Return))

	createStoreOut, err := client.CreateStoreImageTask(
		t.Context(), &ec2sdk.CreateStoreImageTaskInput{
			ImageId: aws.String(img.ImageID),
			Bucket:  aws.String("store-image-bucket"),
		},
	)
	require.NoError(t, err)
	objectKey := aws.ToString(createStoreOut.ObjectKey)
	require.NotEmpty(t, objectKey)

	describeStoreOut, err := client.DescribeStoreImageTasks(
		t.Context(), &ec2sdk.DescribeStoreImageTasksInput{ImageIds: []string{img.ImageID}},
	)
	require.NoError(t, err)
	require.Len(t, describeStoreOut.StoreImageTaskResults, 1)
	assert.Equal(t, img.ImageID, aws.ToString(describeStoreOut.StoreImageTaskResults[0].AmiId))
	assert.Equal(t, objectKey, aws.ToString(describeStoreOut.StoreImageTaskResults[0].S3objectKey))

	createRestoreOut, err := client.CreateRestoreImageTask(
		t.Context(), &ec2sdk.CreateRestoreImageTaskInput{
			Bucket:    aws.String("store-image-bucket"),
			ObjectKey: aws.String(objectKey),
			Name:      aws.String("restored-ami"),
		},
	)
	require.NoError(t, err)
	restoredImageID := aws.ToString(createRestoreOut.ImageId)
	require.NotEmpty(t, restoredImageID)
	require.NotEqual(t, img.ImageID, restoredImageID)

	report, err := backend.CreateImageUsageReport(img.ImageID, nil, nil)
	require.NoError(t, err)

	delReportOut, err := client.DeleteImageUsageReport(
		t.Context(), &ec2sdk.DeleteImageUsageReportInput{ReportId: aws.String(report.ReportID)},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(delReportOut.Return))

	tgw, att1, _ := setupTGWWithTwoVPCAttachments(t, backend)

	rt, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	delRTOut, err := client.DeleteTransitGatewayRouteTable(
		t.Context(),
		&ec2sdk.DeleteTransitGatewayRouteTableInput{TransitGatewayRouteTableId: aws.String(rt.RouteTableID)},
	)
	require.NoError(t, err)
	require.NotNil(t, delRTOut.TransitGatewayRouteTable)
	assert.Equal(t, rt.RouteTableID, aws.ToString(delRTOut.TransitGatewayRouteTable.TransitGatewayRouteTableId))

	rt2, err := backend.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	pl, err := backend.CreateManagedPrefixList("slice26-delref-pl", "IPv4", 10, nil)
	require.NoError(t, err)
	_, err = backend.CreateTransitGatewayPrefixListReference(rt2.RouteTableID, pl.PrefixListID, false)
	require.NoError(t, err)

	delRefOut, err := client.DeleteTransitGatewayPrefixListReference(
		t.Context(), &ec2sdk.DeleteTransitGatewayPrefixListReferenceInput{
			TransitGatewayRouteTableId: aws.String(rt2.RouteTableID),
			PrefixListId:               aws.String(pl.PrefixListID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delRefOut.TransitGatewayPrefixListReference)
	assert.Equal(t, pl.PrefixListID, aws.ToString(delRefOut.TransitGatewayPrefixListReference.PrefixListId))

	domain, err := backend.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)
	subnetC, err := backend.CreateSubnet(att1.VpcID, "10.0.3.0/24", "us-east-1a")
	require.NoError(t, err)
	_, err = backend.AssociateTransitGatewayMulticastDomain(
		domain.ID,
		att1.TransitGatewayAttachmentID,
		[]string{subnetC.ID},
	)
	require.NoError(t, err)

	disassocDomainOut, err := client.DisassociateTransitGatewayMulticastDomain(
		t.Context(), &ec2sdk.DisassociateTransitGatewayMulticastDomainInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
			TransitGatewayAttachmentId:      aws.String(att1.TransitGatewayAttachmentID),
			SubnetIds:                       []string{subnetC.ID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, disassocDomainOut.Associations)
	assert.Equal(t, domain.ID, aws.ToString(disassocDomainOut.Associations.TransitGatewayMulticastDomainId))

	delDomainOut, err := client.DeleteTransitGatewayMulticastDomain(
		t.Context(), &ec2sdk.DeleteTransitGatewayMulticastDomainInput{
			TransitGatewayMulticastDomainId: aws.String(domain.ID),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, delDomainOut.TransitGatewayMulticastDomain)
	assert.Equal(
		t, domain.ID, aws.ToString(delDomainOut.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId),
	)

	discovery, err := backend.CreateIpamResourceDiscovery("slice26 discovery", []string{"us-east-1"})
	require.NoError(t, err)

	modDiscoveryOut, err := client.ModifyIpamResourceDiscovery(
		t.Context(), &ec2sdk.ModifyIpamResourceDiscoveryInput{
			IpamResourceDiscoveryId: aws.String(discovery.IpamResourceDiscoveryID),
			AddOperatingRegions: []types.AddIpamOperatingRegion{
				{RegionName: aws.String("eu-west-1")},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modDiscoveryOut.IpamResourceDiscovery)
	assert.Contains(t, modDiscoveryOut.IpamResourceDiscovery.OperatingRegions,
		types.IpamOperatingRegion{RegionName: aws.String("eu-west-1")})

	// GetIpamAddressHistory / GetIpamDiscoveredAccounts /
	// GetIpamDiscoveredPublicAddresses / GetIpamDiscoveredResourceCidrs are
	// void-result ops by design (handler_ipam.go: "modeling real IPAM
	// address-usage history requires a live discovery pipeline this mock
	// does not implement") -- covering them means asserting the correctly
	// shaped, empty response, not fabricating discovery state.
	ipam, err := backend.CreateIpam()
	require.NoError(t, err)

	historyOut, err := client.GetIpamAddressHistory(
		t.Context(), &ec2sdk.GetIpamAddressHistoryInput{
			IpamScopeId: aws.String(ipam.PrivateDefaultScopeID),
			Cidr:        aws.String("10.0.0.0/16"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, historyOut.HistoryRecords)

	discoveredAccountsOut, err := client.GetIpamDiscoveredAccounts(
		t.Context(), &ec2sdk.GetIpamDiscoveredAccountsInput{
			IpamResourceDiscoveryId: aws.String(discovery.IpamResourceDiscoveryID),
			DiscoveryRegion:         aws.String(backend.Region),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, discoveredAccountsOut.IpamDiscoveredAccounts)

	discoveredPublicAddrsOut, err := client.GetIpamDiscoveredPublicAddresses(
		t.Context(), &ec2sdk.GetIpamDiscoveredPublicAddressesInput{
			IpamResourceDiscoveryId: aws.String(discovery.IpamResourceDiscoveryID),
			AddressRegion:           aws.String(backend.Region),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, discoveredPublicAddrsOut.IpamDiscoveredPublicAddresses)

	discoveredCidrsOut, err := client.GetIpamDiscoveredResourceCidrs(
		t.Context(), &ec2sdk.GetIpamDiscoveredResourceCidrsInput{
			IpamResourceDiscoveryId: aws.String(discovery.IpamResourceDiscoveryID),
			ResourceRegion:          aws.String(backend.Region),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, discoveredCidrsOut.IpamDiscoveredResourceCidrs)
}
