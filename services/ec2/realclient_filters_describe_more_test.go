package ec2_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestRealClient_DescribeVpcPeeringConnectionsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	a, err := b.CreateVpc("10.10.0.0/16", "")
	require.NoError(t, err)
	v2, err := b.CreateVpc("10.11.0.0/16", "")
	require.NoError(t, err)
	v3, err := b.CreateVpc("10.12.0.0/16", "")
	require.NoError(t, err)
	p1, err := b.CreateVpcPeeringConnection(a.ID, v2.ID, "", "")
	require.NoError(t, err)
	p2, err := b.CreateVpcPeeringConnection(a.ID, v3.ID, "222222222222", "")
	require.NoError(t, err)
	_, err = b.AcceptVpcPeeringConnection(p1.VpcPeeringConnectionID)
	require.NoError(t, err)
	require.NoError(t, b.CreateTags([]string{p2.VpcPeeringConnectionID}, map[string]string{"Owner": "TeamA"}))

	id1, id2 := p1.VpcPeeringConnectionID, p2.VpcPeeringConnectionID

	runTailCases(t, []tailCase{
		{"id", tailFilter("vpc-peering-connection-id", id2), []string{id2}},
		{"status-active", tailFilter("status-code", "active"), []string{id1}},
		{"status-miss", tailFilter("status-code", "rejected"), nil},
		{"requester-vpc", tailFilter("requester-vpc-info.vpc-id", a.ID), []string{id1, id2}},
		{"accepter-vpc", tailFilter("accepter-vpc-info.vpc-id", v3.ID), []string{id2}},
		{"accepter-owner", tailFilter("accepter-vpc-info.owner-id", "222222222222"), []string{id2}},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{id2}},
		{"tag-key-miss", tailFilter("tag-key", "Nope"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.VpcPeeringConnection, error) {
		out, callErr := client.DescribeVpcPeeringConnections(
			ctx, &ec2sdk.DescribeVpcPeeringConnectionsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.VpcPeeringConnections, nil
	}, func(p types.VpcPeeringConnection) string { return aws.ToString(p.VpcPeeringConnectionId) })
}

func TestRealClient_GetSecurityGroupsForVpcFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = tailAcct
	client := newTestEC2Client(t, h)

	vpc, err := b.CreateVpc("10.20.0.0/16", "")
	require.NoError(t, err)
	sg1, err := b.CreateSecurityGroup("web", "front end", vpc.ID)
	require.NoError(t, err)
	sg2, err := b.CreateSecurityGroup("db", "back end", vpc.ID)
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"id", tailFilter("group-id", sg1.ID), []string{sg1.ID}},
		{"name", tailFilter("group-name", "db"), []string{sg2.ID}},
		{"description", tailFilter("description", "front end"), []string{sg1.ID}},
		{"owner", append(tailFilter("owner-id", tailAcct), tailFilter("group-id", sg2.ID)...), []string{sg2.ID}},
		{"owner-miss", tailFilter("owner-id", "999999999999"), nil},
		{"primary-vpc-miss", tailFilter("primary-vpc-id", "vpc-nope"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.SecurityGroupForVpc, error) {
		out, callErr := client.GetSecurityGroupsForVpc(
			ctx, &ec2sdk.GetSecurityGroupsForVpcInput{VpcId: aws.String(vpc.ID), Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.SecurityGroupForVpcs, nil
	}, func(s types.SecurityGroupForVpc) string { return aws.ToString(s.GroupId) })
}

func TestRealClient_GetTransitGatewayPolicyTableEntriesFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "t"})
	require.NoError(t, err)
	rt1, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	rt2, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)
	pt, err := b.CreateTransitGatewayPolicyTable(tgw.ID, nil)
	require.NoError(t, err)

	_, err = b.CreateTransitGatewayPolicyTableEntry(pt.TransitGatewayPolicyTableID, &ec2.TransitGatewayPolicyTableEntry{
		PolicyRuleNumber: 10, TargetRouteTableID: rt1.RouteTableID, SourceCidrBlock: "10.0.0.0/8", Protocol: "6",
		SourcePortRange: "80", DestinationCidrBlock: "10.1.0.0/16", DestinationPortRange: "443",
		MetaDataKey: "env", MetaDataValue: "prod",
	})
	require.NoError(t, err)
	_, err = b.CreateTransitGatewayPolicyTableEntry(pt.TransitGatewayPolicyTableID, &ec2.TransitGatewayPolicyTableEntry{
		PolicyRuleNumber: 20, TargetRouteTableID: rt2.RouteTableID, SourceCidrBlock: "172.16.0.0/12", Protocol: "17",
		MetaDataKey: "env", MetaDataValue: "dev",
	})
	require.NoError(t, err)

	pre := "policy-rule."

	runTailCases(t, []tailCase{
		{"number", tailFilter("policy-rule-number", "20"), []string{"20"}},
		{"target", tailFilter("target-route-table-id", rt1.RouteTableID), []string{"10"}},
		{"src-ip", tailFilter(pre+"source-ip", "172.16.0.0/12"), []string{"20"}},
		{"dst-ip", tailFilter(pre+"destination-ip", "10.1.0.0/16"), []string{"10"}},
		{"src-port", tailFilter(pre+"source-port", "80"), []string{"10"}},
		{"dst-port", tailFilter(pre+"destination-port", "443"), []string{"10"}},
		{"protocol", tailFilter(pre+"protocol", "17"), []string{"20"}},
		{"meta-key", tailFilter(pre+"meta-data.key", "env"), []string{"10", "20"}},
		{"meta-value", tailFilter(pre+"meta-data.value", "dev"), []string{"20"}},
		{"miss", tailFilter("policy-rule-number", "99"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.TransitGatewayPolicyTableEntry, error) {
		out, callErr := client.GetTransitGatewayPolicyTableEntries(
			ctx, &ec2sdk.GetTransitGatewayPolicyTableEntriesInput{
				TransitGatewayPolicyTableId: aws.String(pt.TransitGatewayPolicyTableID), Filters: f,
			},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.TransitGatewayPolicyTableEntries, nil
	}, func(e types.TransitGatewayPolicyTableEntry) string { return aws.ToString(e.PolicyRuleNumber) })
}

func TestRealClient_SearchTransitGatewayMulticastGroupsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "t"})
	require.NoError(t, err)
	dom, err := b.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)
	did := dom.ID

	_, err = b.RegisterTransitGatewayMulticastGroupMembers(did, "224.0.0.1", []string{"eni-member"})
	require.NoError(t, err)
	_, err = b.RegisterTransitGatewayMulticastGroupSources(did, "224.0.0.2", []string{"eni-source"})
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"group-ip", tailFilter("group-ip-address", "224.0.0.2"), []string{"eni-source"}},
		{"is-member", tailFilter("is-group-member", "true"), []string{"eni-member"}},
		{"is-source", tailFilter("is-group-source", "true"), []string{"eni-source"}},
		{"member-type", tailFilter("member-type", "static"), []string{"eni-member"}},
		{"source-type-miss", tailFilter("source-type", "igmp"), nil},
		{"resource-id", tailFilter("resource-id", "eni-source"), []string{"eni-source"}},
		{"resource-type-miss", tailFilter("resource-type", "tgw-peering"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.TransitGatewayMulticastGroup, error) {
		out, callErr := client.SearchTransitGatewayMulticastGroups(
			ctx, &ec2sdk.SearchTransitGatewayMulticastGroupsInput{
				TransitGatewayMulticastDomainId: aws.String(did), Filters: f,
			},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.MulticastGroups, nil
	}, func(g types.TransitGatewayMulticastGroup) string { return aws.ToString(g.NetworkInterfaceId) })
}

func TestRealClient_DescribeInstanceTopologyFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	small, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	large, err := b.RunInstances("ami-123", "m5.large", "", 1)
	require.NoError(t, err)

	idS, idL := small[0].ID, large[0].ID

	runTailCases(t, []tailCase{
		{"type", tailFilter("instance-type", "m5.large"), []string{idL}},
		{"type-wildcard", tailFilter("instance-type", "t2.*"), []string{idS}},
		{"az", tailFilter("availability-zone", "us-east-1a"), []string{idS, idL}},
		{"az-miss", tailFilter("availability-zone", "us-east-1z"), nil},
		{"zone-id", tailFilter("zone-id", "us-east-1a1"), []string{idS, idL}},
		{"zone-id-miss", tailFilter("zone-id", "use1-az9"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.InstanceTopology, error) {
		out, callErr := client.DescribeInstanceTopology(ctx, &ec2sdk.DescribeInstanceTopologyInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.Instances, nil
	}, func(i types.InstanceTopology) string { return aws.ToString(i.InstanceId) })
}

func TestRealClient_DescribeInstanceImageMetadataFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	small, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	large, err := b.RunInstances("ami-123", "m5.large", "", 1)
	require.NoError(t, err)
	require.NoError(t, b.CreateTags([]string{small[0].ID}, map[string]string{"Owner": "TeamA"}))

	idS, idL := small[0].ID, large[0].ID
	year := time.Now().UTC().Format("2006") + "*"

	runTailCases(t, []tailCase{
		{"id", tailFilter("instance-id", idL), []string{idL}},
		{"type", tailFilter("instance-type", "t2.micro"), []string{idS}},
		{"state", tailFilter("instance-state-name", small[0].State.Name), []string{idS, idL}},
		{"state-miss", tailFilter("instance-state-name", "stopped"), nil},
		{"owner", tailFilter("owner-id", tailAcct), []string{idS, idL}},
		{"owner-miss", tailFilter("owner-id", "999999999999"), nil},
		{"az", tailFilter("availability-zone", "us-east-1a"), []string{idS, idL}},
		{"zone-id-miss", tailFilter("zone-id", "use1-az9"), nil},
		{"launch-hit", tailFilter("launch-time", year), []string{idS, idL}},
		{"launch-miss", tailFilter("launch-time", "1999*"), nil},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{idS}},
		{"tag-key", tailFilter("tag-key", "Owner"), []string{idS}},
	}, func(ctx context.Context, f []types.Filter) ([]types.InstanceImageMetadata, error) {
		out, callErr := client.DescribeInstanceImageMetadata(
			ctx, &ec2sdk.DescribeInstanceImageMetadataInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.InstanceImageMetadata, nil
	}, func(i types.InstanceImageMetadata) string { return aws.ToString(i.InstanceId) })
}

func TestRealClient_DescribeCapacityReservationTopologyFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	c1, err := b.CreateCapacityReservation("p4d.24xlarge", "us-east-1a", "open", "default", 1, nil)
	require.NoError(t, err)
	c2, err := b.CreateCapacityReservation("m5.large", "us-east-1b", "open", "default", 1, nil)
	require.NoError(t, err)

	id1, id2 := c1.CapacityReservationID, c2.CapacityReservationID

	runTailCases(t, []tailCase{
		{"az", tailFilter("availability-zone", "us-east-1b"), []string{id2}},
		{"type", tailFilter("instance-type", "m5.large"), []string{id2}},
		{"type-wildcard", tailFilter("instance-type", "p4d*"), []string{id1}},
		{"miss", tailFilter("instance-type", "c5.*"), nil},
		{"none", nil, []string{id1, id2}},
	}, func(ctx context.Context, f []types.Filter) ([]types.CapacityReservationTopology, error) {
		out, callErr := client.DescribeCapacityReservationTopology(
			ctx, &ec2sdk.DescribeCapacityReservationTopologyInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.CapacityReservations, nil
	}, func(c types.CapacityReservationTopology) string { return aws.ToString(c.CapacityReservationId) })
}

func TestRealClient_SearchLocalGatewayRoutesFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	lg, err := b.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)
	rt, err := b.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)
	rtID := rt.LocalGatewayRouteTableID

	_, err = b.CreateLocalGatewayRoute(rtID, "10.0.0.0/24", "", "", "")
	require.NoError(t, err)
	_, err = b.CreateLocalGatewayRoute(rtID, "", "pl-0123", "", "")
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"type", tailFilter("type", "static"), []string{"10.0.0.0/24", ""}},
		{"type-miss", tailFilter("type", "propagated"), nil},
		{"prefix-list", tailFilter("prefix-list-id", "pl-0123"), []string{""}},
		{"prefix-list-miss", tailFilter("prefix-list-id", "pl-nope"), nil},
		{"state", tailFilter("state", "active"), []string{"10.0.0.0/24", ""}},
	}, func(ctx context.Context, f []types.Filter) ([]types.LocalGatewayRoute, error) {
		out, callErr := client.SearchLocalGatewayRoutes(ctx, &ec2sdk.SearchLocalGatewayRoutesInput{
			LocalGatewayRouteTableId: aws.String(rtID), Filters: f,
		})
		if callErr != nil {
			return nil, callErr
		}

		return out.Routes, nil
	}, func(r types.LocalGatewayRoute) string { return aws.ToString(r.DestinationCidrBlock) })
}

func TestRealClient_DescribeCapacityBlocksDateFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	offerings, err := b.DescribeCapacityBlockOfferings("p4d.24xlarge", 24, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(offerings), 2)
	blk1, cr1, err := b.PurchaseCapacityBlock(offerings[0].CapacityBlockOfferingID, "", nil)
	require.NoError(t, err)
	blk2, _, err := b.PurchaseCapacityBlock(offerings[1].CapacityBlockOfferingID, "", nil)
	require.NoError(t, err)

	ext, err := b.DescribeCapacityBlockExtensionOfferings(cr1.CapacityReservationID, 12)
	require.NoError(t, err)
	_, err = b.PurchaseCapacityBlockExtension(ext[0].CapacityBlockExtensionOfferingID, cr1.CapacityReservationID)
	require.NoError(t, err)

	id1, id2 := blk1.CapacityBlockID, blk2.CapacityBlockID
	year := time.Now().UTC().Format("2006") + "*"

	var extended *ec2.CapacityBlock

	for _, c := range b.DescribeCapacityBlocks(nil, nil) {
		if c.CapacityBlockID == id1 {
			extended = c
		}
	}

	require.NotNil(t, extended)

	runTailCases(t, []tailCase{
		{"create-hit", tailFilter("create-date", year), []string{id1, id2}},
		{"create-miss", tailFilter("create-date", "1999*"), nil},
		{"start-miss", tailFilter("start-date", "1999*"), nil},
		{"end-exact", tailFilter("end-date", extended.EndDate.Format(time.RFC3339)), []string{id1}},
	}, func(ctx context.Context, f []types.Filter) ([]types.CapacityBlock, error) {
		out, callErr := client.DescribeCapacityBlocks(ctx, &ec2sdk.DescribeCapacityBlocksInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.CapacityBlocks, nil
	}, func(c types.CapacityBlock) string { return aws.ToString(c.CapacityBlockId) })
}

func TestRealClient_DescribeCapacityBlockExtensionHistoryFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	offerings, err := b.DescribeCapacityBlockOfferings("p4d.24xlarge", 24, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(offerings), 2)

	offeringIDs := make([]string, 0, 2)

	for _, o := range offerings[:2] {
		_, cr, purchaseErr := b.PurchaseCapacityBlock(o.CapacityBlockOfferingID, "", nil)
		require.NoError(t, purchaseErr)

		ext, extErr := b.DescribeCapacityBlockExtensionOfferings(cr.CapacityReservationID, 12)
		require.NoError(t, extErr)
		_, extErr = b.PurchaseCapacityBlockExtension(ext[0].CapacityBlockExtensionOfferingID, cr.CapacityReservationID)
		require.NoError(t, extErr)

		offeringIDs = append(offeringIDs, ext[0].CapacityBlockExtensionOfferingID)
	}

	runTailCases(t, []tailCase{
		{"first", tailFilter("capacity-block-extension-offering-id", offeringIDs[0]), offeringIDs[:1]},
		{"second", tailFilter("capacity-block-extension-offering-id", offeringIDs[1]), offeringIDs[1:]},
		{"miss", tailFilter("capacity-block-extension-offering-id", "cbeo-nope"), nil},
		{"none", nil, offeringIDs},
	}, func(ctx context.Context, f []types.Filter) ([]types.CapacityBlockExtension, error) {
		out, callErr := client.DescribeCapacityBlockExtensionHistory(
			ctx, &ec2sdk.DescribeCapacityBlockExtensionHistoryInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.CapacityBlockExtensions, nil
	}, func(e types.CapacityBlockExtension) string { return aws.ToString(e.CapacityBlockExtensionOfferingId) })
}

func TestRealClient_DescribeInstanceEventWindowsInstanceTagFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	insts, err := b.RunInstances("ami-123", "t2.micro", "", 2)
	require.NoError(t, err)
	require.NoError(t, b.CreateTags([]string{insts[0].ID}, map[string]string{"Team": "infra"}))
	require.NoError(t, b.CreateTags([]string{insts[1].ID}, map[string]string{"Role": "db"}))

	w1, err := b.CreateInstanceEventWindow("w1", "0-4 * * * 1,5")
	require.NoError(t, err)
	w2, err := b.CreateInstanceEventWindow("w2", "0-4 * * * 1,5")
	require.NoError(t, err)
	_, err = b.AssociateInstanceEventWindow(w1.InstanceEventWindowID, []string{insts[0].ID}, nil)
	require.NoError(t, err)
	_, err = b.AssociateInstanceEventWindow(w2.InstanceEventWindowID, []string{insts[1].ID}, nil)
	require.NoError(t, err)

	id1, id2 := w1.InstanceEventWindowID, w2.InstanceEventWindowID

	runTailCases(t, []tailCase{
		{"key", tailFilter("instance-tag-key", "Team"), []string{id1}},
		{"key-other", tailFilter("instance-tag-key", "Role"), []string{id2}},
		{"key-miss", tailFilter("instance-tag-key", "Nope"), nil},
		{"value", tailFilter("instance-tag-value", "db"), []string{id2}},
		{"value-miss", tailFilter("instance-tag-value", "nope"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.InstanceEventWindow, error) {
		out, callErr := client.DescribeInstanceEventWindows(
			ctx, &ec2sdk.DescribeInstanceEventWindowsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.InstanceEventWindows, nil
	}, func(w types.InstanceEventWindow) string { return aws.ToString(w.InstanceEventWindowId) })
}
