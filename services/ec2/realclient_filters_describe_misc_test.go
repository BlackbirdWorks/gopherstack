package ec2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func newMiscClient(t *testing.T) (*ec2.InMemoryBackend, *ec2sdk.Client) {
	t.Helper()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")

	return b, newTestEC2Client(t, ec2.NewHandler(b))
}

func TestRealClient_DescribeRegionsFilters(t *testing.T) {
	t.Parallel()

	_, client := newMiscClient(t)

	runTailCases(t, []tailCase{
		{"name", tailFilter("region-name", "eu-west-1", "us-west-2"), []string{"eu-west-1", "us-west-2"}},
		{"endpoint", tailFilter("endpoint", "ec2.us-east-2.amazonaws.com"), []string{"us-east-2"}},
		{"miss", tailFilter("region-name", "mars-north-1"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.Region, error) {
		out, err := client.DescribeRegions(ctx, &ec2sdk.DescribeRegionsInput{Filters: f})
		if err != nil {
			return nil, err
		}

		return out.Regions, nil
	}, func(r types.Region) string { return aws.ToString(r.RegionName) })
}

func TestRealClient_DescribeNetworkInterfacePermissionsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	ni1, err := b.CreateNetworkInterface("subnet-default", "one")
	require.NoError(t, err)
	ni2, err := b.CreateNetworkInterface("subnet-default", "two")
	require.NoError(t, err)
	p1, err := b.CreateNetworkInterfacePermission(ni1.ID, "111111111111", "", "INSTANCE-ATTACH")
	require.NoError(t, err)
	p2, err := b.CreateNetworkInterfacePermission(ni2.ID, "222222222222", "", "EIP-ASSOCIATE")
	require.NoError(t, err)

	pre := "network-interface-permission."

	runTailCases(t, []tailCase{
		{"id", tailFilter(pre+"network-interface-permission-id", p2.PermissionID), []string{p2.PermissionID}},
		{"eni", tailFilter(pre+"network-interface-id", ni1.ID), []string{p1.PermissionID}},
		{"account", tailFilter(pre+"aws-account-id", "222222222222"), []string{p2.PermissionID}},
		{"permission", tailFilter(pre+"permission", "INSTANCE-ATTACH"), []string{p1.PermissionID}},
		{"service-miss", tailFilter(pre+"aws-service", "nope"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.NetworkInterfacePermission, error) {
		out, callErr := client.DescribeNetworkInterfacePermissions(
			ctx, &ec2sdk.DescribeNetworkInterfacePermissionsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.NetworkInterfacePermissions, nil
	}, func(p types.NetworkInterfacePermission) string { return aws.ToString(p.NetworkInterfacePermissionId) })
}

func TestRealClient_DescribeIpv6PoolsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	p1 := b.CreateIpv6Pool("a", []string{"2001:db8::/48"})
	p2 := b.CreateIpv6Pool("b", []string{"2001:db9::/48"})
	require.NoError(t, b.CreateTags([]string{p1.PoolID}, map[string]string{"Owner": "TeamA"}))

	runTailCases(t, []tailCase{
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{p1.PoolID}},
		{"tag-miss", tailFilter("tag:Owner", "TeamB"), nil},
		{"tag-key", tailFilter("tag-key", "Owner"), []string{p1.PoolID}},
		{"none", nil, []string{p1.PoolID, p2.PoolID}},
	}, func(ctx context.Context, f []types.Filter) ([]types.Ipv6Pool, error) {
		out, callErr := client.DescribeIpv6Pools(ctx, &ec2sdk.DescribeIpv6PoolsInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.Ipv6Pools, nil
	}, func(p types.Ipv6Pool) string { return aws.ToString(p.PoolId) })
}

func TestRealClient_DescribeTrafficMirrorFilterRulesFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	f, err := b.CreateTrafficMirrorFilter("f", nil)
	require.NoError(t, err)
	r1, err := b.CreateTrafficMirrorFilterRule(
		f.TrafficMirrorFilterID, "ingress", "accept", "10.0.0.0/24", "10.1.0.0/24", "first", 100, 6, nil,
	)
	require.NoError(t, err)
	r2, err := b.CreateTrafficMirrorFilterRule(
		f.TrafficMirrorFilterID, "egress", "reject", "10.2.0.0/24", "10.3.0.0/24", "second", 200, 17, nil,
	)
	require.NoError(t, err)

	id1, id2 := r1.TrafficMirrorFilterRuleID, r2.TrafficMirrorFilterRuleID

	runTailCases(t, []tailCase{
		{"id", tailFilter("traffic-mirror-filter-rule-id", id1), []string{id1}},
		{"filter-id", tailFilter("traffic-mirror-filter-id", f.TrafficMirrorFilterID), []string{id1, id2}},
		{"number", tailFilter("rule-number", "200"), []string{id2}},
		{"action", tailFilter("rule-action", "accept"), []string{id1}},
		{"direction", tailFilter("traffic-direction", "egress"), []string{id2}},
		{"protocol", tailFilter("protocol", "6"), []string{id1}},
		{"src", tailFilter("source-cidr-block", "10.2.0.0/24"), []string{id2}},
		{"dst", tailFilter("destination-cidr-block", "10.1.0.0/24"), []string{id1}},
		{"description", tailFilter("description", "second"), []string{id2}},
		{"miss", tailFilter("rule-number", "300"), nil},
	}, func(ctx context.Context, fl []types.Filter) ([]types.TrafficMirrorFilterRule, error) {
		out, callErr := client.DescribeTrafficMirrorFilterRules(ctx, &ec2sdk.DescribeTrafficMirrorFilterRulesInput{
			TrafficMirrorFilterId: aws.String(f.TrafficMirrorFilterID), Filters: fl,
		})
		if callErr != nil {
			return nil, callErr
		}

		return out.TrafficMirrorFilterRules, nil
	}, func(r types.TrafficMirrorFilterRule) string { return aws.ToString(r.TrafficMirrorFilterRuleId) })
}

func TestRealClient_DescribeReplaceRootVolumeTasksFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	insts, err := b.RunInstances("ami-123", "t2.micro", "", 2)
	require.NoError(t, err)
	t1, err := b.CreateReplaceRootVolumeTask(insts[0].ID, "")
	require.NoError(t, err)
	t2, err := b.CreateReplaceRootVolumeTask(insts[1].ID, "")
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"instance", tailFilter("instance-id", insts[1].ID), []string{t2.ReplaceRootVolumeTaskID}},
		{"miss", tailFilter("instance-id", "i-nope"), nil},
		{"none", nil, []string{t1.ReplaceRootVolumeTaskID, t2.ReplaceRootVolumeTaskID}},
	}, func(ctx context.Context, f []types.Filter) ([]types.ReplaceRootVolumeTask, error) {
		out, callErr := client.DescribeReplaceRootVolumeTasks(
			ctx, &ec2sdk.DescribeReplaceRootVolumeTasksInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.ReplaceRootVolumeTasks, nil
	}, func(r types.ReplaceRootVolumeTask) string { return aws.ToString(r.ReplaceRootVolumeTaskId) })
}

func TestRealClient_DescribeVpcClassicLinkFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	v1, err := b.CreateVpc("10.50.0.0/16", "default")
	require.NoError(t, err)
	v2, err := b.CreateVpc("10.51.0.0/16", "default")
	require.NoError(t, err)
	require.NoError(t, b.EnableVpcClassicLink(v1.ID))
	require.NoError(t, b.CreateTags([]string{v2.ID}, map[string]string{"Owner": "TeamA"}))

	scope := []string{v1.ID, v2.ID}

	runTailCases(t, []tailCase{
		{"enabled", tailFilter("is-classic-link-enabled", "true"), []string{v1.ID}},
		{"disabled", tailFilter("is-classic-link-enabled", "false"), []string{v2.ID}},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{v2.ID}},
		{"tag-key", tailFilter("tag-key", "Nope"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.VpcClassicLink, error) {
		out, callErr := client.DescribeVpcClassicLink(ctx, &ec2sdk.DescribeVpcClassicLinkInput{
			VpcIds: scope, Filters: f,
		})
		if callErr != nil {
			return nil, callErr
		}

		return out.Vpcs, nil
	}, func(v types.VpcClassicLink) string { return aws.ToString(v.VpcId) })
}

func TestRealClient_DescribeReservedInstancesModificationsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	b.SeedReservedInstancesOffering("rio-1", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", "standard",
		94608000, 500.0, 0.0)
	ri1, err := b.PurchaseReservedInstancesOffering("rio-1", 1)
	require.NoError(t, err)
	ri2, err := b.PurchaseReservedInstancesOffering("rio-1", 1)
	require.NoError(t, err)
	m1, err := b.ModifyReservedInstances([]string{ri1.ReservedInstancesID}, []ec2.ReservedInstancesConfigurationTarget{
		{InstanceType: "t3.large", AvailabilityZone: "us-east-1b", InstanceCount: 2},
	})
	require.NoError(t, err)
	m2, err := b.ModifyReservedInstances([]string{ri2.ReservedInstancesID}, []ec2.ReservedInstancesConfigurationTarget{
		{InstanceType: "t3.small", AvailabilityZone: "us-east-1c", InstanceCount: 5},
	})
	require.NoError(t, err)

	id1, id2 := m1.ReservedInstancesModificationID, m2.ReservedInstancesModificationID
	tc := "modification-result.target-configuration."

	runTailCases(t, []tailCase{
		{"mod-id", tailFilter("reserved-instances-modification-id", id2), []string{id2}},
		{"ri-id", tailFilter("reserved-instances-id", ri1.ReservedInstancesID), []string{id1}},
		{"status", tailFilter("status", m1.Status), []string{id1, id2}},
		{"status-miss", tailFilter("status", "failed"), nil},
		{"type", tailFilter(tc+"instance-type", "t3.small"), []string{id2}},
		{"az", tailFilter(tc+"availability-zone", "us-east-1b"), []string{id1}},
		{"count", tailFilter(tc+"instance-count", "5"), []string{id2}},
	}, func(ctx context.Context, f []types.Filter) ([]types.ReservedInstancesModification, error) {
		out, callErr := client.DescribeReservedInstancesModifications(
			ctx, &ec2sdk.DescribeReservedInstancesModificationsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.ReservedInstancesModifications, nil
	}, func(m types.ReservedInstancesModification) string {
		return aws.ToString(m.ReservedInstancesModificationId)
	})
}

func TestRealClient_DescribeOutpostLagsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	arn1 := "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"
	arn2 := "arn:aws:outposts:us-east-1:000000000000:outpost/op-2"
	l1, err := b.SeedOutpostLag(ec2.OutpostLag{OutpostArn: arn1})
	require.NoError(t, err)
	l2, err := b.SeedOutpostLag(ec2.OutpostLag{OutpostArn: arn2, OwnerID: "999999999999"})
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"id", tailFilter("outpost-lag-id", l1.OutpostLagID), []string{l1.OutpostLagID}},
		{"arn", tailFilter("outpost-arn", arn2), []string{l2.OutpostLagID}},
		{"owner", tailFilter("owner-id", "999999999999"), []string{l2.OutpostLagID}},
		{"miss", tailFilter("outpost-lag-id", "olag-nope"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.OutpostLag, error) {
		out, callErr := client.DescribeOutpostLags(ctx, &ec2sdk.DescribeOutpostLagsInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.OutpostLags, nil
	}, func(l types.OutpostLag) string { return aws.ToString(l.OutpostLagId) })
}

func TestRealClient_DescribeVpcBlockPublicAccessExclusionsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	v1, err := b.CreateVpc("10.60.0.0/16", "default")
	require.NoError(t, err)
	v2, err := b.CreateVpc("10.61.0.0/16", "default")
	require.NoError(t, err)
	e1, err := b.CreateVpcBlockPublicAccessExclusion(
		v1.ID, "", "allow-bidirectional", map[string]string{"Owner": "TeamA"},
	)
	require.NoError(t, err)
	e2, err := b.CreateVpcBlockPublicAccessExclusion(v2.ID, "", "allow-egress", nil)
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"arn", tailFilter("resource-arn", e1.ResourceArn), []string{e1.ExclusionID}},
		{"mode", tailFilter("internet-gateway-exclusion-mode", "allow-egress"), []string{e2.ExclusionID}},
		{"state", tailFilter("state", e1.State), []string{e1.ExclusionID, e2.ExclusionID}},
		{"state-miss", tailFilter("state", "delete-complete"), nil},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{e1.ExclusionID}},
		{"tag-key", tailFilter("tag-key", "Owner"), []string{e1.ExclusionID}},
		{"tag-value", tailFilter("tag-value", "TeamA"), []string{e1.ExclusionID}},
	}, func(ctx context.Context, f []types.Filter) ([]types.VpcBlockPublicAccessExclusion, error) {
		out, callErr := client.DescribeVpcBlockPublicAccessExclusions(
			ctx, &ec2sdk.DescribeVpcBlockPublicAccessExclusionsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.VpcBlockPublicAccessExclusions, nil
	}, func(e types.VpcBlockPublicAccessExclusion) string { return aws.ToString(e.ExclusionId) })
}

func TestRealClient_GetSubnetCidrReservationsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	vpc, err := b.CreateVpc("10.70.0.0/16", "default")
	require.NoError(t, err)
	sn, err := b.CreateSubnet(vpc.ID, "10.70.1.0/24", "us-east-1a")
	require.NoError(t, err)
	r1, err := b.CreateSubnetCidrReservation(sn.ID, "10.70.1.0/28", "prefix", "")
	require.NoError(t, err)
	r2, err := b.CreateSubnetCidrReservation(sn.ID, "10.70.1.16/28", "explicit", "")
	require.NoError(t, err)
	require.NoError(t, b.CreateTags([]string{r1.SubnetCIDRReservationID}, map[string]string{"Owner": "TeamA"}))

	id1, id2 := r1.SubnetCIDRReservationID, r2.SubnetCIDRReservationID

	runTailCases(t, []tailCase{
		{"type", tailFilter("reservationType", "explicit"), []string{id2}},
		{"subnet", tailFilter("subnet-id", sn.ID), []string{id1, id2}},
		{"subnet-miss", tailFilter("subnet-id", "subnet-nope"), nil},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{id1}},
		{"tag-key", tailFilter("tag-key", "Owner"), []string{id1}},
	}, func(ctx context.Context, f []types.Filter) ([]types.SubnetCidrReservation, error) {
		out, callErr := client.GetSubnetCidrReservations(ctx, &ec2sdk.GetSubnetCidrReservationsInput{
			SubnetId: aws.String(sn.ID), Filters: f,
		})
		if callErr != nil {
			return nil, callErr
		}

		return out.SubnetIpv4CidrReservations, nil
	}, func(r types.SubnetCidrReservation) string { return aws.ToString(r.SubnetCidrReservationId) })
}

func TestRealClient_DescribeImageUsageReportsFilters(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	img, err := b.RegisterImage("usage-ami", "", "")
	require.NoError(t, err)
	r1, err := b.CreateImageUsageReport(img.ImageID, nil, nil)
	require.NoError(t, err)
	r2, err := b.CreateImageUsageReport(img.ImageID, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.CreateTags([]string{r2.ReportID}, map[string]string{"Owner": "TeamA"}))

	runTailCases(t, []tailCase{
		{"state", tailFilter("state", r1.State), []string{r1.ReportID, r2.ReportID}},
		{"state-miss", tailFilter("state", "error"), nil},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{r2.ReportID}},
		{"tag-key", tailFilter("tag-key", "Owner"), []string{r2.ReportID}},
	}, func(ctx context.Context, f []types.Filter) ([]types.ImageUsageReport, error) {
		out, callErr := client.DescribeImageUsageReports(ctx, &ec2sdk.DescribeImageUsageReportsInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.ImageUsageReports, nil
	}, func(r types.ImageUsageReport) string { return aws.ToString(r.ReportId) })
}
