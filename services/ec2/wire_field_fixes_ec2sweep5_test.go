package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeReservedInstances_Tags_RealClient covers gopherstack-g8k9:
// ReservedInstance is a taggable resource (resourceExistsLocked recognises
// b.reservedInstances), but DescribeReservedInstances never emitted tagSet.
func TestDescribeReservedInstances_Tags_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	b.SeedReservedInstancesOffering(
		"rio-g8k9-001", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", 94608000, 500.0, 0.0,
	)
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	purchaseInput := &ec2sdk.PurchaseReservedInstancesOfferingInput{
		ReservedInstancesOfferingId: aws.String("rio-g8k9-001"),
		InstanceCount:               aws.Int32(1),
	}
	purchase, err := client.PurchaseReservedInstancesOffering(t.Context(), purchaseInput)
	require.NoError(t, err)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(purchase.ReservedInstancesId)},
		Tags:      []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeReservedInstances(t.Context(), &ec2sdk.DescribeReservedInstancesInput{
		ReservedInstancesIds: []string{aws.ToString(purchase.ReservedInstancesId)},
	})
	require.NoError(t, err)
	require.Len(t, out.ReservedInstances, 1)
	require.Len(t, out.ReservedInstances[0].Tags, 1, "tagSet empty - DescribeReservedInstances dropped it")
	assert.Equal(t, "env", aws.ToString(out.ReservedInstances[0].Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(out.ReservedInstances[0].Tags[0].Value))
}

// TestTrafficMirrorResources_Tags_RealClient covers gopherstack-g8k9 for all
// four Traffic Mirror resource types: each supports TagSpecifications on
// Create (real client-side required for none, but all four accept it per
// ec2@v1.319.1's api_op_Create* files) and each is recognised by
// resourceExistsLocked, but none of the four Create/Describe response paths
// ever emitted tagSet before this fix.
func TestTrafficMirrorResources_Tags_RealClient(t *testing.T) {
	t.Parallel()

	tagSpec := func(rt types.ResourceType) []types.TagSpecification {
		return []types.TagSpecification{{
			ResourceType: rt,
			Tags:         []types.Tag{{Key: aws.String("owner"), Value: aws.String("g8k9")}},
		}}
	}

	assertTagged := func(t *testing.T, tags []types.Tag, label string) {
		t.Helper()
		require.Len(t, tags, 1, "tagSet empty - "+label+" dropped it")
		assert.Equal(t, "owner", aws.ToString(tags[0].Key))
		assert.Equal(t, "g8k9", aws.ToString(tags[0].Value))
	}

	t.Run("filter and nested rule", func(t *testing.T) {
		t.Parallel()

		h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
		client := newTestEC2Client(t, h)

		filter, err := client.CreateTrafficMirrorFilter(t.Context(), &ec2sdk.CreateTrafficMirrorFilterInput{
			TagSpecifications: tagSpec(types.ResourceTypeTrafficMirrorFilter),
		})
		require.NoError(t, err)
		assertTagged(t, filter.TrafficMirrorFilter.Tags, "CreateTrafficMirrorFilter")

		rule, err := client.CreateTrafficMirrorFilterRule(t.Context(), &ec2sdk.CreateTrafficMirrorFilterRuleInput{
			TrafficMirrorFilterId: filter.TrafficMirrorFilter.TrafficMirrorFilterId,
			TrafficDirection:      types.TrafficDirectionIngress,
			RuleAction:            types.TrafficMirrorRuleActionAccept,
			DestinationCidrBlock:  aws.String("0.0.0.0/0"),
			SourceCidrBlock:       aws.String("10.0.0.0/8"),
			RuleNumber:            aws.Int32(100),
			TagSpecifications:     tagSpec(types.ResourceTypeTrafficMirrorFilterRule),
		})
		require.NoError(t, err)
		assertTagged(t, rule.TrafficMirrorFilterRule.Tags, "CreateTrafficMirrorFilterRule")

		out, err := client.DescribeTrafficMirrorFilters(t.Context(), &ec2sdk.DescribeTrafficMirrorFiltersInput{
			TrafficMirrorFilterIds: []string{aws.ToString(filter.TrafficMirrorFilter.TrafficMirrorFilterId)},
		})
		require.NoError(t, err)
		require.Len(t, out.TrafficMirrorFilters, 1)
		assertTagged(t, out.TrafficMirrorFilters[0].Tags, "DescribeTrafficMirrorFilters filter")
		require.Len(t, out.TrafficMirrorFilters[0].IngressFilterRules, 1)
		assertTagged(
			t,
			out.TrafficMirrorFilters[0].IngressFilterRules[0].Tags,
			"DescribeTrafficMirrorFilters nested rule",
		)
	})

	t.Run("target", func(t *testing.T) {
		t.Parallel()

		h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
		client := newTestEC2Client(t, h)

		target, err := client.CreateTrafficMirrorTarget(t.Context(), &ec2sdk.CreateTrafficMirrorTargetInput{
			NetworkInterfaceId: aws.String("eni-g8k9test"),
			TagSpecifications:  tagSpec(types.ResourceTypeTrafficMirrorTarget),
		})
		require.NoError(t, err)
		assertTagged(t, target.TrafficMirrorTarget.Tags, "CreateTrafficMirrorTarget")

		out, err := client.DescribeTrafficMirrorTargets(t.Context(), &ec2sdk.DescribeTrafficMirrorTargetsInput{
			TrafficMirrorTargetIds: []string{aws.ToString(target.TrafficMirrorTarget.TrafficMirrorTargetId)},
		})
		require.NoError(t, err)
		require.Len(t, out.TrafficMirrorTargets, 1)
		assertTagged(t, out.TrafficMirrorTargets[0].Tags, "DescribeTrafficMirrorTargets")
	})

	t.Run("session", func(t *testing.T) {
		t.Parallel()

		h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
		client := newTestEC2Client(t, h)

		target, err := client.CreateTrafficMirrorTarget(t.Context(), &ec2sdk.CreateTrafficMirrorTargetInput{
			NetworkInterfaceId: aws.String("eni-g8k9target"),
		})
		require.NoError(t, err)

		filter, err := client.CreateTrafficMirrorFilter(t.Context(), &ec2sdk.CreateTrafficMirrorFilterInput{})
		require.NoError(t, err)

		session, err := client.CreateTrafficMirrorSession(t.Context(), &ec2sdk.CreateTrafficMirrorSessionInput{
			NetworkInterfaceId:    aws.String("eni-g8k9session"),
			TrafficMirrorTargetId: target.TrafficMirrorTarget.TrafficMirrorTargetId,
			TrafficMirrorFilterId: filter.TrafficMirrorFilter.TrafficMirrorFilterId,
			SessionNumber:         aws.Int32(1),
			TagSpecifications:     tagSpec(types.ResourceTypeTrafficMirrorSession),
		})
		require.NoError(t, err)
		assertTagged(t, session.TrafficMirrorSession.Tags, "CreateTrafficMirrorSession")

		out, err := client.DescribeTrafficMirrorSessions(t.Context(), &ec2sdk.DescribeTrafficMirrorSessionsInput{
			TrafficMirrorSessionIds: []string{aws.ToString(session.TrafficMirrorSession.TrafficMirrorSessionId)},
		})
		require.NoError(t, err)
		require.Len(t, out.TrafficMirrorSessions, 1)
		assertTagged(t, out.TrafficMirrorSessions[0].Tags, "DescribeTrafficMirrorSessions")
	})
}

// TestVpcEndpointServiceConfiguration_NlbArnsAndPrivateDns_RealClient covers
// gopherstack-g8k9: VpcEndpointServiceConfig.NetworkLoadBalancerARNs is set
// at Create time and PrivateDNSNameState is live-toggled by the real
// StartVpcEndpointServicePrivateDnsVerification operation, but neither
// Create nor Describe ever emitted networkLoadBalancerArnSet or
// privateDnsNameConfiguration.
func TestVpcEndpointServiceConfiguration_NlbArnsAndPrivateDns_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	nlbArn := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/g8k9/abc"

	created, err := client.CreateVpcEndpointServiceConfiguration(
		t.Context(),
		&ec2sdk.CreateVpcEndpointServiceConfigurationInput{NetworkLoadBalancerArns: []string{nlbArn}},
	)
	require.NoError(t, err)
	require.Len(t, created.ServiceConfiguration.NetworkLoadBalancerArns, 1,
		"NetworkLoadBalancerArns empty - CreateVpcEndpointServiceConfiguration dropped it")
	assert.Equal(t, nlbArn, created.ServiceConfiguration.NetworkLoadBalancerArns[0])

	svcID := created.ServiceConfiguration.ServiceId

	_, err = client.StartVpcEndpointServicePrivateDnsVerification(
		t.Context(),
		&ec2sdk.StartVpcEndpointServicePrivateDnsVerificationInput{ServiceId: svcID},
	)
	require.NoError(t, err)

	out, err := client.DescribeVpcEndpointServiceConfigurations(
		t.Context(),
		&ec2sdk.DescribeVpcEndpointServiceConfigurationsInput{ServiceIds: []string{aws.ToString(svcID)}},
	)
	require.NoError(t, err)
	require.Len(t, out.ServiceConfigurations, 1)
	cfg := out.ServiceConfigurations[0]
	require.Len(t, cfg.NetworkLoadBalancerArns, 1,
		"NetworkLoadBalancerArns empty - DescribeVpcEndpointServiceConfigurations dropped it")
	assert.Equal(t, nlbArn, cfg.NetworkLoadBalancerArns[0])
	require.NotNil(t, cfg.PrivateDnsNameConfiguration,
		"PrivateDnsNameConfiguration nil - verification state never surfaced")
	assert.Equal(t, types.DnsNameStateVerified, cfg.PrivateDnsNameConfiguration.State)
}

// TestDescribeHosts_ModifiedFields_RealClient covers gopherstack-g8k9:
// ModifyHosts (a real operation) mutates AutoPlacement, HostRecovery,
// HostMaintenance and InstanceFamily on a Dedicated Host, but DescribeHosts
// never emitted any of the four - InstanceType was also emitted at the
// wrong (flat, non-existent) wire location instead of nested under
// hostProperties.
func TestDescribeHosts_ModifiedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	alloc, err := client.AllocateHosts(t.Context(), &ec2sdk.AllocateHostsInput{
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceType:     aws.String("m5.large"),
		Quantity:         aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, alloc.HostIds, 1)
	hostID := alloc.HostIds[0]

	_, err = client.ModifyHosts(t.Context(), &ec2sdk.ModifyHostsInput{
		HostIds:         []string{hostID},
		AutoPlacement:   types.AutoPlacementOn,
		HostRecovery:    types.HostRecoveryOn,
		HostMaintenance: types.HostMaintenanceOff,
		InstanceFamily:  aws.String("m5"),
	})
	require.NoError(t, err)

	out, err := client.DescribeHosts(t.Context(), &ec2sdk.DescribeHostsInput{HostIds: []string{hostID}})
	require.NoError(t, err)
	require.Len(t, out.Hosts, 1)
	host := out.Hosts[0]

	assert.Equal(t, types.AutoPlacementOn, host.AutoPlacement, "AutoPlacement not surfaced")
	assert.Equal(t, types.HostRecoveryOn, host.HostRecovery, "HostRecovery not surfaced")
	assert.Equal(t, types.HostMaintenanceOff, host.HostMaintenance, "HostMaintenance not surfaced")
	require.NotNil(t, host.HostProperties, "HostProperties nil - InstanceFamily never nested correctly")
	assert.Equal(t, "m5", aws.ToString(host.HostProperties.InstanceFamily))
}

// TestDescribeImageAttribute_Description_RealClient covers gopherstack-g8k9:
// ModifyImageAttribute's Description.Value form is captured into the
// backend's generic imageAttributes store, but DescribeImageAttribute
// always returned an empty placeholder for every attribute except a
// hardcoded launchPermission stub.
func TestDescribeImageAttribute_Description_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	const imageID = "ami-0c55b159cbfafe1f0"

	_, err := client.ModifyImageAttribute(t.Context(), &ec2sdk.ModifyImageAttributeInput{
		ImageId:     aws.String(imageID),
		Description: &types.AttributeValue{Value: aws.String("g8k9 description")},
	})
	require.NoError(t, err)

	out, err := client.DescribeImageAttribute(t.Context(), &ec2sdk.DescribeImageAttributeInput{
		ImageId:   aws.String(imageID),
		Attribute: types.ImageAttributeNameDescription,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Description, "Description nil - DescribeImageAttribute never read back the stored value")
	assert.Equal(t, "g8k9 description", aws.ToString(out.Description.Value))
}
