package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreate_Tags_RoundTrip_FinalBatch covers the last batch of
// gopherstack-wjlrn: CreateClientVpnEndpoint, CreateFleet,
// CreateManagedPrefixList, CreateSubnetCidrReservation,
// CreateVpcEndpointServiceConfiguration and CreateVpcPeeringConnection never
// called parseTagSpecification, so TagSpecifications were silently dropped.
// Each case creates a resource with a tag, then confirms the tag comes back
// on the matching Describe/Get. CreateInterruptibleCapacityReservationAllocation
// is excluded: the backend models the allocation with no distinct resource
// ID (capacity_reservations.go), so there is nothing to attach a tag to.
func TestCreate_Tags_RoundTrip_FinalBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "client vpn endpoint", run: testCreateClientVpnEndpointTags},
		{name: "fleet and launched instances", run: testCreateFleetTags},
		{name: "managed prefix list", run: testCreateManagedPrefixListTags},
		{name: "subnet cidr reservation", run: testCreateSubnetCidrReservationTags},
		{name: "vpc endpoint service configuration", run: testCreateVpcEndpointServiceConfigurationTags},
		{name: "vpc peering connection", run: testCreateVpcPeeringConnectionTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testCreateClientVpnEndpointTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateClientVpnEndpoint"},
		"ClientCidrBlock":                 []string{"10.20.0.0/16"},
		"TagSpecification.1.ResourceType": []string{"client-vpn-endpoint"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)

	id := extractBetween(t, createResp, "<clientVpnEndpointId>", "</clientVpnEndpointId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                []string{"DescribeClientVpnEndpoints"},
		"ClientVpnEndpointId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateFleetTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateFleet"},
		"Type":   []string{"instant"},
		"TargetCapacitySpecification.TotalTargetCapacity": []string{"1"},
		"TagSpecification.1.ResourceType":                 []string{"fleet"},
		"TagSpecification.1.Tag.1.Key":                    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":                  []string{"demo-fleet"},
		"TagSpecification.2.ResourceType":                 []string{"instance"},
		"TagSpecification.2.Tag.1.Key":                    []string{"Name"},
		"TagSpecification.2.Tag.1.Value":                  []string{"demo-instance"},
	})
	require.NoError(t, err)
	// CreateFleetOutput has no tagSet member (api_op_CreateFleet.go
	// CreateFleetOutput: only Errors/FleetId/Instances) -- the fleet tag is
	// only readable back via DescribeFleets, asserted below.
	assert.NotContains(t, createResp, "<tagSet>")

	fleetID := extractBetween(t, createResp, "<fleetId>", "</fleetId>")
	require.NotEmpty(t, fleetID)

	instanceIDsBlock := extractBetween(t, createResp, "<instanceIds>", "</instanceIds>")
	instanceID := extractBetween(t, instanceIDsBlock, "<item>", "</item>")
	require.NotEmpty(t, instanceID, "instant fleet must report its launched instance")

	describeFleetResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"DescribeFleets"},
		"FleetId.1": []string{fleetID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeFleetResp, "<key>Name</key><value>demo-fleet</value>")

	describeInstResp, err := dispatchHandler(h, url.Values{
		"Action":       []string{"DescribeInstances"},
		"InstanceId.1": []string{instanceID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeInstResp, "<key>Name</key><value>demo-instance</value>")
}

func testCreateManagedPrefixListTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateManagedPrefixList"},
		"PrefixListName":                  []string{"my-list"},
		"AddressFamily":                   []string{"IPv4"},
		"MaxEntries":                      []string{"5"},
		"TagSpecification.1.ResourceType": []string{"prefix-list"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<prefixListId>", "</prefixListId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":         []string{"DescribeManagedPrefixLists"},
		"PrefixListId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateSubnetCidrReservationTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateSubnetCidrReservation"},
		"SubnetId":                        []string{"subnet-default"},
		"Cidr":                            []string{"10.0.0.0/28"},
		"TagSpecification.1.ResourceType": []string{"subnet-cidr-reservation"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<subnetCidrReservationId>", "</subnetCidrReservationId>")
	require.NotEmpty(t, id)

	getResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"GetSubnetCidrReservations"},
		"SubnetId": []string{"subnet-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, getResp, "<key>Name</key><value>demo</value>")
}

func testCreateVpcEndpointServiceConfigurationTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	nlbARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/demo/abc"

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVpcEndpointServiceConfiguration"},
		"NetworkLoadBalancerArn.1":        []string{nlbARN},
		"TagSpecification.1.ResourceType": []string{"vpc-endpoint-service"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<serviceId>", "</serviceId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"DescribeVpcEndpointServiceConfigurations"},
		"ServiceId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateVpcPeeringConnectionTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	peerVpc, err := h.Backend.CreateVpc("10.30.0.0/16", "default")
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVpcPeeringConnection"},
		"VpcId":                           []string{"vpc-default"},
		"PeerVpcId":                       []string{peerVpc.ID},
		"TagSpecification.1.ResourceType": []string{"vpc-peering-connection"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>",
		"tag must be visible on the CreateVpcPeeringConnection response itself")

	id := extractBetween(t, createResp, "<vpcPeeringConnectionId>", "</vpcPeeringConnectionId>")
	require.NotEmpty(t, id)

	// Tags must be visible via DescribeVpcPeeringConnections regardless of which
	// side (requester or accepter) is querying -- it is a single resource with
	// one ID, not two independently-tagged views.
	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"DescribeVpcPeeringConnections"},
		"VpcPeeringConnectionId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
	assert.Contains(t, describeResp, id)
}
