package ec2_test

// ec2sweep43: cmd/reqfielddiff's never-declared-field sweep found six
// operations whose real SDK input declares MaxResults/NextToken but whose
// handler read neither, so a client asking for a bounded page got every
// item back in one page and no NextToken. All six are confirmed real by
// reading the pinned SDK's own doc comment on MaxResults for each operation
// and the handler body: DescribeNetworkInterfacePermissions,
// DescribeReservedInstancesOfferings, DescribeScheduledInstanceAvailability,
// DescribeScheduledInstances, GetVpnConnectionDeviceTypes and
// SearchTransitGatewayRoutes.
//
// Four of the six seed past their documented default/max page size and omit
// MaxResults entirely, so the assertions can only pass if the handler
// applies the documented default -- a test that always sets MaxResults
// cannot observe a default at all. The other two
// (DescribeScheduledInstanceAvailability, GetVpnConnectionDeviceTypes) sit
// behind a backend catalog fixed well below their documented MaxResults
// floor (3 static schedule-availability entries against a floor of 5; a
// handful of VPN device types against a floor of 200), so the default page
// size can never be forced to truncate against this backend's data model --
// recorded rather than faked. Both still get a real, testable fix: MaxResults
// values outside the documented range are now rejected, which was silently
// accepted before this pass.

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDescribeNetworkInterfacePermissions_DefaultMaxResults(t *testing.T) {
	t.Parallel()

	const seedCount = 51 // exceeds the documented default of 50

	b, client := newTestBackendAndClient(t)

	eni, err := b.CreateNetworkInterface("subnet-default", "ec2sweep43 eni")
	require.NoError(t, err)

	for i := range seedCount {
		_, permErr := b.CreateNetworkInterfacePermission(
			eni.ID,
			"123456789012",
			"ec2.amazonaws.com",
			fmt.Sprintf("INSTANCE-ATTACH-%d", i),
		)
		require.NoError(t, permErr)
	}

	page1, err := client.DescribeNetworkInterfacePermissions(
		t.Context(),
		&ec2sdk.DescribeNetworkInterfacePermissionsInput{},
	)
	require.NoError(t, err)
	assert.Len(
		t,
		page1.NetworkInterfacePermissions,
		50,
		"must truncate to the documented default of 50 when MaxResults is omitted",
	)
	require.NotNil(t, page1.NextToken)

	page2, err := client.DescribeNetworkInterfacePermissions(
		t.Context(),
		&ec2sdk.DescribeNetworkInterfacePermissionsInput{
			NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	assert.Len(t, page2.NetworkInterfacePermissions, 1)
	assert.Nil(t, page2.NextToken)

	seen := make(map[string]bool, seedCount)
	for _, p := range append(page1.NetworkInterfacePermissions, page2.NetworkInterfacePermissions...) {
		id := aws.ToString(p.NetworkInterfacePermissionId)
		require.False(t, seen[id], "permission %q returned on more than one page", id)
		seen[id] = true
	}
	assert.Len(t, seen, seedCount)
}

func TestDescribeReservedInstancesOfferings_DefaultMaxResults(t *testing.T) {
	t.Parallel()

	const seedCount = 101 // exceeds the documented max/default of 100

	b, client := newTestBackendAndClient(t)

	for i := range seedCount {
		b.SeedReservedInstancesOffering(
			fmt.Sprintf("ec2sweep43-offering-%03d", i), "t3.micro", "us-east-1a", "Linux/UNIX", "Standard",
			31536000, 0, 0.05,
		)
	}

	page1, err := client.DescribeReservedInstancesOfferings(
		t.Context(),
		&ec2sdk.DescribeReservedInstancesOfferingsInput{},
	)
	require.NoError(t, err)
	assert.Len(
		t,
		page1.ReservedInstancesOfferings,
		100,
		"must truncate to the documented default/max of 100 when MaxResults is omitted",
	)
	require.NotNil(t, page1.NextToken)

	page2, err := client.DescribeReservedInstancesOfferings(
		t.Context(),
		&ec2sdk.DescribeReservedInstancesOfferingsInput{
			NextToken: page1.NextToken,
		},
	)
	require.NoError(t, err)
	assert.Len(t, page2.ReservedInstancesOfferings, 1)
	assert.Nil(t, page2.NextToken)

	seen := make(map[string]bool, seedCount)
	for _, o := range append(page1.ReservedInstancesOfferings, page2.ReservedInstancesOfferings...) {
		id := aws.ToString(o.ReservedInstancesOfferingId)
		require.False(t, seen[id], "offering %q returned on more than one page", id)
		seen[id] = true
	}
	assert.Len(t, seen, seedCount)
}

func TestDescribeScheduledInstances_DefaultMaxResults(t *testing.T) {
	t.Parallel()

	const seedCount = 101 // exceeds the documented default of 100

	b, client := newTestBackendAndClient(t)

	requests := make([]ec2.ScheduledInstancePurchaseRequest, seedCount)
	for i := range requests {
		requests[i] = ec2.ScheduledInstancePurchaseRequest{
			PurchaseToken: "sit-us-east-1-c4large-weekly",
			InstanceCount: 1,
		}
	}

	_, err := b.PurchaseScheduledInstances(requests)
	require.NoError(t, err)

	page1, err := client.DescribeScheduledInstances(t.Context(), &ec2sdk.DescribeScheduledInstancesInput{})
	require.NoError(t, err)
	assert.Len(
		t,
		page1.ScheduledInstanceSet,
		100,
		"must truncate to the documented default of 100 when MaxResults is omitted",
	)
	require.NotNil(t, page1.NextToken)

	page2, err := client.DescribeScheduledInstances(t.Context(), &ec2sdk.DescribeScheduledInstancesInput{
		NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.ScheduledInstanceSet, 1)
	assert.Nil(t, page2.NextToken)

	seen := make(map[string]bool, seedCount)
	for _, s := range append(page1.ScheduledInstanceSet, page2.ScheduledInstanceSet...) {
		id := aws.ToString(s.ScheduledInstanceId)
		require.False(t, seen[id], "scheduled instance %q returned on more than one page", id)
		seen[id] = true
	}
	assert.Len(t, seen, seedCount)
}

func TestSearchTransitGatewayRoutes_DefaultMaxResults(t *testing.T) {
	t.Parallel()

	const seedCount = 1001 // exceeds the documented default of 1000

	b, client := newTestBackendAndClient(t)

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{})
	require.NoError(t, err)

	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
	require.NoError(t, err)

	for i := range seedCount {
		cidr := fmt.Sprintf("10.%d.%d.0/32", i/256, i%256)
		_, routeErr := b.CreateTransitGatewayRoute(rt.RouteTableID, cidr, "", true)
		require.NoError(t, routeErr)
	}

	req := &ec2sdk.SearchTransitGatewayRoutesInput{
		TransitGatewayRouteTableId: aws.String(rt.RouteTableID),
		Filters:                    []types.Filter{{Name: aws.String("type"), Values: []string{"static"}}},
	}

	page1, err := client.SearchTransitGatewayRoutes(t.Context(), req)
	require.NoError(t, err)
	assert.Len(t, page1.Routes, 1000, "must truncate to the documented default of 1000 when MaxResults is omitted")
	require.NotNil(t, page1.NextToken)
	assert.True(t, aws.ToBool(page1.AdditionalRoutesAvailable))

	req.NextToken = page1.NextToken
	page2, err := client.SearchTransitGatewayRoutes(t.Context(), req)
	require.NoError(t, err)
	assert.Len(t, page2.Routes, 1)
	assert.Nil(t, page2.NextToken)
	assert.False(t, aws.ToBool(page2.AdditionalRoutesAvailable))

	seen := make(map[string]bool, seedCount)
	for _, r := range append(page1.Routes, page2.Routes...) {
		cidr := aws.ToString(r.DestinationCidrBlock)
		require.False(t, seen[cidr], "route %q returned on more than one page", cidr)
		seen[cidr] = true
	}
	assert.Len(t, seen, seedCount)
}

// TestDescribeScheduledInstanceAvailability_MaxResultsRangeEnforced covers
// the part of api_op_DescribeScheduledInstanceAvailability.go's documented
// MaxResults contract this backend's static 3-entry catalog can actually
// exercise: values outside "between 5 and 300" are now rejected. Before this
// pass MaxResults was read nowhere in the handler, so an out-of-range value
// was silently accepted and ignored -- confirmed by reverting the handler's
// parseEC2Pagination call and re-running this test, which then fails because
// no error is returned.
func TestDescribeScheduledInstanceAvailability_MaxResultsRangeEnforced(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	// The backend never reads FirstSlotStartTimeRange/Recurrence (confirmed
	// against DescribeScheduledInstanceAvailability's backend implementation,
	// which only consults Filters/MinSlotDurationInHours/MaxSlotDurationInHours),
	// but the generated client validates them client-side before the request
	// is ever sent, so both are populated here purely to clear that gate.
	req := func(maxResults int32) *ec2sdk.DescribeScheduledInstanceAvailabilityInput {
		return &ec2sdk.DescribeScheduledInstanceAvailabilityInput{
			MaxResults:             aws.Int32(maxResults),
			MinSlotDurationInHours: aws.Int32(1),
			MaxSlotDurationInHours: aws.Int32(200),
			FirstSlotStartTimeRange: &types.SlotDateTimeRangeRequest{
				EarliestTime: aws.Time(time.Now()),
				LatestTime:   aws.Time(time.Now().AddDate(0, 1, 0)),
			},
			Recurrence: &types.ScheduledInstanceRecurrenceRequest{
				Frequency: aws.String("Weekly"),
			},
		}
	}

	for _, n := range []int32{4, 301} {
		_, err := client.DescribeScheduledInstanceAvailability(t.Context(), req(n))
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
	}

	out, err := client.DescribeScheduledInstanceAvailability(t.Context(), req(5))
	require.NoError(t, err)
	assert.Len(
		t,
		out.ScheduledInstanceAvailabilitySet,
		3,
		"the static catalog has exactly 3 entries, below MaxResults' own documented floor of 5",
	)
	assert.Nil(t, out.NextToken)
}

// TestGetVpnConnectionDeviceTypes_MaxResultsRangeEnforced covers the same
// shape as DescribeScheduledInstanceAvailability above: this backend's
// static device-type catalog is far below the documented MaxResults floor of
// 200, so truncation itself can't be forced, but out-of-range rejection can.
// Omitting MaxResults must still return every catalog entry, matching
// api_op_GetVpnConnectionDeviceTypes.go's documented "if this parameter is
// not used, then GetVpnConnectionDeviceTypes returns all results".
func TestGetVpnConnectionDeviceTypes_MaxResultsRangeEnforced(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	want := len(b.GetVpnConnectionDeviceTypes())
	require.Less(t, want, 200, "test assumes the static catalog is below MaxResults' documented floor of 200")

	for _, n := range []int32{50, 1001} {
		_, err := client.GetVpnConnectionDeviceTypes(t.Context(), &ec2sdk.GetVpnConnectionDeviceTypesInput{
			MaxResults: aws.Int32(n),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
	}

	out, err := client.GetVpnConnectionDeviceTypes(t.Context(), &ec2sdk.GetVpnConnectionDeviceTypesInput{})
	require.NoError(t, err)
	assert.Len(t, out.VpnConnectionDeviceTypes, want)
	assert.Nil(t, out.NextToken)
}
