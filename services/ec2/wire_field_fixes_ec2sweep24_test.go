package ec2_test

// Fixes for handler_spot_fleet.go and handler_elastic_ips.go (ec2sweep24):
//
//   - RequestSpotFleet dropped TagSpecification.N entirely (accept-and-drop):
//     real SpotFleetRequestConfigData.TagSpecifications (ec2@v1.319.1
//     deserializers.go:159297, "TagSpecification") was parsed into config but
//     never applied via Backend.CreateTags, so a fleet tagged at creation came
//     back untagged from DescribeSpotFleetRequests.
//   - CancelSpotFleetRequests rendered <error> as a bare string. The real
//     deserializer (awsEc2query_deserializeDocumentCancelSpotFleetRequestsError,
//     deserializers.go:81882) reads <error> as a nested <code>/<message>
//     structure; a scalar value has no child elements for it to find, so the
//     error code was silently dropped. The backend's own error value also
//     didn't match the real CancelBatchErrorCode enum.
//   - DescribeAddresses accepted-and-dropped the PublicIp.N request member
//     (serializers.go:76230, FlatKey "PublicIp") -- a client filtering by
//     public IP got every address back instead.
//   - addressTransferDetailItem (shared by Enable/DisableAddressTransfer and
//     DescribeAddressTransfers) used wire tags "transferOfferStatus" and
//     "transferOfferExpiry" -- different element NAMEs than the real
//     AddressTransfer deserializer's "addressTransferStatus" and
//     "transferOfferExpirationTimestamp" (deserializers.go:75605), so those two
//     fields were always zero-valued on a real client despite the server
//     computing real data for both.
//   - DescribeMovingAddresses accepted-and-dropped its "moving-status" Filter.
//   - Seven List ops across both files declare real MaxResults/NextToken on
//     their SDK Input/Output but the handlers returned every item in one
//     unbounded page: DescribeSpotFleetRequests, DescribeSpotFleetInstances,
//     DescribeSpotFleetRequestHistory, GetSpotPlacementScores,
//     DescribeAddressesAttribute, DescribeAddressTransfers,
//     DescribeMovingAddresses.

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ec2sweep24SeedCount = 5

func TestRequestSpotFleet_TagSpecification_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	req, err := client.RequestSpotFleet(t.Context(), &ec2sdk.RequestSpotFleetInput{
		SpotFleetRequestConfig: &types.SpotFleetRequestConfigData{
			IamFleetRole:   aws.String("arn:aws:iam::000000000000:role/fleet-role"),
			TargetCapacity: aws.Int32(1),
			LaunchSpecifications: []types.SpotFleetLaunchSpecification{{
				ImageId:      aws.String("ami-fleet0002"),
				InstanceType: types.InstanceTypeM5Large,
			}},
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeSpotFleetRequest,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("sweep24")}},
			}},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeSpotFleetRequests(t.Context(), &ec2sdk.DescribeSpotFleetRequestsInput{
		SpotFleetRequestIds: []string{aws.ToString(req.SpotFleetRequestId)},
	})
	require.NoError(t, err)
	require.Len(t, out.SpotFleetRequestConfigs, 1)
	require.NotEmpty(
		t, out.SpotFleetRequestConfigs[0].Tags,
		"Tags empty - TagSpecification.N accepted at RequestSpotFleet but never applied",
	)
	assert.Equal(t, "Team", aws.ToString(out.SpotFleetRequestConfigs[0].Tags[0].Key))
	assert.Equal(t, "sweep24", aws.ToString(out.SpotFleetRequestConfigs[0].Tags[0].Value))
}

func TestCancelSpotFleetRequests_ErrorCode_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.CancelSpotFleetRequests(t.Context(), &ec2sdk.CancelSpotFleetRequestsInput{
		SpotFleetRequestIds: []string{"sfr-does-not-exist"},
		TerminateInstances:  aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.UnsuccessfulFleetRequests, 1)

	item := out.UnsuccessfulFleetRequests[0]
	require.NotNil(t, item.Error, "Error nil - CancelSpotFleetRequestsErrorItem.Error is a struct, not a string")
	assert.Equal(
		t, types.CancelBatchErrorCodeFleetRequestIdDoesNotExist, item.Error.Code,
		"Code empty - a bare <error>text</error> has no <code> child for the real deserializer to find",
	)
}

func TestDescribeSpotFleetRequests_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for i := range ec2sweep24SeedCount {
		_, err := client.RequestSpotFleet(t.Context(), &ec2sdk.RequestSpotFleetInput{
			SpotFleetRequestConfig: &types.SpotFleetRequestConfigData{
				IamFleetRole:   aws.String("arn:aws:iam::000000000000:role/fleet-role"),
				TargetCapacity: aws.Int32(0),
				LaunchSpecifications: []types.SpotFleetLaunchSpecification{{
					ImageId:      aws.String("ami-sweep24-" + strconv.Itoa(i)),
					InstanceType: types.InstanceTypeM5Large,
				}},
			},
		})
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeSpotFleetRequestsPaginator(
		client, &ec2sdk.DescribeSpotFleetRequestsInput{},
		func(o *ec2sdk.DescribeSpotFleetRequestsPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.SpotFleetRequestConfigs))
		for _, cfg := range p.SpotFleetRequestConfigs {
			ids = append(ids, aws.ToString(cfg.SpotFleetRequestId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep24SeedCount)
}

func TestDescribeSpotFleetInstances_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	req, err := client.RequestSpotFleet(t.Context(), &ec2sdk.RequestSpotFleetInput{
		SpotFleetRequestConfig: &types.SpotFleetRequestConfigData{
			IamFleetRole:   aws.String("arn:aws:iam::000000000000:role/fleet-role"),
			TargetCapacity: aws.Int32(ec2sweep24SeedCount),
			LaunchSpecifications: []types.SpotFleetLaunchSpecification{{
				ImageId:      aws.String("ami-sweep24-instances"),
				InstanceType: types.InstanceTypeM5Large,
			}},
		},
	})
	require.NoError(t, err)
	fleetID := req.SpotFleetRequestId

	var (
		pages     [][]string
		nextToken *string
	)

	for range ec2sweep11LoopGuard {
		out, pageErr := client.DescribeSpotFleetInstances(t.Context(), &ec2sdk.DescribeSpotFleetInstancesInput{
			SpotFleetRequestId: fleetID,
			MaxResults:         aws.Int32(ec2sweep11MaxResults),
			NextToken:          nextToken,
		})
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.ActiveInstances))
		for _, inst := range out.ActiveInstances {
			ids = append(ids, aws.ToString(inst.InstanceId))
		}
		pages = append(pages, ids)

		if aws.ToString(out.NextToken) == "" {
			break
		}
		nextToken = out.NextToken
	}

	assertDisjointPages(t, pages, ec2sweep24SeedCount)
}

func TestDescribeSpotFleetRequestHistory_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	req, err := client.RequestSpotFleet(t.Context(), &ec2sdk.RequestSpotFleetInput{
		SpotFleetRequestConfig: &types.SpotFleetRequestConfigData{
			IamFleetRole:   aws.String("arn:aws:iam::000000000000:role/fleet-role"),
			TargetCapacity: aws.Int32(1),
			LaunchSpecifications: []types.SpotFleetLaunchSpecification{{
				ImageId:      aws.String("ami-sweep24-history"),
				InstanceType: types.InstanceTypeM5Large,
			}},
		},
	})
	require.NoError(t, err)
	fleetID := req.SpotFleetRequestId

	// One history record exists from creation; five more from Modify calls
	// below, each with a distinct target capacity so EventInformation strings
	// are unique.
	for _, tc := range []int32{2, 3, 4, 5, 6} {
		_, modErr := client.ModifySpotFleetRequest(t.Context(), &ec2sdk.ModifySpotFleetRequestInput{
			SpotFleetRequestId: fleetID,
			TargetCapacity:     aws.Int32(tc),
		})
		require.NoError(t, modErr)
	}

	var (
		pages     [][]string
		nextToken *string
	)

	for range ec2sweep11LoopGuard {
		out, pageErr := client.DescribeSpotFleetRequestHistory(
			t.Context(), &ec2sdk.DescribeSpotFleetRequestHistoryInput{
				SpotFleetRequestId: fleetID,
				StartTime:          aws.Time(time.Now().Add(-time.Hour)),
				MaxResults:         aws.Int32(ec2sweep11MaxResults),
				NextToken:          nextToken,
			},
		)
		require.NoError(t, pageErr)

		infos := make([]string, 0, len(out.HistoryRecords))
		for _, rec := range out.HistoryRecords {
			require.NotNil(t, rec.EventInformation, "EventInformation nil - real deserializer nests it, not a scalar")
			infos = append(infos, aws.ToString(rec.EventInformation.EventDescription))
		}
		pages = append(pages, infos)

		if aws.ToString(out.NextToken) == "" {
			assert.NotNil(t, out.LastEvaluatedTime, "LastEvaluatedTime nil on final page")

			break
		}
		nextToken = out.NextToken
	}

	assertDisjointPages(t, pages, 1+5)
}

func TestGetSpotPlacementScores_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	paginator := ec2sdk.NewGetSpotPlacementScoresPaginator(
		client, &ec2sdk.GetSpotPlacementScoresInput{
			InstanceTypes:  []string{"m5.large"},
			TargetCapacity: aws.Int32(10),
		},
		func(o *ec2sdk.GetSpotPlacementScoresPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.SpotPlacementScores))
		for _, s := range p.SpotPlacementScores {
			ids = append(ids, aws.ToString(s.Region))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, 10)
}

func TestDescribeAddresses_PublicIpFilter_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	var wantIP string
	for range 3 {
		alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
		require.NoError(t, err)
		wantIP = aws.ToString(alloc.PublicIp)
	}

	out, err := client.DescribeAddresses(t.Context(), &ec2sdk.DescribeAddressesInput{
		PublicIps: []string{wantIP},
	})
	require.NoError(t, err)
	require.Len(
		t, out.Addresses, 1,
		"PublicIp.N accepted-and-dropped - server returned every address instead of just the requested one",
	)
	assert.Equal(t, wantIP, aws.ToString(out.Addresses[0].PublicIp))
}

func TestEnableAddressTransfer_StatusAndExpiry_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	out, err := client.EnableAddressTransfer(t.Context(), &ec2sdk.EnableAddressTransferInput{
		AllocationId:      alloc.AllocationId,
		TransferAccountId: aws.String("111111111111"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.AddressTransfer)

	assert.Equal(
		t, types.AddressTransferStatusPending, out.AddressTransfer.AddressTransferStatus,
		"AddressTransferStatus empty - wire tag was \"transferOfferStatus\", "+
			"a different element name than the real \"addressTransferStatus\"",
	)
	assert.NotNil(
		t, out.AddressTransfer.TransferOfferExpirationTimestamp,
		"TransferOfferExpirationTimestamp nil - wire tag was \"transferOfferExpiry\", "+
			"a different element name than the real \"transferOfferExpirationTimestamp\"",
	)
}

func TestDescribeAddressesAttribute_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for range ec2sweep24SeedCount {
		_, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeAddressesAttributePaginator(
		client, &ec2sdk.DescribeAddressesAttributeInput{},
		func(o *ec2sdk.DescribeAddressesAttributePaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.Addresses))
		for _, a := range p.Addresses {
			ids = append(ids, aws.ToString(a.AllocationId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep24SeedCount)
}

func TestDescribeAddressTransfers_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for i := range ec2sweep24SeedCount {
		alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
		require.NoError(t, err)

		_, err = client.EnableAddressTransfer(t.Context(), &ec2sdk.EnableAddressTransferInput{
			AllocationId:      alloc.AllocationId,
			TransferAccountId: aws.String("11111111111" + strconv.Itoa(i)),
		})
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeAddressTransfersPaginator(
		client, &ec2sdk.DescribeAddressTransfersInput{},
		func(o *ec2sdk.DescribeAddressTransfersPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.AddressTransfers))
		for _, tr := range p.AddressTransfers {
			ids = append(ids, aws.ToString(tr.AllocationId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep24SeedCount)
}

func TestDescribeMovingAddresses_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	// DescribeMovingAddresses documents "between 5 and 1000" (unlike the other
	// list ops in this file, which fall back to the 1..1000 default), so it
	// needs its own MaxResults floor and a seed count above that floor.
	const (
		movingAddressesMinResults = 5
		movingAddressesSeedCount  = 7
	)

	_, client := newTestBackendAndClient(t)

	for range movingAddressesSeedCount {
		alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
		require.NoError(t, err)

		_, err = client.MoveAddressToVpc(t.Context(), &ec2sdk.MoveAddressToVpcInput{
			PublicIp: alloc.PublicIp,
		})
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeMovingAddressesPaginator(
		client, &ec2sdk.DescribeMovingAddressesInput{},
		func(o *ec2sdk.DescribeMovingAddressesPaginatorOptions) { o.Limit = movingAddressesMinResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.MovingAddressStatuses))
		for _, s := range p.MovingAddressStatuses {
			ids = append(ids, aws.ToString(s.PublicIp))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, movingAddressesSeedCount)
}

func TestDescribeMovingAddresses_MovingStatusFilter_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	_, err = client.MoveAddressToVpc(t.Context(), &ec2sdk.MoveAddressToVpcInput{PublicIp: alloc.PublicIp})
	require.NoError(t, err)

	out, err := client.DescribeMovingAddresses(t.Context(), &ec2sdk.DescribeMovingAddressesInput{
		Filters: []types.Filter{{
			Name:   aws.String("moving-status"),
			Values: []string{"restoringToClassic"},
		}},
	})
	require.NoError(t, err)
	assert.Empty(
		t, out.MovingAddressStatuses,
		"moving-status filter accepted-and-dropped - a non-matching value still returned every entry",
	)
}
