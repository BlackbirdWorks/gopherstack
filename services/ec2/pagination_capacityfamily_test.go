package ec2_test

// Pagination fixes for ten of the ops registered by registerCapacityFamilyOps
// (handler_capacity_family.go): each defines MaxResults/NextToken on its real
// SDK input but the handler ignored both, always returning every item in one
// page with no NextToken. Found while auditing the 30 registerCapacityFamilyOps
// ops living outside handler_capacity_reservations.go (gopherstack-6cuc
// follow-up pass).
//
// DescribeCapacityBlockExtensionOfferings was audited and left unfixed: this
// backend always generates exactly one offering per call, so a pagination fix
// there is unprovable (paginating over one item can't fail a test) -- see
// PARITY.md.

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"
)

const (
	capFamilySeedCount  = 3
	capFamilyMaxResults = 1
	capFamilyLoopGuard  = 10
)

func TestDescribeCapacityReservationFleets_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for i := range capFamilySeedCount {
		_, err := client.CreateCapacityReservationFleet(
			t.Context(), &ec2sdk.CreateCapacityReservationFleetInput{
				TotalTargetCapacity: aws.Int32(1),
				InstanceTypeSpecifications: []types.ReservationFleetInstanceSpecification{
					{
						AvailabilityZone: aws.String(fmt.Sprintf("us-east-1%c", 'a'+i)),
						InstanceType:     types.InstanceTypeM5Large,
						InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
					},
				},
			},
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeCapacityReservationFleetsPaginator(
		client, &ec2sdk.DescribeCapacityReservationFleetsInput{},
		func(o *ec2sdk.DescribeCapacityReservationFleetsPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityReservationFleets))
		for _, f := range out.CapacityReservationFleets {
			ids = append(ids, aws.ToString(f.CapacityReservationFleetId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestDescribeCapacityBlockOfferings_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	page1, err := client.DescribeCapacityBlockOfferings(
		t.Context(), &ec2sdk.DescribeCapacityBlockOfferingsInput{
			InstanceType:          aws.String("m5.large"),
			CapacityDurationHours: aws.Int32(24),
			InstanceCount:         aws.Int32(1),
			MaxResults:            aws.Int32(capFamilyMaxResults),
		},
	)
	require.NoError(t, err)
	require.Lenf(t, page1.CapacityBlockOfferings, capFamilyMaxResults,
		"MaxResults=%d ignored - all offerings returned on page one", capFamilyMaxResults)
	require.NotEmpty(t, page1.NextToken, "NextToken empty despite more offerings remaining")
}

func TestDescribeCapacityBlocks_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for range capFamilySeedCount {
		purchaseCapacityBlock(t, client)
	}

	paginator := ec2sdk.NewDescribeCapacityBlocksPaginator(
		client, &ec2sdk.DescribeCapacityBlocksInput{},
		func(o *ec2sdk.DescribeCapacityBlocksPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityBlocks))
		for _, b := range out.CapacityBlocks {
			ids = append(ids, aws.ToString(b.CapacityBlockId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestDescribeCapacityBlockStatus_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	wantIDs := make([]string, 0, capFamilySeedCount)
	for range capFamilySeedCount {
		block := purchaseCapacityBlock(t, client)
		wantIDs = append(wantIDs, aws.ToString(block.CapacityBlockId))
	}

	paginator := ec2sdk.NewDescribeCapacityBlockStatusPaginator(
		client, &ec2sdk.DescribeCapacityBlockStatusInput{},
		func(o *ec2sdk.DescribeCapacityBlockStatusPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityBlockStatuses))
		for _, s := range out.CapacityBlockStatuses {
			ids = append(ids, aws.ToString(s.CapacityBlockId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, len(wantIDs))
}

func TestDescribeCapacityBlockExtensionHistory_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for range capFamilySeedCount {
		block := purchaseCapacityBlock(t, client)
		reservationID := block.CapacityReservationIds[0]

		extOfferings, err := client.DescribeCapacityBlockExtensionOfferings(
			t.Context(), &ec2sdk.DescribeCapacityBlockExtensionOfferingsInput{
				CapacityReservationId:               aws.String(reservationID),
				CapacityBlockExtensionDurationHours: aws.Int32(24),
			},
		)
		require.NoError(t, err)
		require.NotEmpty(t, extOfferings.CapacityBlockExtensionOfferings)

		_, err = client.PurchaseCapacityBlockExtension(
			t.Context(), &ec2sdk.PurchaseCapacityBlockExtensionInput{
				CapacityBlockExtensionOfferingId: extOfferings.CapacityBlockExtensionOfferings[0].CapacityBlockExtensionOfferingId,
				CapacityReservationId:            aws.String(reservationID),
			},
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeCapacityBlockExtensionHistoryPaginator(
		client, &ec2sdk.DescribeCapacityBlockExtensionHistoryInput{},
		func(o *ec2sdk.DescribeCapacityBlockExtensionHistoryPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityBlockExtensions))
		for _, e := range out.CapacityBlockExtensions {
			ids = append(ids, aws.ToString(e.CapacityBlockExtensionOfferingId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestDescribeCapacityReservationBillingRequests_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for i := range capFamilySeedCount {
		cr, err := client.CreateCapacityReservation(
			t.Context(), &ec2sdk.CreateCapacityReservationInput{
				InstanceType:     aws.String("m5.large"),
				InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
				AvailabilityZone: aws.String("us-east-1a"),
				InstanceCount:    aws.Int32(1),
			},
		)
		require.NoError(t, err)

		_, err = client.AssociateCapacityReservationBillingOwner(
			t.Context(), &ec2sdk.AssociateCapacityReservationBillingOwnerInput{
				CapacityReservationId:           cr.CapacityReservation.CapacityReservationId,
				UnusedReservationBillingOwnerId: aws.String(fmt.Sprintf("11111111111%d", i)),
			},
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeCapacityReservationBillingRequestsPaginator(
		client, &ec2sdk.DescribeCapacityReservationBillingRequestsInput{
			Role: types.CallerRoleOdcrOwner,
		},
		func(o *ec2sdk.DescribeCapacityReservationBillingRequestsPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityReservationBillingRequests))
		for _, r := range out.CapacityReservationBillingRequests {
			ids = append(ids, aws.ToString(r.CapacityReservationId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestDescribeCapacityManagerDataExports_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for i := range capFamilySeedCount {
		_, err := client.CreateCapacityManagerDataExport(
			t.Context(), &ec2sdk.CreateCapacityManagerDataExportInput{
				OutputFormat: types.OutputFormatCsv,
				S3BucketName: aws.String(fmt.Sprintf("capmgr-export-bucket-%d", i)),
				Schedule:     types.ScheduleHourly,
			},
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeCapacityManagerDataExportsPaginator(
		client, &ec2sdk.DescribeCapacityManagerDataExportsInput{},
		func(o *ec2sdk.DescribeCapacityManagerDataExportsPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityManagerDataExports))
		for _, e := range out.CapacityManagerDataExports {
			ids = append(ids, aws.ToString(e.CapacityManagerDataExportId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestGetCapacityManagerMonitoredTagKeys_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	activate := make([]string, 0, capFamilySeedCount)
	for i := range capFamilySeedCount {
		activate = append(activate, fmt.Sprintf("team-%d", i))
	}

	_, updateErr := client.UpdateCapacityManagerMonitoredTagKeys(
		t.Context(), &ec2sdk.UpdateCapacityManagerMonitoredTagKeysInput{ActivateTagKeys: activate},
	)
	require.NoError(t, updateErr)

	paginator := ec2sdk.NewGetCapacityManagerMonitoredTagKeysPaginator(
		client, &ec2sdk.GetCapacityManagerMonitoredTagKeysInput{},
		func(o *ec2sdk.GetCapacityManagerMonitoredTagKeysPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityManagerTagKeys))
		for _, k := range out.CapacityManagerTagKeys {
			ids = append(ids, aws.ToString(k.TagKey))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestDescribeCapacityReservations_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	for range capFamilySeedCount {
		_, err := client.CreateCapacityReservation(
			t.Context(), &ec2sdk.CreateCapacityReservationInput{
				InstanceType:     aws.String("m5.large"),
				InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
				AvailabilityZone: aws.String("us-east-1a"),
				InstanceCount:    aws.Int32(1),
			},
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeCapacityReservationsPaginator(
		client, &ec2sdk.DescribeCapacityReservationsInput{},
		func(o *ec2sdk.DescribeCapacityReservationsPaginatorOptions) {
			o.Limit = capFamilyMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, capFamilyLoopGuard, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(out.CapacityReservations))
		for _, cr := range out.CapacityReservations {
			ids = append(ids, aws.ToString(cr.CapacityReservationId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, capFamilySeedCount)
}

func TestDescribeCapacityReservationCancellationQuotes_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	quoteIDs := make([]string, 0, capFamilySeedCount)
	for range capFamilySeedCount {
		cr, err := client.CreateCapacityReservation(
			t.Context(), &ec2sdk.CreateCapacityReservationInput{
				InstanceType:     aws.String("m5.large"),
				InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
				AvailabilityZone: aws.String("us-east-1a"),
				InstanceCount:    aws.Int32(1),
			},
		)
		require.NoError(t, err)

		quote, err := client.CreateCapacityReservationCancellationQuote(
			t.Context(), &ec2sdk.CreateCapacityReservationCancellationQuoteInput{
				CapacityReservationId: cr.CapacityReservation.CapacityReservationId,
			},
		)
		require.NoError(t, err)
		quoteIDs = append(
			quoteIDs, aws.ToString(quote.CapacityReservationCancellationQuote.CapacityReservationCancellationQuoteId),
		)
	}

	seen := make(map[string]bool, capFamilySeedCount)

	nextToken := (*string)(nil)
	for page := range capFamilySeedCount + 1 {
		out, err := client.DescribeCapacityReservationCancellationQuotes(
			t.Context(), &ec2sdk.DescribeCapacityReservationCancellationQuotesInput{
				MaxResults: aws.Int32(capFamilyMaxResults),
				NextToken:  nextToken,
			},
		)
		require.NoError(t, err)

		if page == 0 {
			require.Lenf(t, out.CapacityReservationCancellationQuotes, capFamilyMaxResults,
				"MaxResults=%d ignored - all quotes returned on page one", capFamilyMaxResults)
		}

		for _, q := range out.CapacityReservationCancellationQuotes {
			id := aws.ToString(q.CapacityReservationCancellationQuoteId)
			require.Falsef(t, seen[id], "quote %q returned on more than one page", id)
			seen[id] = true
		}

		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	require.ElementsMatch(t, quoteIDs, mapKeys(seen))
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

// purchaseCapacityBlock describes and purchases one Capacity Block, returning
// the purchased block (with its backing Capacity Reservation ID).
func purchaseCapacityBlock(t *testing.T, client *ec2sdk.Client) types.CapacityBlock {
	t.Helper()

	offerings, err := client.DescribeCapacityBlockOfferings(
		t.Context(), &ec2sdk.DescribeCapacityBlockOfferingsInput{
			InstanceType:          aws.String("m5.large"),
			CapacityDurationHours: aws.Int32(24),
			InstanceCount:         aws.Int32(1),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, offerings.CapacityBlockOfferings)

	purchase, err := client.PurchaseCapacityBlock(
		t.Context(), &ec2sdk.PurchaseCapacityBlockInput{
			CapacityBlockOfferingId: offerings.CapacityBlockOfferings[0].CapacityBlockOfferingId,
			InstancePlatform:        types.CapacityReservationInstancePlatformLinuxUnix,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, purchase.CapacityBlocks)

	return purchase.CapacityBlocks[0]
}
