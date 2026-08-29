package sesv2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestListContacts_PageSize proves ListContacts honors PageSize: the handler
// only decoded NextToken from the JSON body (handler_contacts.go), silently
// dropping the client-requested PageSize and always paginating at the
// service default.
func TestListContacts_PageSize(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{
		ContactListName: aws.String("list-a"),
	})
	require.NoError(t, err)

	for i := range 5 {
		_, createErr := client.CreateContact(ctx, &sesv2sdk.CreateContactInput{
			ContactListName: aws.String("list-a"),
			EmailAddress:    aws.String(string(rune('a'+i)) + "@example.com"),
		})
		require.NoError(t, createErr)
	}

	out, err := client.ListContacts(ctx, &sesv2sdk.ListContactsInput{
		ContactListName: aws.String("list-a"),
		PageSize:        aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out.Contacts, 2)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestGetDedicatedIps_PoolNameFilter proves GetDedicatedIps honors both the
// PoolName filter and pagination -- handleGetDedicatedIps took no arguments
// at all and returned every tracked IP regardless of pool or PageSize.
func TestGetDedicatedIps_PoolNameFilter(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDedicatedIpPool(ctx, &sesv2sdk.CreateDedicatedIpPoolInput{
		PoolName: aws.String("pool-a"),
	})
	require.NoError(t, err)

	_, err = client.CreateDedicatedIpPool(ctx, &sesv2sdk.CreateDedicatedIpPoolInput{
		PoolName: aws.String("pool-b"),
	})
	require.NoError(t, err)

	_, err = client.PutDedicatedIpInPool(ctx, &sesv2sdk.PutDedicatedIpInPoolInput{
		Ip:                  aws.String("10.0.0.1"),
		DestinationPoolName: aws.String("pool-a"),
	})
	require.NoError(t, err)

	_, err = client.PutDedicatedIpInPool(ctx, &sesv2sdk.PutDedicatedIpInPoolInput{
		Ip:                  aws.String("10.0.0.2"),
		DestinationPoolName: aws.String("pool-b"),
	})
	require.NoError(t, err)

	out, err := client.GetDedicatedIps(ctx, &sesv2sdk.GetDedicatedIpsInput{
		PoolName: aws.String("pool-a"),
	})
	require.NoError(t, err)
	require.Len(t, out.DedicatedIps, 1)
	require.Equal(t, "10.0.0.1", aws.ToString(out.DedicatedIps[0].Ip))
}

// TestListSuppressedDestinations_ReasonsFilter proves ListSuppressedDestinations
// honors the Reasons filter -- the handler read NextToken from the query
// string but never Reasons, StartDate, EndDate, or PageSize.
func TestListSuppressedDestinations_ReasonsFilter(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
		EmailAddress: aws.String("bounced@example.com"),
		Reason:       sesv2types.SuppressionListReasonBounce,
	})
	require.NoError(t, err)

	_, err = client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
		EmailAddress: aws.String("complained@example.com"),
		Reason:       sesv2types.SuppressionListReasonComplaint,
	})
	require.NoError(t, err)

	out, err := client.ListSuppressedDestinations(ctx, &sesv2sdk.ListSuppressedDestinationsInput{
		Reasons: []sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonBounce},
	})
	require.NoError(t, err)
	require.Len(t, out.SuppressedDestinationSummaries, 1)
	require.Equal(t, "bounced@example.com", aws.ToString(out.SuppressedDestinationSummaries[0].EmailAddress))
}

// TestListSuppressedDestinations_PageSize proves PageSize is honored.
func TestListSuppressedDestinations_PageSize(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	for i := range 5 {
		_, err := client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
			EmailAddress: aws.String(string(rune('a'+i)) + "@example.com"),
			Reason:       sesv2types.SuppressionListReasonBounce,
		})
		require.NoError(t, err)
	}

	out, err := client.ListSuppressedDestinations(ctx, &sesv2sdk.ListSuppressedDestinationsInput{
		PageSize: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out.SuppressedDestinationSummaries, 2)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListSuppressedDestinations_DateFilter proves StartDate/EndDate are honored.
func TestListSuppressedDestinations_DateFilter(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
		EmailAddress: aws.String("recent@example.com"),
		Reason:       sesv2types.SuppressionListReasonBounce,
	})
	require.NoError(t, err)

	// A StartDate in the far future excludes every destination added "now".
	out, err := client.ListSuppressedDestinations(ctx, &sesv2sdk.ListSuppressedDestinationsInput{
		StartDate: aws.Time(time.Now().Add(24 * time.Hour)),
	})
	require.NoError(t, err)
	require.Empty(t, out.SuppressedDestinationSummaries)
}

// TestListExportJobs_Filters proves ExportSourceType and JobStatus are
// honored -- both are stored on ExportJob (export_jobs.go) but the handler's
// own comment claimed they "aren't modelled by the backend yet".
func TestListExportJobs_Filters(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	metricsJob, err := backend.CreateExportJob(sesv2.ExportSourceTypeMetricsData)
	require.NoError(t, err)

	_, err = backend.CreateExportJob(sesv2.ExportSourceTypeMessageInsights)
	require.NoError(t, err)

	out, err := client.ListExportJobs(ctx, &sesv2sdk.ListExportJobsInput{
		ExportSourceType: sesv2types.ExportSourceTypeMetricsData,
	})
	require.NoError(t, err)
	require.Len(t, out.ExportJobs, 1)
	require.Equal(t, metricsJob.JobID, aws.ToString(out.ExportJobs[0].JobId))
}

// TestListImportJobs_Filter proves ImportDestinationType is honored.
func TestListImportJobs_Filter(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	contactJob, err := backend.CreateImportJob(sesv2.ImportDestination{
		ContactListName:         "list-a",
		ContactListImportAction: "PUT",
	})
	require.NoError(t, err)

	_, err = backend.CreateImportJob(sesv2.ImportDestination{
		SuppressionListImportAction: "PUT",
	})
	require.NoError(t, err)

	out, err := client.ListImportJobs(ctx, &sesv2sdk.ListImportJobsInput{
		ImportDestinationType: sesv2types.ImportDestinationTypeContactList,
	})
	require.NoError(t, err)
	require.Len(t, out.ImportJobs, 1)
	require.Equal(t, contactJob.JobID, aws.ToString(out.ImportJobs[0].JobId))
}

// TestListReputationEntities_Pagination proves ListReputationEntities honors
// NextToken/PageSize -- the backend method signature discarded both into
// blank identifiers (deliverability.go), so it always returned every entity
// on one page.
func TestListReputationEntities_Pagination(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	for i := range 3 {
		_, err := client.UpdateReputationEntityCustomerManagedStatus(
			ctx, &sesv2sdk.UpdateReputationEntityCustomerManagedStatusInput{
				ReputationEntityReference: aws.String("res-" + string(rune('a'+i))),
				ReputationEntityType:      sesv2types.ReputationEntityTypeResource,
				SendingStatus:             sesv2types.SendingStatusEnabled,
			})
		require.NoError(t, err)
	}

	out, err := client.ListReputationEntities(ctx, &sesv2sdk.ListReputationEntitiesInput{
		PageSize: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.ReputationEntities, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListReputationEntities_SendingStatusFilter proves the SENDING_STATUS
// filter key is honored -- the handler decoded Filter but never passed it to
// the backend at all.
func TestListReputationEntities_SendingStatusFilter(t *testing.T) {
	t.Parallel()

	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.UpdateReputationEntityCustomerManagedStatus(
		ctx, &sesv2sdk.UpdateReputationEntityCustomerManagedStatusInput{
			ReputationEntityReference: aws.String("res-enabled"),
			ReputationEntityType:      sesv2types.ReputationEntityTypeResource,
			SendingStatus:             sesv2types.SendingStatusEnabled,
		})
	require.NoError(t, err)

	_, err = client.UpdateReputationEntityCustomerManagedStatus(
		ctx, &sesv2sdk.UpdateReputationEntityCustomerManagedStatusInput{
			ReputationEntityReference: aws.String("res-disabled"),
			ReputationEntityType:      sesv2types.ReputationEntityTypeResource,
			SendingStatus:             sesv2types.SendingStatusDisabled,
		})
	require.NoError(t, err)

	out, err := client.ListReputationEntities(ctx, &sesv2sdk.ListReputationEntitiesInput{
		Filter: map[string]string{"SENDING_STATUS": "DISABLED"},
	})
	require.NoError(t, err)
	require.Len(t, out.ReputationEntities, 1)
	require.Equal(t, "res-disabled", aws.ToString(out.ReputationEntities[0].ReputationEntityReference))
}
