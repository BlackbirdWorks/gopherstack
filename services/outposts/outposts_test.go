package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

func TestCreateOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)

	out, err := client.CreateOutpost(t.Context(), &outpostssdk.CreateOutpostInput{
		Name:                  aws.String("my-outpost"),
		SiteId:                aws.String(siteID),
		SupportedHardwareType: types.SupportedHardwareTypeRack,
		Description:           aws.String("test outpost"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Outpost)
	require.NotEmpty(t, aws.ToString(out.Outpost.OutpostId))
	require.Contains(t, aws.ToString(out.Outpost.OutpostArn), aws.ToString(out.Outpost.OutpostId))
	require.Equal(t, siteID, aws.ToString(out.Outpost.SiteId))
	require.Equal(t, types.SupportedHardwareTypeRack, out.Outpost.SupportedHardwareType)
	require.NotEmpty(t, aws.ToString(out.Outpost.LifeCycleStatus))
}

func TestCreateOutpost_UnknownSite(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.CreateOutpost(t.Context(), &outpostssdk.CreateOutpostInput{
		Name:   aws.String("my-outpost"),
		SiteId: aws.String("os-does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestGetOutpost_NotFound(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.GetOutpost(t.Context(), &outpostssdk.GetOutpostInput{
		OutpostId: aws.String("op-does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestUpdateOutpost_PartialUpdate(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.UpdateOutpost(t.Context(), &outpostssdk.UpdateOutpostInput{
		OutpostId: created.OutpostId,
		Name:      aws.String("renamed"),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed", aws.ToString(out.Outpost.Name))
	require.Equal(t, created.SupportedHardwareType, out.Outpost.SupportedHardwareType,
		"an unset field must not be overwritten by a partial update")
}

func TestDeleteOutpost(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	_, err := client.DeleteOutpost(t.Context(), &outpostssdk.DeleteOutpostInput{OutpostId: created.OutpostId})
	require.NoError(t, err)

	_, err = client.GetOutpost(t.Context(), &outpostssdk.GetOutpostInput{OutpostId: created.OutpostId})
	require.Error(t, err)
}

func TestListOutposts_FiltersByLifeCycleStatus(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.ListOutposts(t.Context(), &outpostssdk.ListOutpostsInput{
		LifeCycleStatusFilter: []string{aws.ToString(created.LifeCycleStatus)},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Outposts)

	out, err = client.ListOutposts(t.Context(), &outpostssdk.ListOutpostsInput{
		LifeCycleStatusFilter: []string{"NO_SUCH_STATUS"},
	})
	require.NoError(t, err)
	require.Empty(t, out.Outposts)
}

func TestStartOutpostDecommission(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.StartOutpostDecommission(t.Context(), &outpostssdk.StartOutpostDecommissionInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Equal(t, types.DecommissionRequestStatusRequested, out.Status)
	require.Empty(t, out.BlockingResourceTypes)

	// A second call against the same, now-pending-decommission Outpost is
	// skipped (idempotent replay), not re-requested.
	out2, err := client.StartOutpostDecommission(t.Context(), &outpostssdk.StartOutpostDecommissionInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Equal(t, types.DecommissionRequestStatusSkipped, out2.Status)
}

func TestStartOutpostDecommission_ValidateOnlyDoesNotMutate(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.StartOutpostDecommission(t.Context(), &outpostssdk.StartOutpostDecommissionInput{
		OutpostIdentifier: created.OutpostId,
		ValidateOnly:      true,
	})
	require.NoError(t, err)
	require.Equal(t, types.DecommissionRequestStatusRequested, out.Status)

	// Because ValidateOnly did not mutate, a second (real) call should still
	// see REQUESTED, not SKIPPED.
	out2, err := client.StartOutpostDecommission(t.Context(), &outpostssdk.StartOutpostDecommissionInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Equal(t, types.DecommissionRequestStatusRequested, out2.Status)
}

func TestGetOutpostBillingInformation_EmptyUntilRenewalOrOrder(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.GetOutpostBillingInformation(t.Context(), &outpostssdk.GetOutpostBillingInformationInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Empty(t, out.Subscriptions)
}

func TestCreateRenewal_RecordsSubscriptionAndBilling(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	renewOut, err := client.CreateRenewal(t.Context(), &outpostssdk.CreateRenewalInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionAllUpfront,
		PaymentTerm:       types.PaymentTermOneYear,
	})
	require.NoError(t, err)
	require.Equal(t, types.CurrencyCodeUsd, renewOut.Currency)
	require.Positive(t, aws.ToFloat32(renewOut.UpfrontPrice))

	billing, err := client.GetOutpostBillingInformation(t.Context(), &outpostssdk.GetOutpostBillingInformationInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, billing.Subscriptions, 1)
	require.Equal(t, types.SubscriptionTypeRenewal, billing.Subscriptions[0].SubscriptionType)
	require.NotEmpty(t, aws.ToString(billing.ContractEndDate))
}

func TestCreateRenewal_IdempotentClientToken(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	req := &outpostssdk.CreateRenewalInput{
		OutpostIdentifier: created.OutpostId,
		PaymentOption:     types.PaymentOptionNoUpfront,
		PaymentTerm:       types.PaymentTermThreeYears,
		ClientToken:       aws.String("fixed-token"),
	}

	first, err := client.CreateRenewal(t.Context(), req)
	require.NoError(t, err)

	second, err := client.CreateRenewal(t.Context(), req)
	require.NoError(t, err)
	require.InDelta(t, aws.ToFloat32(first.MonthlyRecurringPrice), aws.ToFloat32(second.MonthlyRecurringPrice), 0.001)

	billing, err := client.GetOutpostBillingInformation(t.Context(), &outpostssdk.GetOutpostBillingInformationInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, billing.Subscriptions, 1, "a replayed ClientToken must not record a second subscription")
}

func TestGetRenewalPricing(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	out, err := client.GetRenewalPricing(t.Context(), &outpostssdk.GetRenewalPricingInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Equal(t, types.PricingResultPriced, out.PricingResult)
	require.NotEmpty(t, out.PricingOptions)
}

func TestGetOutpostInstanceTypes_ReflectsCapacityTaskCompletion(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	empty, err := client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
		OutpostId: created.OutpostId,
	})
	require.NoError(t, err)
	require.Empty(t, empty.InstanceTypes)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, assets.Assets, 1, "CreateOutpost must seed exactly one asset")

	assetID := assets.Assets[0].AssetId

	waitForCapacityTaskCompletion(t, client, created.OutpostId, assetID, "m5.xlarge", 4)

	after, err := client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
		OutpostId: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, after.InstanceTypes, 1)
	require.Equal(t, "m5.xlarge", aws.ToString(after.InstanceTypes[0].InstanceType))
	require.Equal(t, int32(4), aws.ToInt32(after.InstanceTypes[0].VCPUs))
}

func TestGetOutpostSupportedInstanceTypes_DiffersFromConfigured(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	configured, err := client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
		OutpostId: created.OutpostId,
	})
	require.NoError(t, err)
	require.Empty(t, configured.InstanceTypes, "nothing is configured on a freshly-created Outpost")

	supported, err := client.GetOutpostSupportedInstanceTypes(
		t.Context(),
		&outpostssdk.GetOutpostSupportedInstanceTypesInput{
			OutpostIdentifier: created.OutpostId,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, supported.InstanceTypes,
		"supported instance types must not be empty even when nothing is yet configured")
}
