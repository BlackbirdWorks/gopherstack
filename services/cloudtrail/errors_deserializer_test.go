package cloudtrail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// These tests prove that this package's not-found/already-exists error
// paths return a code the real SDK client can type via errors.As, not just
// a matching HTTP status (gopherstack-wlo1). Before the fix each of these
// used a generic sentinel mapped to a code absent from that op's own
// deserializeOpError<Op> switch (aws-sdk-go-v2/service/cloudtrail@v1.58.4
// deserializers.go), so the real client only ever got a
// smithy.GenericAPIError.

func TestAddTags_UnknownResourceSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.AddTags(t.Context(), &cloudtrailsdk.AddTagsInput{
		ResourceId: aws.String("arn:aws:cloudtrail:us-east-1:000000000000:trail/does-not-exist"),
		TagsList:   []cttypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.Error(t, err)

	var target *cttypes.ResourceNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"AddTags on an unknown resource must type ResourceNotFoundException",
	)
}

func TestRemoveTags_UnknownResourceSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.RemoveTags(t.Context(), &cloudtrailsdk.RemoveTagsInput{
		ResourceId: aws.String("arn:aws:cloudtrail:us-east-1:000000000000:trail/does-not-exist"),
		TagsList:   []cttypes.Tag{{Key: aws.String("k")}},
	})
	require.Error(t, err)

	var target *cttypes.ResourceNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"RemoveTags on an unknown resource must type ResourceNotFoundException",
	)
}

func TestGetImport_UnknownIDSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.GetImport(t.Context(), &cloudtrailsdk.GetImportInput{
		ImportId: aws.String("import-999999"),
	})
	require.Error(t, err)

	var target *cttypes.ImportNotFoundException
	require.ErrorAs(t, err, &target, "GetImport on an unknown ID must type ImportNotFoundException")
}

func TestStopImport_UnknownIDSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.StopImport(t.Context(), &cloudtrailsdk.StopImportInput{
		ImportId: aws.String("import-999999"),
	})
	require.Error(t, err)

	var target *cttypes.ImportNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"StopImport on an unknown ID must type ImportNotFoundException",
	)
}

func TestGetResourcePolicy_NoneAttachedSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.GetResourcePolicy(t.Context(), &cloudtrailsdk.GetResourcePolicyInput{
		ResourceArn: aws.String("arn:aws:cloudtrail:us-east-1:000000000000:trail/no-policy"),
	})
	require.Error(t, err)

	var target *cttypes.ResourcePolicyNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"GetResourcePolicy with no policy attached must type ResourcePolicyNotFoundException",
	)
}

func TestDeleteResourcePolicy_NoneAttachedSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.DeleteResourcePolicy(t.Context(), &cloudtrailsdk.DeleteResourcePolicyInput{
		ResourceArn: aws.String("arn:aws:cloudtrail:us-east-1:000000000000:trail/no-policy"),
	})
	require.Error(t, err)

	var target *cttypes.ResourcePolicyNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"DeleteResourcePolicy with no policy attached must type ResourcePolicyNotFoundException",
	)
}

func TestGetDashboard_UnknownIDSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.GetDashboard(t.Context(), &cloudtrailsdk.GetDashboardInput{
		DashboardId: aws.String("dashboard-999999"),
	})
	require.Error(t, err)

	var target *cttypes.ResourceNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"GetDashboard on an unknown ID must type ResourceNotFoundException",
	)
}

func TestDeleteDashboard_UnknownIDSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.DeleteDashboard(t.Context(), &cloudtrailsdk.DeleteDashboardInput{
		DashboardId: aws.String("dashboard-999999"),
	})
	require.Error(t, err)

	var target *cttypes.ResourceNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"DeleteDashboard on an unknown ID must type ResourceNotFoundException",
	)
}

func TestStartDashboardRefresh_UnknownIDSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.StartDashboardRefresh(t.Context(), &cloudtrailsdk.StartDashboardRefreshInput{
		DashboardId: aws.String("dashboard-999999"),
	})
	require.Error(t, err)

	var target *cttypes.ResourceNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"StartDashboardRefresh on an unknown ID must type ResourceNotFoundException",
	)
}

func TestCreateChannel_DuplicateNameSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	input := &cloudtrailsdk.CreateChannelInput{
		Name:   aws.String("dup-channel"),
		Source: aws.String("Custom"),
		Destinations: []cttypes.Destination{
			{
				Type: cttypes.DestinationTypeEventDataStore,
				Location: aws.String(
					"arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/eds-1",
				),
			},
		},
	}
	_, err := client.CreateChannel(t.Context(), input)
	require.NoError(t, err)

	_, err = client.CreateChannel(t.Context(), input)
	require.Error(t, err)

	var target *cttypes.ChannelAlreadyExistsException
	require.ErrorAs(
		t,
		err,
		&target,
		"CreateChannel on a duplicate name must type ChannelAlreadyExistsException",
	)
}

func TestCreateEventDataStore_DuplicateNameSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	input := &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("dup-eds"),
	}
	_, err := client.CreateEventDataStore(t.Context(), input)
	require.NoError(t, err)

	_, err = client.CreateEventDataStore(t.Context(), input)
	require.Error(t, err)

	var target *cttypes.EventDataStoreAlreadyExistsException
	require.ErrorAs(
		t,
		err,
		&target,
		"CreateEventDataStore on a duplicate name must type EventDataStoreAlreadyExistsException",
	)
}

func TestCreateDashboard_DuplicateNameSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	input := &cloudtrailsdk.CreateDashboardInput{
		Name: aws.String("dup-dashboard"),
	}
	_, err := client.CreateDashboard(t.Context(), input)
	require.NoError(t, err)

	_, err = client.CreateDashboard(t.Context(), input)
	require.Error(t, err)

	var target *cttypes.ConflictException
	require.ErrorAs(
		t,
		err,
		&target,
		"CreateDashboard on a duplicate name must type ConflictException",
	)
}

func TestGetInsightSelectors_UnknownEventDataStoreSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.GetInsightSelectors(t.Context(), &cloudtrailsdk.GetInsightSelectorsInput{
		EventDataStore: aws.String(
			"arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/does-not-exist",
		),
	})
	require.Error(t, err)

	// GetInsightSelectors' own deserializeOpError switch has no
	// EventDataStoreNotFoundException case -- only TrailNotFoundException.
	var target *cttypes.TrailNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"GetInsightSelectors on an unknown EventDataStore must type TrailNotFoundException",
	)
}

func TestPutInsightSelectors_UnknownEventDataStoreSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.PutInsightSelectors(t.Context(), &cloudtrailsdk.PutInsightSelectorsInput{
		EventDataStore: aws.String(
			"arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/does-not-exist",
		),
		InsightSelectors: []cttypes.InsightSelector{
			{InsightType: cttypes.InsightTypeApiCallRateInsight},
		},
	})
	require.Error(t, err)

	// PutInsightSelectors' own deserializeOpError switch has no
	// EventDataStoreNotFoundException case -- only TrailNotFoundException.
	var target *cttypes.TrailNotFoundException
	require.ErrorAs(
		t,
		err,
		&target,
		"PutInsightSelectors on an unknown EventDataStore must type TrailNotFoundException",
	)
}
