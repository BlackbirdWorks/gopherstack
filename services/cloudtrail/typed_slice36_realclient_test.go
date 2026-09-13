package cloudtrail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestChannel_TypedLifecycle drives CreateChannel, GetChannel, UpdateChannel,
// ListChannels and DeleteChannel through a real SDK client.
func TestChannel_TypedLifecycle(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	createOut, err := client.CreateChannel(t.Context(), &cloudtrailsdk.CreateChannelInput{
		Name:   aws.String("my-channel"),
		Source: aws.String("Custom"),
		Destinations: []cttypes.Destination{
			{Type: cttypes.DestinationTypeAwsService, Location: aws.String("aws-service")},
		},
		Tags: []cttypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)
	channelArn := aws.ToString(createOut.ChannelArn)
	require.NotEmpty(t, channelArn)
	require.Len(t, createOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(createOut.Tags[0].Key))

	getOut, err := client.GetChannel(t.Context(), &cloudtrailsdk.GetChannelInput{
		Channel: aws.String(channelArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-channel", aws.ToString(getOut.Name))
	assert.Equal(t, "Custom", aws.ToString(getOut.Source))
	require.Len(t, getOut.Destinations, 1)

	updateOut, err := client.UpdateChannel(t.Context(), &cloudtrailsdk.UpdateChannelInput{
		Channel: aws.String(channelArn),
		Name:    aws.String("my-channel-renamed"),
		Destinations: []cttypes.Destination{
			{Type: cttypes.DestinationTypeAwsService, Location: aws.String("aws-service-2")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-channel-renamed", aws.ToString(updateOut.Name))

	listOut, err := client.ListChannels(t.Context(), &cloudtrailsdk.ListChannelsInput{})
	require.NoError(t, err)
	var found bool
	for _, ch := range listOut.Channels {
		if aws.ToString(ch.ChannelArn) == channelArn {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.DeleteChannel(t.Context(), &cloudtrailsdk.DeleteChannelInput{
		Channel: aws.String(channelArn),
	})
	require.NoError(t, err)

	_, err = client.GetChannel(t.Context(), &cloudtrailsdk.GetChannelInput{
		Channel: aws.String(channelArn),
	})
	require.Error(t, err)
}

// TestEventDataStore_TypedLifecycle drives CreateEventDataStore,
// GetEventDataStore, UpdateEventDataStore, ListEventDataStores,
// EnableFederation, DisableFederation, StopEventDataStoreIngestion,
// StartEventDataStoreIngestion, RestoreEventDataStore and
// DeleteEventDataStore through a real SDK client.
func TestEventDataStore_TypedLifecycle(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	createOut, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("my-eds"),
	})
	require.NoError(t, err)
	edsArn := aws.ToString(createOut.EventDataStoreArn)
	require.NotEmpty(t, edsArn)

	getOut, err := client.GetEventDataStore(t.Context(), &cloudtrailsdk.GetEventDataStoreInput{
		EventDataStore: aws.String(edsArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-eds", aws.ToString(getOut.Name))

	updateOut, err := client.UpdateEventDataStore(t.Context(), &cloudtrailsdk.UpdateEventDataStoreInput{
		EventDataStore:  aws.String(edsArn),
		RetentionPeriod: aws.Int32(120),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(120), aws.ToInt32(updateOut.RetentionPeriod))

	listOut, err := client.ListEventDataStores(t.Context(), &cloudtrailsdk.ListEventDataStoresInput{})
	require.NoError(t, err)
	var found bool
	for _, eds := range listOut.EventDataStores {
		if aws.ToString(eds.EventDataStoreArn) == edsArn {
			found = true
		}
	}
	assert.True(t, found)

	enableOut, err := client.EnableFederation(t.Context(), &cloudtrailsdk.EnableFederationInput{
		EventDataStore:    aws.String(edsArn),
		FederationRoleArn: aws.String("arn:aws:iam::123456789012:role/federation-role"),
	})
	require.NoError(t, err)
	assert.Equal(t, cttypes.FederationStatusEnabled, enableOut.FederationStatus)

	disableOut, err := client.DisableFederation(t.Context(), &cloudtrailsdk.DisableFederationInput{
		EventDataStore: aws.String(edsArn),
	})
	require.NoError(t, err)
	assert.Equal(t, cttypes.FederationStatusDisabled, disableOut.FederationStatus)

	_, err = client.StopEventDataStoreIngestion(t.Context(), &cloudtrailsdk.StopEventDataStoreIngestionInput{
		EventDataStore: aws.String(edsArn),
	})
	require.NoError(t, err)

	_, err = client.StartEventDataStoreIngestion(t.Context(), &cloudtrailsdk.StartEventDataStoreIngestionInput{
		EventDataStore: aws.String(edsArn),
	})
	require.NoError(t, err)

	restoreOut, err := client.RestoreEventDataStore(t.Context(), &cloudtrailsdk.RestoreEventDataStoreInput{
		EventDataStore: aws.String(edsArn),
	})
	require.NoError(t, err)
	assert.Equal(t, cttypes.EventDataStoreStatusEnabled, restoreOut.Status)

	_, err = client.DeleteEventDataStore(t.Context(), &cloudtrailsdk.DeleteEventDataStoreInput{
		EventDataStore: aws.String(edsArn),
	})
	require.NoError(t, err)
}

// TestTrail_TypedLifecycle drives GetTrail, StartLogging, StopLogging and
// UpdateTrail through a real SDK client.
func TestTrail_TypedLifecycle(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.CreateTrail(t.Context(), &cloudtrailsdk.CreateTrailInput{
		Name:         aws.String("my-trail"),
		S3BucketName: aws.String("my-bucket"),
	})
	require.NoError(t, err)

	getOut, err := client.GetTrail(t.Context(), &cloudtrailsdk.GetTrailInput{
		Name: aws.String("my-trail"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Trail)
	assert.Equal(t, "my-trail", aws.ToString(getOut.Trail.Name))
	assert.Equal(t, "my-bucket", aws.ToString(getOut.Trail.S3BucketName))

	_, err = client.StartLogging(t.Context(), &cloudtrailsdk.StartLoggingInput{
		Name: aws.String("my-trail"),
	})
	require.NoError(t, err)

	statusOut, err := client.GetTrailStatus(t.Context(), &cloudtrailsdk.GetTrailStatusInput{
		Name: aws.String("my-trail"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(statusOut.IsLogging))

	_, err = client.StopLogging(t.Context(), &cloudtrailsdk.StopLoggingInput{
		Name: aws.String("my-trail"),
	})
	require.NoError(t, err)

	statusOut2, err := client.GetTrailStatus(t.Context(), &cloudtrailsdk.GetTrailStatusInput{
		Name: aws.String("my-trail"),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(statusOut2.IsLogging))

	updateOut, err := client.UpdateTrail(t.Context(), &cloudtrailsdk.UpdateTrailInput{
		Name:               aws.String("my-trail"),
		S3BucketName:       aws.String("my-other-bucket"),
		IsMultiRegionTrail: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-other-bucket", aws.ToString(updateOut.S3BucketName))
	assert.True(t, aws.ToBool(updateOut.IsMultiRegionTrail))
}

// TestEventSelectorsAndConfiguration_TypedRoundTrip drives GetEventSelectors,
// PutEventConfiguration and GetEventConfiguration through a real SDK client
// (PutEventSelectors itself is already covered).
func TestEventSelectorsAndConfiguration_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.CreateTrail(t.Context(), &cloudtrailsdk.CreateTrailInput{
		Name:         aws.String("my-trail"),
		S3BucketName: aws.String("my-bucket"),
	})
	require.NoError(t, err)

	_, err = client.PutEventSelectors(t.Context(), &cloudtrailsdk.PutEventSelectorsInput{
		TrailName: aws.String("my-trail"),
		EventSelectors: []cttypes.EventSelector{
			{ReadWriteType: cttypes.ReadWriteTypeWriteOnly, IncludeManagementEvents: aws.Bool(true)},
		},
	})
	require.NoError(t, err)

	getSelOut, err := client.GetEventSelectors(t.Context(), &cloudtrailsdk.GetEventSelectorsInput{
		TrailName: aws.String("my-trail"),
	})
	require.NoError(t, err)
	require.Len(t, getSelOut.EventSelectors, 1)
	assert.Equal(t, cttypes.ReadWriteTypeWriteOnly, getSelOut.EventSelectors[0].ReadWriteType)
	assert.NotEmpty(t, aws.ToString(getSelOut.TrailARN))

	putCfgOut, err := client.PutEventConfiguration(t.Context(), &cloudtrailsdk.PutEventConfigurationInput{
		TrailName:    aws.String("my-trail"),
		MaxEventSize: cttypes.MaxEventSizeLarge,
	})
	require.NoError(t, err)
	assert.Equal(t, cttypes.MaxEventSizeLarge, putCfgOut.MaxEventSize)
	assert.NotEmpty(t, aws.ToString(putCfgOut.TrailARN))

	getCfgOut, err := client.GetEventConfiguration(t.Context(), &cloudtrailsdk.GetEventConfigurationInput{
		TrailName: aws.String("my-trail"),
	})
	require.NoError(t, err)
	assert.Equal(t, cttypes.MaxEventSizeLarge, getCfgOut.MaxEventSize)
}

// TestResourcePolicy_TypedRoundTrip drives PutResourcePolicy through a real
// SDK client (GetResourcePolicy/DeleteResourcePolicy are already covered).
func TestResourcePolicy_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	createOut, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("my-eds"),
	})
	require.NoError(t, err)
	edsArn := aws.ToString(createOut.EventDataStoreArn)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	putOut, err := client.PutResourcePolicy(t.Context(), &cloudtrailsdk.PutResourcePolicyInput{
		ResourceArn:    aws.String(edsArn),
		ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(putOut.ResourcePolicy))

	getOut, err := client.GetResourcePolicy(t.Context(), &cloudtrailsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(edsArn),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(getOut.ResourcePolicy))
}

// TestDashboard_TypedListAndUpdate drives ListDashboards and UpdateDashboard
// through a real SDK client.
func TestDashboard_TypedListAndUpdate(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	createOut, err := client.CreateDashboard(t.Context(), &cloudtrailsdk.CreateDashboardInput{
		Name: aws.String("my-dashboard"),
	})
	require.NoError(t, err)
	dashArn := aws.ToString(createOut.DashboardArn)
	require.NotEmpty(t, dashArn)

	listOut, err := client.ListDashboards(t.Context(), &cloudtrailsdk.ListDashboardsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Dashboards, 1)
	assert.Equal(t, dashArn, aws.ToString(listOut.Dashboards[0].DashboardArn))

	updateOut, err := client.UpdateDashboard(t.Context(), &cloudtrailsdk.UpdateDashboardInput{
		DashboardId:                  aws.String(dashArn),
		TerminationProtectionEnabled: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(updateOut.TerminationProtectionEnabled))
}

// TestImports_TypedLifecycle drives StartImport, ListImports and
// ListImportFailures through a real SDK client.
func TestImports_TypedLifecycle(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	createOut, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("my-eds"),
	})
	require.NoError(t, err)
	edsArn := aws.ToString(createOut.EventDataStoreArn)

	startOut, err := client.StartImport(t.Context(), &cloudtrailsdk.StartImportInput{
		Destinations: []string{edsArn},
		ImportSource: &cttypes.ImportSource{
			S3: &cttypes.S3ImportSource{
				S3LocationUri:         aws.String("s3://my-bucket/prefix"),
				S3BucketRegion:        aws.String(ctTagsRTRegion),
				S3BucketAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/import-role"),
			},
		},
	})
	require.NoError(t, err)
	importID := aws.ToString(startOut.ImportId)
	require.NotEmpty(t, importID)
	require.Len(t, startOut.Destinations, 1)
	assert.Equal(t, edsArn, startOut.Destinations[0])

	listOut, err := client.ListImports(t.Context(), &cloudtrailsdk.ListImportsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Imports, 1)
	assert.Equal(t, importID, aws.ToString(listOut.Imports[0].ImportId))
	require.Len(t, listOut.Imports[0].Destinations, 1)
	assert.Equal(t, edsArn, listOut.Imports[0].Destinations[0])

	failuresOut, err := client.ListImportFailures(t.Context(), &cloudtrailsdk.ListImportFailuresInput{
		ImportId: aws.String(importID),
	})
	require.NoError(t, err)
	assert.Empty(t, failuresOut.Failures)
}

// TestQueryFamily_TypedRoundTrip drives CancelQuery, GenerateQuery and
// SearchSampleQueries through a real SDK client.
func TestQueryFamily_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	createOut, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("my-eds"),
	})
	require.NoError(t, err)
	edsArn := aws.ToString(createOut.EventDataStoreArn)

	startOut, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
		QueryStatement: aws.String("SELECT eventName FROM " + edsArn),
	})
	require.NoError(t, err)
	queryID := aws.ToString(startOut.QueryId)
	require.NotEmpty(t, queryID)

	cancelOut, err := client.CancelQuery(t.Context(), &cloudtrailsdk.CancelQueryInput{
		QueryId: aws.String(queryID),
	})
	require.NoError(t, err)
	assert.Equal(t, cttypes.QueryStatusCancelled, cancelOut.QueryStatus)

	genOut, err := client.GenerateQuery(t.Context(), &cloudtrailsdk.GenerateQueryInput{
		EventDataStores: []string{edsArn},
		Prompt:          aws.String("show me all events"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(genOut.QueryStatement))

	searchOut, err := client.SearchSampleQueries(t.Context(), &cloudtrailsdk.SearchSampleQueriesInput{
		SearchPhrase: aws.String("errors"),
	})
	require.NoError(t, err)
	assert.Empty(t, searchOut.SearchResults)
}

// TestOrganizationDelegatedAdmin_TypedRoundTrip drives
// RegisterOrganizationDelegatedAdmin and DeregisterOrganizationDelegatedAdmin
// through a real SDK client.
func TestOrganizationDelegatedAdmin_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	regOut, err := client.RegisterOrganizationDelegatedAdmin(
		t.Context(), &cloudtrailsdk.RegisterOrganizationDelegatedAdminInput{
			MemberAccountId: aws.String("222233334444"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, regOut)

	deregOut, err := client.DeregisterOrganizationDelegatedAdmin(
		t.Context(), &cloudtrailsdk.DeregisterOrganizationDelegatedAdminInput{
			DelegatedAdminAccountId: aws.String("222233334444"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, deregOut)
}

// TestListPublicKeys_TypedRoundTrip drives ListPublicKeys through a real SDK
// client.
func TestListPublicKeys_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	out, err := client.ListPublicKeys(t.Context(), &cloudtrailsdk.ListPublicKeysInput{})
	require.NoError(t, err)
	assert.Empty(t, out.PublicKeyList)
}

// TestInsights_TypedRoundTrip drives ListInsightsData and
// ListInsightsMetricData through a real SDK client.
func TestInsights_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.CreateTrail(t.Context(), &cloudtrailsdk.CreateTrailInput{
		Name:         aws.String("my-trail"),
		S3BucketName: aws.String("my-bucket"),
	})
	require.NoError(t, err)

	dataOut, err := client.ListInsightsData(t.Context(), &cloudtrailsdk.ListInsightsDataInput{
		DataType:      cttypes.ListInsightsDataTypeInsightsEvents,
		InsightSource: aws.String("arn:aws:cloudtrail:us-east-1:123456789012:trail/my-trail"),
	})
	require.NoError(t, err)
	assert.Empty(t, dataOut.Events)

	metricOut, err := client.ListInsightsMetricData(t.Context(), &cloudtrailsdk.ListInsightsMetricDataInput{
		EventName:   aws.String("PutObject"),
		EventSource: aws.String("s3.amazonaws.com"),
		InsightType: cttypes.InsightTypeApiCallRateInsight,
		TrailName:   aws.String("my-trail"),
	})
	require.NoError(t, err)
	assert.Equal(t, "PutObject", aws.ToString(metricOut.EventName))
	assert.NotEmpty(t, aws.ToString(metricOut.TrailARN))
}
