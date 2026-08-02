package lightsail_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/require"
)

// TestAlarmAndContactMethodRoundTrip exercises family W end to end.
func TestAlarmAndContactMethodRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames: []string{"alarm-target"}, AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId: aws.String("amazon_linux_2023"), BundleId: aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.PutAlarm(ctx, &lightsailsdk.PutAlarmInput{
		AlarmName: aws.String("alarm-1"), ComparisonOperator: lightsailtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods: aws.Int32(1), MetricName: lightsailtypes.MetricNameCPUUtilization,
		MonitoredResourceName: aws.String("alarm-target"), Threshold: aws.Float64(80),
	})
	require.NoError(t, err)

	getOut, err := client.GetAlarms(ctx, &lightsailsdk.GetAlarmsInput{AlarmName: aws.String("alarm-1")})
	require.NoError(t, err)
	require.Len(t, getOut.Alarms, 1)
	require.Equal(t, lightsailtypes.AlarmStateInsufficientData, getOut.Alarms[0].State)

	_, err = client.TestAlarm(
		ctx,
		&lightsailsdk.TestAlarmInput{AlarmName: aws.String("alarm-1"), State: lightsailtypes.AlarmStateAlarm},
	)
	require.NoError(t, err)

	afterTest, err := client.GetAlarms(ctx, &lightsailsdk.GetAlarmsInput{AlarmName: aws.String("alarm-1")})
	require.NoError(t, err)
	require.Equal(t, lightsailtypes.AlarmStateAlarm, afterTest.Alarms[0].State)

	_, err = client.DeleteAlarm(ctx, &lightsailsdk.DeleteAlarmInput{AlarmName: aws.String("alarm-1")})
	require.NoError(t, err)

	_, err = client.CreateContactMethod(ctx, &lightsailsdk.CreateContactMethodInput{
		Protocol: lightsailtypes.ContactProtocolEmail, ContactEndpoint: aws.String("ops@example.com"),
	})
	require.NoError(t, err)

	cmOut, err := client.GetContactMethods(ctx, &lightsailsdk.GetContactMethodsInput{})
	require.NoError(t, err)
	require.Len(t, cmOut.ContactMethods, 1)
	require.Equal(t, lightsailtypes.ContactMethodStatusPendingVerification, cmOut.ContactMethods[0].Status)

	_, err = client.SendContactMethodVerification(ctx, &lightsailsdk.SendContactMethodVerificationInput{
		Protocol: lightsailtypes.ContactMethodVerificationProtocolEmail,
	})
	require.NoError(t, err)

	afterVerify, err := client.GetContactMethods(ctx, &lightsailsdk.GetContactMethodsInput{})
	require.NoError(t, err)
	require.Equal(t, lightsailtypes.ContactMethodStatusValid, afterVerify.ContactMethods[0].Status)

	_, err = client.DeleteContactMethod(
		ctx,
		&lightsailsdk.DeleteContactMethodInput{Protocol: lightsailtypes.ContactProtocolEmail},
	)
	require.NoError(t, err)
}

// TestVpcPeeringAndTaggingAndMiscRoundTrip exercises family X, Y, AA, BB
// end to end, including the name-first TagResource/UntagResource inversion
// (PARITY.md 5.1).
func TestVpcPeeringAndTaggingAndMiscRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	isPeered, err := client.IsVpcPeered(ctx, &lightsailsdk.IsVpcPeeredInput{})
	require.NoError(t, err)
	require.False(t, aws.ToBool(isPeered.IsPeered))

	_, err = client.PeerVpc(ctx, &lightsailsdk.PeerVpcInput{})
	require.NoError(t, err)

	afterPeer, err := client.IsVpcPeered(ctx, &lightsailsdk.IsVpcPeeredInput{})
	require.NoError(t, err)
	require.True(t, aws.ToBool(afterPeer.IsPeered))

	_, err = client.UnpeerVpc(ctx, &lightsailsdk.UnpeerVpcInput{})
	require.NoError(t, err)

	// Tagging (family Y) -- ResourceName-first, not ARN-first.
	_, err = client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames: []string{"tag-target"}, AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId: aws.String("amazon_linux_2023"), BundleId: aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &lightsailsdk.TagResourceInput{
		ResourceName: aws.String(
			"tag-target",
		), Tags: []lightsailtypes.Tag{{Key: aws.String("owner"), Value: aws.String("team-a")}},
	})
	require.NoError(t, err)

	tagged, err := client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{InstanceName: aws.String("tag-target")})
	require.NoError(t, err)
	require.Len(t, tagged.Instance.Tags, 1)
	require.Equal(t, "owner", aws.ToString(tagged.Instance.Tags[0].Key))

	_, err = client.UntagResource(
		ctx,
		&lightsailsdk.UntagResourceInput{ResourceName: aws.String("tag-target"), TagKeys: []string{"owner"}},
	)
	require.NoError(t, err)

	untagged, err := client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{InstanceName: aws.String("tag-target")})
	require.NoError(t, err)
	require.Empty(t, untagged.Instance.Tags)

	// GUI sessions (family AA).
	_, err = client.CreateGUISessionAccessDetails(
		ctx,
		&lightsailsdk.CreateGUISessionAccessDetailsInput{ResourceName: aws.String("tag-target")},
	)
	require.NoError(t, err)

	_, err = client.StartGUISession(ctx, &lightsailsdk.StartGUISessionInput{ResourceName: aws.String("tag-target")})
	require.NoError(t, err)

	_, err = client.StopGUISession(ctx, &lightsailsdk.StopGUISessionInput{ResourceName: aws.String("tag-target")})
	require.NoError(t, err)

	// Misc (family BB).
	namesOut, err := client.GetActiveNames(ctx, &lightsailsdk.GetActiveNamesInput{})
	require.NoError(t, err)
	require.Contains(t, namesOut.ActiveNames, "tag-target")

	now := time.Now()

	_, err = client.GetCostEstimate(ctx, &lightsailsdk.GetCostEstimateInput{
		ResourceName: aws.String("tag-target"), StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now),
	})
	require.NoError(t, err)
}

// TestReferenceDataRoundTrip exercises family A end to end.
func TestReferenceDataRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	bpOut, err := client.GetBlueprints(ctx, &lightsailsdk.GetBlueprintsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, bpOut.Blueprints)

	bdOut, err := client.GetBundles(ctx, &lightsailsdk.GetBundlesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, bdOut.Bundles)

	rdsBpOut, err := client.GetRelationalDatabaseBlueprints(ctx, &lightsailsdk.GetRelationalDatabaseBlueprintsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, rdsBpOut.Blueprints)

	rdsBdOut, err := client.GetRelationalDatabaseBundles(ctx, &lightsailsdk.GetRelationalDatabaseBundlesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, rdsBdOut.Bundles)

	bucketBdOut, err := client.GetBucketBundles(ctx, &lightsailsdk.GetBucketBundlesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, bucketBdOut.Bundles)

	distBdOut, err := client.GetDistributionBundles(ctx, &lightsailsdk.GetDistributionBundlesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, distBdOut.Bundles)

	powersOut, err := client.GetContainerServicePowers(ctx, &lightsailsdk.GetContainerServicePowersInput{})
	require.NoError(t, err)
	require.NotEmpty(t, powersOut.Powers)

	regionsOut, err := client.GetRegions(ctx, &lightsailsdk.GetRegionsInput{IncludeAvailabilityZones: aws.Bool(true)})
	require.NoError(t, err)
	require.Len(t, regionsOut.Regions, 20)
	require.NotEmpty(t, regionsOut.Regions[0].AvailabilityZones)

	metaOut, err := client.GetContainerAPIMetadata(ctx, &lightsailsdk.GetContainerAPIMetadataInput{})
	require.NoError(t, err)
	require.NotEmpty(t, metaOut.Metadata)
}
