package shield_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	shieldsdk "github.com/aws/aws-sdk-go-v2/service/shield"
	shieldtypes "github.com/aws/aws-sdk-go-v2/service/shield/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/shield"
)

const shieldRealClientRegion = "us-east-1"

// newTestShieldClient stands up the real aws-sdk-go-v2 shield client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- this service
// had no prior typed-client helper (grep found none).
func newTestShieldClient(t *testing.T, h *shield.Handler) *shieldsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(shieldRealClientRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return shieldsdk.NewFromConfig(cfg, func(o *shieldsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestShieldHandlerAndBackend() (*shield.Handler, *shield.InMemoryBackend) {
	backend := shield.NewInMemoryBackend("123456789012", shieldRealClientRegion)

	return shield.NewHandler(backend), backend
}

// TestSlice33_Shield_RealClient drives every gopherstack-n3zi
// typed-slice-33 uncovered shield op through the real aws-sdk-go-v2 client.
func TestSlice33_Shield_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testProtectionGroupsRealClient, "protection_groups"},
		{testDRTRealClient, "drt"},
		{testEmergencyContactsAndProactiveEngagementRealClient, "emergency_contacts_and_proactive_engagement"},
		{testALARRealClient, "alar"},
		{testHealthChecksRealClient, "health_checks"},
		{testAttacksRealClient, "attacks"},
		{testSubscriptionRealClient, "subscription"},
		{testTagsRealClient, "tags"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testProtectionGroupsRealClient covers CreateProtectionGroup,
// DescribeProtectionGroup, ListProtectionGroups, UpdateProtectionGroup,
// DeleteProtectionGroup, ListResourcesInProtectionGroup.
func testProtectionGroupsRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	_, err := client.CreateProtectionGroup(ctx, &shieldsdk.CreateProtectionGroupInput{
		ProtectionGroupId: aws.String("my-pg"),
		Aggregation:       shieldtypes.ProtectionGroupAggregationSum,
		Pattern:           shieldtypes.ProtectionGroupPatternAll,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeProtectionGroup(ctx, &shieldsdk.DescribeProtectionGroupInput{
		ProtectionGroupId: aws.String("my-pg"),
	})
	require.NoError(t, err)
	assert.Equal(t, shieldtypes.ProtectionGroupAggregationSum, descOut.ProtectionGroup.Aggregation)

	listOut, err := client.ListProtectionGroups(ctx, &shieldsdk.ListProtectionGroupsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ProtectionGroups, 1)

	_, err = client.UpdateProtectionGroup(ctx, &shieldsdk.UpdateProtectionGroupInput{
		ProtectionGroupId: aws.String("my-pg"),
		Aggregation:       shieldtypes.ProtectionGroupAggregationMax,
		Pattern:           shieldtypes.ProtectionGroupPatternAll,
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeProtectionGroup(ctx, &shieldsdk.DescribeProtectionGroupInput{
		ProtectionGroupId: aws.String("my-pg"),
	})
	require.NoError(t, err)
	assert.Equal(t, shieldtypes.ProtectionGroupAggregationMax, descOut2.ProtectionGroup.Aggregation)

	resOut, err := client.ListResourcesInProtectionGroup(ctx, &shieldsdk.ListResourcesInProtectionGroupInput{
		ProtectionGroupId: aws.String("my-pg"),
	})
	require.NoError(t, err)
	assert.Empty(t, resOut.ResourceArns)

	_, err = client.DeleteProtectionGroup(ctx, &shieldsdk.DeleteProtectionGroupInput{
		ProtectionGroupId: aws.String("my-pg"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListProtectionGroups(ctx, &shieldsdk.ListProtectionGroupsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.ProtectionGroups)
}

// testDRTRealClient covers AssociateDRTLogBucket, DisassociateDRTLogBucket,
// AssociateDRTRole, DisassociateDRTRole, DescribeDRTAccess.
func testDRTRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	// AssociateDRTRole must precede AssociateDRTLogBucket (real requirement:
	// the DRT log bucket association depends on an already-associated role).
	_, err := client.AssociateDRTRole(ctx, &shieldsdk.AssociateDRTRoleInput{
		RoleArn: aws.String("arn:aws:iam::123456789012:role/drt-role"),
	})
	require.NoError(t, err)

	_, err = client.AssociateDRTLogBucket(ctx, &shieldsdk.AssociateDRTLogBucketInput{
		LogBucket: aws.String("my-drt-log-bucket"),
	})
	require.NoError(t, err)

	accessOut, err := client.DescribeDRTAccess(ctx, &shieldsdk.DescribeDRTAccessInput{})
	require.NoError(t, err)
	assert.Contains(t, accessOut.LogBucketList, "my-drt-log-bucket")
	assert.Equal(t, "arn:aws:iam::123456789012:role/drt-role", aws.ToString(accessOut.RoleArn))

	_, err = client.DisassociateDRTLogBucket(ctx, &shieldsdk.DisassociateDRTLogBucketInput{
		LogBucket: aws.String("my-drt-log-bucket"),
	})
	require.NoError(t, err)

	_, err = client.DisassociateDRTRole(ctx, &shieldsdk.DisassociateDRTRoleInput{})
	require.NoError(t, err)

	accessOut2, err := client.DescribeDRTAccess(ctx, &shieldsdk.DescribeDRTAccessInput{})
	require.NoError(t, err)
	assert.NotContains(t, accessOut2.LogBucketList, "my-drt-log-bucket")
}

// testEmergencyContactsAndProactiveEngagementRealClient covers
// AssociateProactiveEngagementDetails, UpdateEmergencyContactSettings,
// DescribeEmergencyContactSettings, EnableProactiveEngagement,
// DisableProactiveEngagement.
func testEmergencyContactsAndProactiveEngagementRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	_, err := client.AssociateProactiveEngagementDetails(ctx, &shieldsdk.AssociateProactiveEngagementDetailsInput{
		EmergencyContactList: []shieldtypes.EmergencyContact{
			{EmailAddress: aws.String("oncall@example.com")},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeEmergencyContactSettings(ctx, &shieldsdk.DescribeEmergencyContactSettingsInput{})
	require.NoError(t, err)
	require.Len(t, descOut.EmergencyContactList, 1)
	assert.Equal(t, "oncall@example.com", aws.ToString(descOut.EmergencyContactList[0].EmailAddress))

	_, err = client.EnableProactiveEngagement(ctx, &shieldsdk.EnableProactiveEngagementInput{})
	require.NoError(t, err)

	subOut, err := client.DescribeSubscription(ctx, &shieldsdk.DescribeSubscriptionInput{})
	require.NoError(t, err)
	assert.Equal(t, shieldtypes.ProactiveEngagementStatusEnabled, subOut.Subscription.ProactiveEngagementStatus)

	_, err = client.UpdateEmergencyContactSettings(ctx, &shieldsdk.UpdateEmergencyContactSettingsInput{
		EmergencyContactList: []shieldtypes.EmergencyContact{
			{EmailAddress: aws.String("oncall2@example.com")},
		},
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeEmergencyContactSettings(ctx, &shieldsdk.DescribeEmergencyContactSettingsInput{})
	require.NoError(t, err)
	require.Len(t, descOut2.EmergencyContactList, 1)
	assert.Equal(t, "oncall2@example.com", aws.ToString(descOut2.EmergencyContactList[0].EmailAddress))

	_, err = client.DisableProactiveEngagement(ctx, &shieldsdk.DisableProactiveEngagementInput{})
	require.NoError(t, err)

	subOut2, err := client.DescribeSubscription(ctx, &shieldsdk.DescribeSubscriptionInput{})
	require.NoError(t, err)
	assert.Equal(t, shieldtypes.ProactiveEngagementStatusDisabled, subOut2.Subscription.ProactiveEngagementStatus)
}

// testALARRealClient covers EnableApplicationLayerAutomaticResponse,
// UpdateApplicationLayerAutomaticResponse,
// DisableApplicationLayerAutomaticResponse.
func testALARRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	resourceARN := "arn:aws:cloudfront::123456789012:distribution/EXAMPLE"

	_, err := backend.CreateProtection("my-protection", resourceARN, nil)
	require.NoError(t, err)

	_, err = client.EnableApplicationLayerAutomaticResponse(
		ctx,
		&shieldsdk.EnableApplicationLayerAutomaticResponseInput{
			ResourceArn: aws.String(resourceARN),
			Action:      &shieldtypes.ResponseAction{Block: &shieldtypes.BlockAction{}},
		},
	)
	require.NoError(t, err)

	_, err = client.UpdateApplicationLayerAutomaticResponse(
		ctx,
		&shieldsdk.UpdateApplicationLayerAutomaticResponseInput{
			ResourceArn: aws.String(resourceARN),
			Action:      &shieldtypes.ResponseAction{Count: &shieldtypes.CountAction{}},
		},
	)
	require.NoError(t, err)

	_, err = client.DisableApplicationLayerAutomaticResponse(
		ctx, &shieldsdk.DisableApplicationLayerAutomaticResponseInput{ResourceArn: aws.String(resourceARN)},
	)
	require.NoError(t, err)
}

// testHealthChecksRealClient covers AssociateHealthCheck and
// DisassociateHealthCheck.
func testHealthChecksRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	resourceARN := "arn:aws:cloudfront::123456789012:distribution/EXAMPLE2"

	prot, err := backend.CreateProtection("hc-protection", resourceARN, nil)
	require.NoError(t, err)

	healthCheckARN := "arn:aws:route53:::healthcheck/abcd1234-abcd-1234-abcd-1234abcd1234"

	_, err = client.AssociateHealthCheck(ctx, &shieldsdk.AssociateHealthCheckInput{
		ProtectionId:   aws.String(prot.ID),
		HealthCheckArn: aws.String(healthCheckARN),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeProtection(
		ctx,
		&shieldsdk.DescribeProtectionInput{ProtectionId: aws.String(prot.ID)},
	)
	require.NoError(t, err)
	require.Len(t, descOut.Protection.HealthCheckIds, 1)
	assert.Equal(t, healthCheckARN, descOut.Protection.HealthCheckIds[0])

	_, err = client.DisassociateHealthCheck(ctx, &shieldsdk.DisassociateHealthCheckInput{
		ProtectionId:   aws.String(prot.ID),
		HealthCheckArn: aws.String(healthCheckARN),
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeProtection(
		ctx,
		&shieldsdk.DescribeProtectionInput{ProtectionId: aws.String(prot.ID)},
	)
	require.NoError(t, err)
	assert.Empty(t, descOut2.Protection.HealthCheckIds)
}

// testAttacksRealClient covers DescribeAttack, DescribeAttackStatistics,
// ListAttacks. SimulateAttack is a gopherstack-only chaos endpoint (not a
// real AWS Shield operation, so no typed client method exists for it); the
// attack record is seeded by calling the backend method directly, the same
// test-only-seed pattern other structural-gap sweeps in this issue use
// (e.g. cognitoidp's SeedAuthEventForTest) -- the ops under coverage here
// are the real, typed-client-reachable read ops.
func testAttacksRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	resourceARN := "arn:aws:cloudfront::123456789012:distribution/EXAMPLE3"

	_, err := backend.CreateProtection("attack-protection", resourceARN, nil)
	require.NoError(t, err)

	attack, err := backend.SimulateAttack(resourceARN, []string{"UDP_FLOOD"})
	require.NoError(t, err)

	descOut, err := client.DescribeAttack(ctx, &shieldsdk.DescribeAttackInput{AttackId: aws.String(attack.AttackID)})
	require.NoError(t, err)
	require.NotNil(t, descOut.Attack)
	assert.Equal(t, resourceARN, aws.ToString(descOut.Attack.ResourceArn))
	require.NotEmpty(t, descOut.Attack.AttackCounters)
	require.NotEmpty(t, descOut.Attack.Mitigations)

	listOut, err := client.ListAttacks(ctx, &shieldsdk.ListAttacksInput{
		ResourceArns: []string{resourceARN},
	})
	require.NoError(t, err)
	require.Len(t, listOut.AttackSummaries, 1)
	assert.Equal(t, attack.AttackID, aws.ToString(listOut.AttackSummaries[0].AttackId))
	require.Len(t, listOut.AttackSummaries[0].AttackVectors, 1)
	assert.Equal(t, "UDP_FLOOD", aws.ToString(listOut.AttackSummaries[0].AttackVectors[0].VectorType))

	statsOut, err := client.DescribeAttackStatistics(ctx, &shieldsdk.DescribeAttackStatisticsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, statsOut.DataItems)
}

// testSubscriptionRealClient covers UpdateSubscription and
// GetSubscriptionState.
func testSubscriptionRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	stateOut, err := client.GetSubscriptionState(ctx, &shieldsdk.GetSubscriptionStateInput{})
	require.NoError(t, err)
	assert.Equal(t, shieldtypes.SubscriptionStateActive, stateOut.SubscriptionState)

	_, err = client.UpdateSubscription(ctx, &shieldsdk.UpdateSubscriptionInput{
		AutoRenew: shieldtypes.AutoRenewDisabled,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeSubscription(ctx, &shieldsdk.DescribeSubscriptionInput{})
	require.NoError(t, err)
	assert.Equal(t, shieldtypes.AutoRenewDisabled, descOut.Subscription.AutoRenew)
}

// testTagsRealClient covers TagResource, ListTagsForResource,
// UntagResource.
func testTagsRealClient(t *testing.T) {
	t.Helper()

	h, backend := newTestShieldHandlerAndBackend()
	client := newTestShieldClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateSubscription())

	resourceARN := "arn:aws:cloudfront::123456789012:distribution/EXAMPLE4"

	prot, err := backend.CreateProtection("tag-protection", resourceARN, nil)
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &shieldsdk.TagResourceInput{
		ResourceARN: aws.String(prot.ProtectionArn),
		Tags: []shieldtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("sec")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &shieldsdk.ListTagsForResourceInput{
		ResourceARN: aws.String(prot.ProtectionArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	_, err = client.UntagResource(ctx, &shieldsdk.UntagResourceInput{
		ResourceARN: aws.String(prot.ProtectionArn),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &shieldsdk.ListTagsForResourceInput{
		ResourceARN: aws.String(prot.ProtectionArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.Tags[0].Key))
}
