package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestAccessLogSubscription_ServiceNetworkLogType drives
// CreateAccessLogSubscription/GetAccessLogSubscription/
// ListAccessLogSubscriptions through the real SDK client and proves
// ServiceNetworkLogType round-trips. Previously tracked by the backend but
// never emitted by either op -- a real client's typed field was always ""
// regardless of what was created.
func TestAccessLogSubscription_ServiceNetworkLogType(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(
		ctx,
		&vpclatticesdk.CreateServiceInput{Name: aws.String("svc-als")},
	)
	require.NoError(t, err)

	created, err := client.CreateAccessLogSubscription(
		ctx,
		&vpclatticesdk.CreateAccessLogSubscriptionInput{
			ResourceIdentifier:    svc.Id,
			DestinationArn:        aws.String("arn:aws:s3:::my-bucket"),
			ServiceNetworkLogType: vpclatticetypes.ServiceNetworkLogTypeService,
		},
	)
	require.NoError(t, err)

	got, err := client.GetAccessLogSubscription(ctx, &vpclatticesdk.GetAccessLogSubscriptionInput{
		AccessLogSubscriptionIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, vpclatticetypes.ServiceNetworkLogTypeService, got.ServiceNetworkLogType)

	list, err := client.ListAccessLogSubscriptions(
		ctx,
		&vpclatticesdk.ListAccessLogSubscriptionsInput{
			ResourceIdentifier: svc.Id,
		},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(
		t,
		vpclatticetypes.ServiceNetworkLogTypeService,
		list.Items[0].ServiceNetworkLogType,
	)
}

// TestListAccessLogSubscriptions_LastUpdatedAt proves ListAccessLogSubscriptions
// items carry the required LastUpdatedAt member. AccessLogSubscriptionSummary
// already tracked it (and GetAccessLogSubscription already emitted it) but
// alsSummaryToJSON never wrote the "lastUpdatedAt" key, so a real client's
// typed field on every list item was always nil despite the real SDK marking
// it "This member is required." on AccessLogSubscriptionSummary
// (types/types.go).
func TestListAccessLogSubscriptions_LastUpdatedAt(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(
		ctx,
		&vpclatticesdk.CreateServiceInput{Name: aws.String("svc-als-lastupdated")},
	)
	require.NoError(t, err)

	_, err = client.CreateAccessLogSubscription(
		ctx,
		&vpclatticesdk.CreateAccessLogSubscriptionInput{
			ResourceIdentifier: svc.Id,
			DestinationArn:     aws.String("arn:aws:s3:::my-bucket"),
		},
	)
	require.NoError(t, err)

	list, err := client.ListAccessLogSubscriptions(
		ctx,
		&vpclatticesdk.ListAccessLogSubscriptionsInput{
			ResourceIdentifier: svc.Id,
		},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.NotNil(t, list.Items[0].LastUpdatedAt)
}

// TestRule_PathMatchWireKeyAndCaseSensitive drives CreateRule/GetRule
// through the real SDK client with a path-match condition. Previously both
// extractPathMatch (request) and ruleMatchToJSON (response) used "path"
// where the real wire key on both sides is "pathMatch"
// (serializers.go:6541 / deserializers.go) -- a real client's path
// condition was silently discarded on create and never echoed back.
// HeaderMatch/PathMatch's CaseSensitive is also proven to round-trip here:
// tracked by the model but never wired on either side before this fix.
func TestRule_PathMatchWireKeyAndCaseSensitive(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(
		ctx,
		&vpclatticesdk.CreateServiceInput{Name: aws.String("svc-rulematch")},
	)
	require.NoError(t, err)

	listener, err := client.CreateListener(ctx, &vpclatticesdk.CreateListenerInput{
		ServiceIdentifier: svc.Id,
		Name:              aws.String("l1"),
		Protocol:          vpclatticetypes.ListenerProtocolHttp,
		DefaultAction: &vpclatticetypes.RuleActionMemberFixedResponse{
			Value: vpclatticetypes.FixedResponseAction{StatusCode: aws.Int32(404)},
		},
	})
	require.NoError(t, err)

	rule, err := client.CreateRule(ctx, &vpclatticesdk.CreateRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		Name:               aws.String("r1"),
		Priority:           aws.Int32(5),
		Action: &vpclatticetypes.RuleActionMemberFixedResponse{
			Value: vpclatticetypes.FixedResponseAction{StatusCode: aws.Int32(404)},
		},
		Match: &vpclatticetypes.RuleMatchMemberHttpMatch{
			Value: vpclatticetypes.HttpMatch{
				PathMatch: &vpclatticetypes.PathMatch{
					Match:         &vpclatticetypes.PathMatchTypeMemberExact{Value: "/api"},
					CaseSensitive: aws.Bool(true),
				},
				HeaderMatches: []vpclatticetypes.HeaderMatch{
					{
						Name:          aws.String("x-env"),
						Match:         &vpclatticetypes.HeaderMatchTypeMemberExact{Value: "prod"},
						CaseSensitive: aws.Bool(true),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetRule(ctx, &vpclatticesdk.GetRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		RuleIdentifier:     rule.Id,
	})
	require.NoError(t, err)

	matchVal, ok := got.Match.(*vpclatticetypes.RuleMatchMemberHttpMatch)
	require.True(t, ok, "Match must be an HttpMatch member")
	require.NotNil(t, matchVal.Value.PathMatch, "PathMatch must round-trip under the real wire key")
	assert.True(t, aws.ToBool(matchVal.Value.PathMatch.CaseSensitive))

	exact, ok := matchVal.Value.PathMatch.Match.(*vpclatticetypes.PathMatchTypeMemberExact)
	require.True(t, ok)
	assert.Equal(t, "/api", exact.Value)

	require.Len(t, matchVal.Value.HeaderMatches, 1)
	assert.True(t, aws.ToBool(matchVal.Value.HeaderMatches[0].CaseSensitive))
}

// TestListServiceNetworks_AssociationCounts proves ListServiceNetworks
// reports live association counts. Previously toSummary() never
// recomputed NumberOfAssociatedServices/VPCs/ResourceConfigurations (only
// GetServiceNetwork did), so every list item's counts were always 0
// regardless of real associations.
func TestListServiceNetworks_AssociationCounts(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(
		ctx,
		&vpclatticesdk.CreateServiceNetworkInput{Name: aws.String("sn-counts")},
	)
	require.NoError(t, err)

	svc, err := client.CreateService(
		ctx,
		&vpclatticesdk.CreateServiceInput{Name: aws.String("svc-counts")},
	)
	require.NoError(t, err)

	_, err = client.CreateServiceNetworkServiceAssociation(
		ctx,
		&vpclatticesdk.CreateServiceNetworkServiceAssociationInput{
			ServiceNetworkIdentifier: sn.Id,
			ServiceIdentifier:        svc.Id,
		},
	)
	require.NoError(t, err)

	rc, err := client.CreateResourceConfiguration(
		ctx,
		&vpclatticesdk.CreateResourceConfigurationInput{
			Name: aws.String("rc-counts"),
			Type: vpclatticetypes.ResourceConfigurationTypeArn,
			ResourceConfigurationDefinition: &vpclatticetypes.ResourceConfigurationDefinitionMemberArnResource{
				Value: vpclatticetypes.ArnResource{
					Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb"),
				},
			},
		},
	)
	require.NoError(t, err)

	_, err = client.CreateServiceNetworkResourceAssociation(
		ctx,
		&vpclatticesdk.CreateServiceNetworkResourceAssociationInput{
			ServiceNetworkIdentifier:        sn.Id,
			ResourceConfigurationIdentifier: rc.Id,
		},
	)
	require.NoError(t, err)

	list, err := client.ListServiceNetworks(ctx, &vpclatticesdk.ListServiceNetworksInput{})
	require.NoError(t, err)

	require.Len(t, list.Items, 1)
	item := list.Items[0]
	assert.EqualValues(t, 1, aws.ToInt64(item.NumberOfAssociatedServices))
	assert.EqualValues(t, 1, aws.ToInt64(item.NumberOfAssociatedResourceConfigurations))
}

// TestListServices_LastUpdatedAt proves ListServices items carry
// LastUpdatedAt. ServiceSummary.LastUpdatedAt was already tracked (and
// correctly emitted by GetService) but serviceSummaryToJSON never emitted
// it, so a real client's typed field on every list item was always nil.
func TestListServices_LastUpdatedAt(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateService(
		ctx,
		&vpclatticesdk.CreateServiceInput{Name: aws.String("svc-lastupdated")},
	)
	require.NoError(t, err)

	list, err := client.ListServices(ctx, &vpclatticesdk.ListServicesInput{})
	require.NoError(t, err)

	require.Len(t, list.Items, 1)
	assert.NotNil(t, list.Items[0].LastUpdatedAt)
}

// TestTargetGroup_HealthCheck_ProtocolVersionAndMatcher drives
// CreateTargetGroup/GetTargetGroup and proves HealthCheckConfig's
// ProtocolVersion and Matcher.HttpCode round-trip. Both were parsed on
// create (ProtocolVersion) or had a dead struct field with no wiring at
// all (Matcher) but neither was ever echoed back by healthCheckToJSON.
func TestTargetGroup_HealthCheck_ProtocolVersionAndMatcher(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateTargetGroup(ctx, &vpclatticesdk.CreateTargetGroupInput{
		Name: aws.String("tg-healthcheck"),
		Type: vpclatticetypes.TargetGroupTypeInstance,
		Config: &vpclatticetypes.TargetGroupConfig{
			Port:          aws.Int32(80),
			Protocol:      vpclatticetypes.TargetGroupProtocolHttp,
			VpcIdentifier: aws.String("vpc-123"),
			HealthCheck: &vpclatticetypes.HealthCheckConfig{
				Enabled:         aws.Bool(true),
				Protocol:        vpclatticetypes.TargetGroupProtocolHttp,
				ProtocolVersion: vpclatticetypes.HealthCheckProtocolVersionHttp2,
				Matcher:         &vpclatticetypes.MatcherMemberHttpCode{Value: "200-299"},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetTargetGroup(
		ctx,
		&vpclatticesdk.GetTargetGroupInput{TargetGroupIdentifier: created.Id},
	)
	require.NoError(t, err)

	require.NotNil(t, got.Config)
	require.NotNil(t, got.Config.HealthCheck)
	assert.Equal(
		t,
		vpclatticetypes.HealthCheckProtocolVersionHttp2,
		got.Config.HealthCheck.ProtocolVersion,
	)

	matcher, ok := got.Config.HealthCheck.Matcher.(*vpclatticetypes.MatcherMemberHttpCode)
	require.True(t, ok, "Matcher must round-trip under its real wire key")
	assert.Equal(t, "200-299", matcher.Value)
}

// TestListResourceConfigurations_GroupId proves a CHILD resource
// configuration's ResourceConfigurationGroupId reaches
// ListResourceConfigurations. Previously ResourceConfigurationSummary had
// no field for it at all and the List handler's inline map never emitted
// it, even though GetResourceConfiguration already did.
func TestListResourceConfigurations_GroupId(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	group, err := client.CreateResourceConfiguration(
		ctx,
		&vpclatticesdk.CreateResourceConfigurationInput{
			Name:        aws.String("rc-group"),
			Type:        vpclatticetypes.ResourceConfigurationTypeGroup,
			GroupDomain: aws.String("group.example.com"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateResourceConfiguration(
		ctx,
		&vpclatticesdk.CreateResourceConfigurationInput{
			Name:                                 aws.String("rc-child"),
			Type:                                 vpclatticetypes.ResourceConfigurationTypeChild,
			ResourceConfigurationGroupIdentifier: group.Id,
		},
	)
	require.NoError(t, err)

	list, err := client.ListResourceConfigurations(
		ctx,
		&vpclatticesdk.ListResourceConfigurationsInput{
			ResourceConfigurationGroupIdentifier: group.Id,
		},
	)
	require.NoError(t, err)

	require.Len(t, list.Items, 1)
	assert.Equal(
		t,
		aws.ToString(group.Id),
		aws.ToString(list.Items[0].ResourceConfigurationGroupId),
	)
	assert.Equal(t, "group.example.com", aws.ToString(list.Items[0].GroupDomain))
}

// TestResourceConfiguration_CustomDomainNameAndDomainVerificationId proves
// CreateResourceConfiguration's customDomainName/domainVerificationIdentifier
// request fields, previously discarded entirely (the handler never read
// either from the body despite both being real, directly-settable
// CreateResourceConfigurationInput members -- serializers.go:433/438), now
// reach the backend and echo back on Get.
func TestResourceConfiguration_CustomDomainNameAndDomainVerificationId(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateResourceConfiguration(
		ctx,
		&vpclatticesdk.CreateResourceConfigurationInput{
			Name:                         aws.String("rc-domain"),
			Type:                         vpclatticetypes.ResourceConfigurationTypeSingle,
			CustomDomainName:             aws.String("custom.example.com"),
			DomainVerificationIdentifier: aws.String("dvi-abc123"),
		},
	)
	require.NoError(t, err)

	got, err := client.GetResourceConfiguration(ctx, &vpclatticesdk.GetResourceConfigurationInput{
		ResourceConfigurationIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "custom.example.com", aws.ToString(got.CustomDomainName))
	assert.Equal(t, "dvi-abc123", aws.ToString(got.DomainVerificationId))
}

// TestResourceConfiguration_DomainVerificationArnStatusAndAmazonManaged
// drives StartDomainVerification/CreateResourceConfiguration/
// GetResourceConfiguration through the real SDK client. GetResourceConfigurationOutput
// carries amazonManaged/domainVerificationArn/domainVerificationStatus
// (deserializers.go's awsRestjson1_deserializeOpDocumentGetResourceConfigurationOutput)
// but ResourceConfiguration/resourceConfigurationToJSON had no fields for
// any of the three -- a real client's typed fields were always nil/false
// regardless of backend state.
func TestResourceConfiguration_DomainVerificationArnStatusAndAmazonManaged(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	dv, err := client.StartDomainVerification(ctx, &vpclatticesdk.StartDomainVerificationInput{
		DomainName: aws.String("verify.example.com"),
	})
	require.NoError(t, err)

	created, err := client.CreateResourceConfiguration(
		ctx,
		&vpclatticesdk.CreateResourceConfigurationInput{
			Name:                         aws.String("rc-domainverification"),
			Type:                         vpclatticetypes.ResourceConfigurationTypeSingle,
			DomainVerificationIdentifier: dv.Id,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(dv.Arn), aws.ToString(created.DomainVerificationArn))

	got, err := client.GetResourceConfiguration(ctx, &vpclatticesdk.GetResourceConfigurationInput{
		ResourceConfigurationIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(got.AmazonManaged))
	assert.Equal(t, aws.ToString(dv.Arn), aws.ToString(got.DomainVerificationArn))
	assert.Equal(t, vpclatticetypes.VerificationStatusPending, got.DomainVerificationStatus)
}

// TestGetResourceGateway_ServiceManaged drives CreateResourceGateway/
// GetResourceGateway through the real SDK client. GetResourceGatewayOutput
// carries serviceManaged (deserializers.go's
// awsRestjson1_deserializeOpDocumentGetResourceGatewayOutput) but
// ResourceGateway had no field for it and the handler never emitted the
// key -- a real client's typed field was always nil regardless of backend
// state.
func TestGetResourceGateway_ServiceManaged(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateResourceGateway(ctx, &vpclatticesdk.CreateResourceGatewayInput{
		Name:          aws.String("gw-servicemanaged"),
		VpcIdentifier: aws.String("vpc-123"),
	})
	require.NoError(t, err)

	got, err := client.GetResourceGateway(ctx, &vpclatticesdk.GetResourceGatewayInput{
		ResourceGatewayIdentifier: created.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, got.ServiceManaged, "ServiceManaged must round-trip under its real wire key")
	assert.False(t, aws.ToBool(got.ServiceManaged))
}
