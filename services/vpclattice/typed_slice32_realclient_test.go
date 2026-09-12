package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func newSlice32Client(t *testing.T) *vpclatticesdk.Client {
	t.Helper()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")

	return newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
}

// TestSlice32VPCLattice_ServiceNetworkLifecycle drives Get/UpdateServiceNetwork.
func TestSlice32VPCLattice_ServiceNetworkLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	created, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-slice32"),
	})
	require.NoError(t, err)

	got, err := client.GetServiceNetwork(ctx, &vpclatticesdk.GetServiceNetworkInput{
		ServiceNetworkIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "sn-slice32", aws.ToString(got.Name))
	assert.EqualValues(t, 0, aws.ToInt64(got.NumberOfAssociatedServices))

	updated, err := client.UpdateServiceNetwork(ctx, &vpclatticesdk.UpdateServiceNetworkInput{
		ServiceNetworkIdentifier: created.Id,
		AuthType:                 vpclatticetypes.AuthTypeAwsIam,
	})
	require.NoError(t, err)
	assert.Equal(t, vpclatticetypes.AuthTypeAwsIam, updated.AuthType)

	got, err = client.GetServiceNetwork(ctx, &vpclatticesdk.GetServiceNetworkInput{
		ServiceNetworkIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, vpclatticetypes.AuthTypeAwsIam, got.AuthType)
}

// TestSlice32VPCLattice_ServiceNetworkServiceAssociationLifecycle drives
// Get/DeleteServiceNetworkServiceAssociation.
func TestSlice32VPCLattice_ServiceNetworkServiceAssociationLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-snsa"),
	})
	require.NoError(t, err)

	svc, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{Name: aws.String("svc-snsa")})
	require.NoError(t, err)

	assoc, err := client.CreateServiceNetworkServiceAssociation(
		ctx,
		&vpclatticesdk.CreateServiceNetworkServiceAssociationInput{
			ServiceNetworkIdentifier: sn.Id,
			ServiceIdentifier:        svc.Id,
		},
	)
	require.NoError(t, err)

	got, err := client.GetServiceNetworkServiceAssociation(
		ctx,
		&vpclatticesdk.GetServiceNetworkServiceAssociationInput{
			ServiceNetworkServiceAssociationIdentifier: assoc.Id,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "svc-snsa", aws.ToString(got.ServiceName))
	assert.Equal(t, "sn-snsa", aws.ToString(got.ServiceNetworkName))

	listed, err := client.ListServiceNetworkServiceAssociations(
		ctx,
		&vpclatticesdk.ListServiceNetworkServiceAssociationsInput{ServiceNetworkIdentifier: sn.Id},
	)
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	assert.Equal(t, aws.ToString(assoc.Id), aws.ToString(listed.Items[0].Id))

	_, err = client.DeleteServiceNetworkServiceAssociation(
		ctx,
		&vpclatticesdk.DeleteServiceNetworkServiceAssociationInput{
			ServiceNetworkServiceAssociationIdentifier: assoc.Id,
		},
	)
	require.NoError(t, err)

	_, err = client.GetServiceNetworkServiceAssociation(
		ctx,
		&vpclatticesdk.GetServiceNetworkServiceAssociationInput{
			ServiceNetworkServiceAssociationIdentifier: assoc.Id,
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestSlice32VPCLattice_ServiceNetworkVpcAssociationLifecycle drives
// Get/Update/DeleteServiceNetworkVpcAssociation.
func TestSlice32VPCLattice_ServiceNetworkVpcAssociationLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-snva"),
	})
	require.NoError(t, err)

	created, err := client.CreateServiceNetworkVpcAssociation(
		ctx,
		&vpclatticesdk.CreateServiceNetworkVpcAssociationInput{
			ServiceNetworkIdentifier: sn.Id,
			VpcIdentifier:            aws.String("vpc-123"),
			SecurityGroupIds:         []string{"sg-1"},
		},
	)
	require.NoError(t, err)

	got, err := client.GetServiceNetworkVpcAssociation(
		ctx,
		&vpclatticesdk.GetServiceNetworkVpcAssociationInput{
			ServiceNetworkVpcAssociationIdentifier: created.Id,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "vpc-123", aws.ToString(got.VpcId))
	assert.Equal(t, []string{"sg-1"}, got.SecurityGroupIds)

	listed, err := client.ListServiceNetworkVpcAssociations(
		ctx,
		&vpclatticesdk.ListServiceNetworkVpcAssociationsInput{ServiceNetworkIdentifier: sn.Id},
	)
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(listed.Items[0].Id))

	updated, err := client.UpdateServiceNetworkVpcAssociation(
		ctx,
		&vpclatticesdk.UpdateServiceNetworkVpcAssociationInput{
			ServiceNetworkVpcAssociationIdentifier: created.Id,
			SecurityGroupIds:                       []string{"sg-2", "sg-3"},
		},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sg-2", "sg-3"}, updated.SecurityGroupIds)

	_, err = client.DeleteServiceNetworkVpcAssociation(
		ctx,
		&vpclatticesdk.DeleteServiceNetworkVpcAssociationInput{
			ServiceNetworkVpcAssociationIdentifier: created.Id,
		},
	)
	require.NoError(t, err)

	_, err = client.GetServiceNetworkVpcAssociation(
		ctx,
		&vpclatticesdk.GetServiceNetworkVpcAssociationInput{
			ServiceNetworkVpcAssociationIdentifier: created.Id,
		},
	)
	require.Error(t, err)
}

// TestSlice32VPCLattice_ServiceNetworkResourceAssociationLifecycle drives
// Get/ListServiceNetworkResourceAssociations.
func TestSlice32VPCLattice_ServiceNetworkResourceAssociationLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-snra"),
	})
	require.NoError(t, err)

	rc, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name: aws.String("rc-snra"),
		Type: vpclatticetypes.ResourceConfigurationTypeArn,
		ResourceConfigurationDefinition: &vpclatticetypes.ResourceConfigurationDefinitionMemberArnResource{
			Value: vpclatticetypes.ArnResource{
				Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb"),
			},
		},
	})
	require.NoError(t, err)

	assoc, err := client.CreateServiceNetworkResourceAssociation(
		ctx,
		&vpclatticesdk.CreateServiceNetworkResourceAssociationInput{
			ServiceNetworkIdentifier:        sn.Id,
			ResourceConfigurationIdentifier: rc.Id,
		},
	)
	require.NoError(t, err)

	got, err := client.GetServiceNetworkResourceAssociation(
		ctx,
		&vpclatticesdk.GetServiceNetworkResourceAssociationInput{
			ServiceNetworkResourceAssociationIdentifier: assoc.Id,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "rc-snra", aws.ToString(got.ResourceConfigurationName))
	assert.Equal(t, "sn-snra", aws.ToString(got.ServiceNetworkName))

	listed, err := client.ListServiceNetworkResourceAssociations(
		ctx,
		&vpclatticesdk.ListServiceNetworkResourceAssociationsInput{
			ServiceNetworkIdentifier: sn.Id,
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	assert.Equal(t, aws.ToString(assoc.Id), aws.ToString(listed.Items[0].Id))
}

// TestSlice32VPCLattice_ListenerLifecycle drives Get/Update/ListListeners and
// DeleteListener.
func TestSlice32VPCLattice_ListenerLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{Name: aws.String("svc-listener")})
	require.NoError(t, err)

	created, err := client.CreateListener(ctx, &vpclatticesdk.CreateListenerInput{
		ServiceIdentifier: svc.Id,
		Name:              aws.String("listener-slice32"),
		Protocol:          vpclatticetypes.ListenerProtocolHttp,
		DefaultAction: &vpclatticetypes.RuleActionMemberFixedResponse{
			Value: vpclatticetypes.FixedResponseAction{StatusCode: aws.Int32(404)},
		},
	})
	require.NoError(t, err)

	got, err := client.GetListener(ctx, &vpclatticesdk.GetListenerInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "listener-slice32", aws.ToString(got.Name))
	assert.Equal(t, vpclatticetypes.ListenerProtocolHttp, got.Protocol)

	updated, err := client.UpdateListener(ctx, &vpclatticesdk.UpdateListenerInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: created.Id,
		DefaultAction: &vpclatticetypes.RuleActionMemberFixedResponse{
			Value: vpclatticetypes.FixedResponseAction{StatusCode: aws.Int32(410)},
		},
	})
	require.NoError(t, err)

	fr, ok := updated.DefaultAction.(*vpclatticetypes.RuleActionMemberFixedResponse)
	require.True(t, ok)
	assert.EqualValues(t, 410, aws.ToInt32(fr.Value.StatusCode))

	listed, err := client.ListListeners(ctx, &vpclatticesdk.ListListenersInput{ServiceIdentifier: svc.Id})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(listed.Items[0].Id))

	_, err = client.DeleteListener(ctx, &vpclatticesdk.DeleteListenerInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: created.Id,
	})
	require.NoError(t, err)

	_, err = client.GetListener(ctx, &vpclatticesdk.GetListenerInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: created.Id,
	})
	require.Error(t, err)
}

// TestSlice32VPCLattice_RuleLifecycle drives Get/Update/ListRules, DeleteRule
// and BatchUpdateRule.
func TestSlice32VPCLattice_RuleLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{Name: aws.String("svc-rule")})
	require.NoError(t, err)

	listener, err := client.CreateListener(ctx, &vpclatticesdk.CreateListenerInput{
		ServiceIdentifier: svc.Id,
		Name:              aws.String("listener-rule"),
		Protocol:          vpclatticetypes.ListenerProtocolHttp,
		DefaultAction: &vpclatticetypes.RuleActionMemberFixedResponse{
			Value: vpclatticetypes.FixedResponseAction{StatusCode: aws.Int32(404)},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateRule(ctx, &vpclatticesdk.CreateRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		Name:               aws.String("rule-slice32"),
		Priority:           aws.Int32(10),
		Action: &vpclatticetypes.RuleActionMemberFixedResponse{
			Value: vpclatticetypes.FixedResponseAction{StatusCode: aws.Int32(503)},
		},
		Match: &vpclatticetypes.RuleMatchMemberHttpMatch{
			Value: vpclatticetypes.HttpMatch{
				PathMatch: &vpclatticetypes.PathMatch{
					Match: &vpclatticetypes.PathMatchTypeMemberExact{Value: "/health"},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetRule(ctx, &vpclatticesdk.GetRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		RuleIdentifier:     created.Id,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 10, aws.ToInt32(got.Priority))

	updated, err := client.UpdateRule(ctx, &vpclatticesdk.UpdateRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		RuleIdentifier:     created.Id,
		Priority:           aws.Int32(20),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 20, aws.ToInt32(updated.Priority))

	listed, err := client.ListRules(ctx, &vpclatticesdk.ListRulesInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
	})
	require.NoError(t, err)
	require.Len(t, listed.Items, 2, "must include both the default rule and the created rule")

	batchOut, err := client.BatchUpdateRule(ctx, &vpclatticesdk.BatchUpdateRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		Rules: []vpclatticetypes.RuleUpdate{
			{RuleIdentifier: created.Id, Priority: aws.Int32(30)},
			{RuleIdentifier: aws.String("rule-does-not-exist")},
		},
	})
	require.NoError(t, err)
	require.Len(t, batchOut.Successful, 1)
	assert.EqualValues(t, 30, aws.ToInt32(batchOut.Successful[0].Priority))
	require.Len(t, batchOut.Unsuccessful, 1)
	assert.Equal(t, "rule-does-not-exist", aws.ToString(batchOut.Unsuccessful[0].RuleIdentifier))
	assert.NotEmpty(t, aws.ToString(batchOut.Unsuccessful[0].FailureCode))

	_, err = client.DeleteRule(ctx, &vpclatticesdk.DeleteRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		RuleIdentifier:     created.Id,
	})
	require.NoError(t, err)

	_, err = client.GetRule(ctx, &vpclatticesdk.GetRuleInput{
		ServiceIdentifier:  svc.Id,
		ListenerIdentifier: listener.Id,
		RuleIdentifier:     created.Id,
	})
	require.Error(t, err)
}

// TestSlice32VPCLattice_TargetGroupAndTargetsLifecycle drives
// Update/ListTargetGroups, DeleteTargetGroup, Register/Deregister/ListTargets.
func TestSlice32VPCLattice_TargetGroupAndTargetsLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	created, err := client.CreateTargetGroup(ctx, &vpclatticesdk.CreateTargetGroupInput{
		Name: aws.String("tg-slice32"),
		Type: vpclatticetypes.TargetGroupTypeIp,
		Config: &vpclatticetypes.TargetGroupConfig{
			VpcIdentifier: aws.String("vpc-123"),
			Protocol:      vpclatticetypes.TargetGroupProtocolHttp,
			Port:          aws.Int32(80),
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTargetGroups(ctx, &vpclatticesdk.ListTargetGroupsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	assert.Equal(t, aws.ToString(created.Id), aws.ToString(listed.Items[0].Id))

	updated, err := client.UpdateTargetGroup(ctx, &vpclatticesdk.UpdateTargetGroupInput{
		TargetGroupIdentifier: created.Id,
		HealthCheck: &vpclatticetypes.HealthCheckConfig{
			Enabled: aws.Bool(true),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Config)
	require.NotNil(t, updated.Config.HealthCheck)
	assert.True(t, aws.ToBool(updated.Config.HealthCheck.Enabled))

	regOut, err := client.RegisterTargets(ctx, &vpclatticesdk.RegisterTargetsInput{
		TargetGroupIdentifier: created.Id,
		Targets: []vpclatticetypes.Target{
			{Id: aws.String("10.0.0.1"), Port: aws.Int32(8080)},
		},
	})
	require.NoError(t, err)
	require.Len(t, regOut.Successful, 1)
	assert.Equal(t, "10.0.0.1", aws.ToString(regOut.Successful[0].Id))

	targetsListed, err := client.ListTargets(ctx, &vpclatticesdk.ListTargetsInput{
		TargetGroupIdentifier: created.Id,
	})
	require.NoError(t, err)
	require.Len(t, targetsListed.Items, 1)
	assert.Equal(t, "10.0.0.1", aws.ToString(targetsListed.Items[0].Id))
	assert.Equal(t, vpclatticetypes.TargetStatusHealthy, targetsListed.Items[0].Status)

	deregOut, err := client.DeregisterTargets(ctx, &vpclatticesdk.DeregisterTargetsInput{
		TargetGroupIdentifier: created.Id,
		Targets: []vpclatticetypes.Target{
			{Id: aws.String("10.0.0.1"), Port: aws.Int32(8080)},
		},
	})
	require.NoError(t, err)
	require.Len(t, deregOut.Successful, 1)

	targetsListed, err = client.ListTargets(ctx, &vpclatticesdk.ListTargetsInput{
		TargetGroupIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Empty(t, targetsListed.Items)

	_, err = client.DeleteTargetGroup(ctx, &vpclatticesdk.DeleteTargetGroupInput{
		TargetGroupIdentifier: created.Id,
	})
	require.NoError(t, err)

	_, err = client.GetTargetGroup(ctx, &vpclatticesdk.GetTargetGroupInput{
		TargetGroupIdentifier: created.Id,
	})
	require.Error(t, err)
}

// TestSlice32VPCLattice_AccessLogSubscriptionLifecycle drives
// UpdateAccessLogSubscription and DeleteAccessLogSubscription.
func TestSlice32VPCLattice_AccessLogSubscriptionLifecycle(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{Name: aws.String("svc-als")})
	require.NoError(t, err)

	created, err := client.CreateAccessLogSubscription(ctx, &vpclatticesdk.CreateAccessLogSubscriptionInput{
		ResourceIdentifier: svc.Id,
		DestinationArn:     aws.String("arn:aws:s3:::my-log-bucket"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateAccessLogSubscription(ctx, &vpclatticesdk.UpdateAccessLogSubscriptionInput{
		AccessLogSubscriptionIdentifier: created.Id,
		DestinationArn:                  aws.String("arn:aws:s3:::my-other-bucket"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:s3:::my-other-bucket", aws.ToString(updated.DestinationArn))

	_, err = client.DeleteAccessLogSubscription(ctx, &vpclatticesdk.DeleteAccessLogSubscriptionInput{
		AccessLogSubscriptionIdentifier: created.Id,
	})
	require.NoError(t, err)

	_, err = client.GetAccessLogSubscription(ctx, &vpclatticesdk.GetAccessLogSubscriptionInput{
		AccessLogSubscriptionIdentifier: created.Id,
	})
	require.Error(t, err)
}

// TestSlice32VPCLattice_ResourcePolicy drives Get/PutResourcePolicy and
// DeleteResourcePolicy.
func TestSlice32VPCLattice_ResourcePolicy(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-respolicy"),
	})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17",` +
		`"Statement":[{"Effect":"Allow","Principal":"*","Action":"vpc-lattice:*","Resource":"*"}]}`

	_, err = client.PutResourcePolicy(ctx, &vpclatticesdk.PutResourcePolicyInput{
		ResourceArn: sn.Arn,
		Policy:      aws.String(policy),
	})
	require.NoError(t, err)

	got, err := client.GetResourcePolicy(ctx, &vpclatticesdk.GetResourcePolicyInput{ResourceArn: sn.Arn})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(got.Policy))

	_, err = client.DeleteResourcePolicy(ctx, &vpclatticesdk.DeleteResourcePolicyInput{ResourceArn: sn.Arn})
	require.NoError(t, err)

	_, err = client.GetResourcePolicy(ctx, &vpclatticesdk.GetResourcePolicyInput{ResourceArn: sn.Arn})
	require.Error(t, err)
}

// TestSlice32VPCLattice_AuthPolicyDelete drives DeleteAuthPolicy.
func TestSlice32VPCLattice_AuthPolicyDelete(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-authpolicy"),
	})
	require.NoError(t, err)

	_, err = client.PutAuthPolicy(ctx, &vpclatticesdk.PutAuthPolicyInput{
		ResourceIdentifier: sn.Arn,
		Policy:             aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.GetAuthPolicy(ctx, &vpclatticesdk.GetAuthPolicyInput{ResourceIdentifier: sn.Arn})
	require.NoError(t, err)

	_, err = client.DeleteAuthPolicy(ctx, &vpclatticesdk.DeleteAuthPolicyInput{ResourceIdentifier: sn.Arn})
	require.NoError(t, err)

	_, err = client.GetAuthPolicy(ctx, &vpclatticesdk.GetAuthPolicyInput{ResourceIdentifier: sn.Arn})
	require.Error(t, err)
}

// TestSlice32VPCLattice_ResourceConfigurationUpdate drives
// UpdateResourceConfiguration.
func TestSlice32VPCLattice_ResourceConfigurationUpdate(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	created, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name: aws.String("rc-update"),
		Type: vpclatticetypes.ResourceConfigurationTypeArn,
		ResourceConfigurationDefinition: &vpclatticetypes.ResourceConfigurationDefinitionMemberArnResource{
			Value: vpclatticetypes.ArnResource{
				Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb"),
			},
		},
	})
	require.NoError(t, err)

	updated, err := client.UpdateResourceConfiguration(ctx, &vpclatticesdk.UpdateResourceConfigurationInput{
		ResourceConfigurationIdentifier: created.Id,
		PortRanges:                      []string{"443"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"443"}, updated.PortRanges)

	got, err := client.GetResourceConfiguration(ctx, &vpclatticesdk.GetResourceConfigurationInput{
		ResourceConfigurationIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"443"}, got.PortRanges)
}

// TestSlice32VPCLattice_ResourceGatewayUpdate drives UpdateResourceGateway.
func TestSlice32VPCLattice_ResourceGatewayUpdate(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	created, err := client.CreateResourceGateway(ctx, &vpclatticesdk.CreateResourceGatewayInput{
		Name:          aws.String("gw-update"),
		VpcIdentifier: aws.String("vpc-123"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateResourceGateway(ctx, &vpclatticesdk.UpdateResourceGatewayInput{
		ResourceGatewayIdentifier: created.Id,
		SecurityGroupIds:          []string{"sg-9"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-9"}, updated.SecurityGroupIds)

	got, err := client.GetResourceGateway(ctx, &vpclatticesdk.GetResourceGatewayInput{
		ResourceGatewayIdentifier: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-9"}, got.SecurityGroupIds)
}

// TestSlice32VPCLattice_ServiceUpdate drives UpdateService.
func TestSlice32VPCLattice_ServiceUpdate(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	created, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{Name: aws.String("svc-update")})
	require.NoError(t, err)

	updated, err := client.UpdateService(ctx, &vpclatticesdk.UpdateServiceInput{
		ServiceIdentifier: created.Id,
		AuthType:          vpclatticetypes.AuthTypeAwsIam,
	})
	require.NoError(t, err)
	assert.Equal(t, vpclatticetypes.AuthTypeAwsIam, updated.AuthType)

	got, err := client.GetService(ctx, &vpclatticesdk.GetServiceInput{ServiceIdentifier: created.Id})
	require.NoError(t, err)
	assert.Equal(t, vpclatticetypes.AuthTypeAwsIam, got.AuthType)
}

// TestSlice32VPCLattice_DomainVerificationsList drives ListDomainVerifications.
func TestSlice32VPCLattice_DomainVerificationsList(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	_, err := client.StartDomainVerification(ctx, &vpclatticesdk.StartDomainVerificationInput{
		DomainName: aws.String("one.example.com"),
	})
	require.NoError(t, err)

	_, err = client.StartDomainVerification(ctx, &vpclatticesdk.StartDomainVerificationInput{
		DomainName: aws.String("two.example.com"),
	})
	require.NoError(t, err)

	listed, err := client.ListDomainVerifications(ctx, &vpclatticesdk.ListDomainVerificationsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Items, 2)

	names := []string{aws.ToString(listed.Items[0].DomainName), aws.ToString(listed.Items[1].DomainName)}
	assert.ElementsMatch(t, []string{"one.example.com", "two.example.com"}, names)
}

// TestSlice32VPCLattice_ResourceTags drives TagResource, UntagResource and
// ListTagsForResource against a real service network ARN.
func TestSlice32VPCLattice_ResourceTags(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-tags"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &vpclatticesdk.TagResourceInput{
		ResourceArn: sn.Arn,
		Tags:        map[string]string{"team": "net"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(ctx, &vpclatticesdk.ListTagsForResourceInput{ResourceArn: sn.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "net"}, got.Tags)

	_, err = client.UntagResource(ctx, &vpclatticesdk.UntagResourceInput{
		ResourceArn: sn.Arn,
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	got, err = client.ListTagsForResource(ctx, &vpclatticesdk.ListTagsForResourceInput{ResourceArn: sn.Arn})
	require.NoError(t, err)
	assert.Empty(t, got.Tags)
}

// TestSlice32VPCLattice_ResourceEndpointAssociations_AlwaysEmpty drives
// List/DeleteResourceEndpointAssociation and
// ListServiceNetworkVpcEndpointAssociations. Both families are populated in
// real AWS exclusively by EC2 CreateVpcEndpoint calls -- this backend has no
// such cross-service integration, so the honest result is always an empty,
// correctly-shaped list (see service_network_resource_associations.go's
// family doc comment).
func TestSlice32VPCLattice_ResourceEndpointAssociations_AlwaysEmpty(t *testing.T) {
	t.Parallel()

	client := newSlice32Client(t)
	ctx := t.Context()

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-reassoc"),
	})
	require.NoError(t, err)

	rc, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name: aws.String("rc-reassoc"),
		Type: vpclatticetypes.ResourceConfigurationTypeArn,
		ResourceConfigurationDefinition: &vpclatticetypes.ResourceConfigurationDefinitionMemberArnResource{
			Value: vpclatticetypes.ArnResource{
				Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb"),
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListResourceEndpointAssociations(
		ctx,
		&vpclatticesdk.ListResourceEndpointAssociationsInput{ResourceConfigurationIdentifier: rc.Id},
	)
	require.NoError(t, err)
	assert.Empty(t, listed.Items)

	_, err = client.DeleteResourceEndpointAssociation(
		ctx,
		&vpclatticesdk.DeleteResourceEndpointAssociationInput{
			ResourceEndpointAssociationIdentifier: aws.String("rea-no-such"),
		},
	)
	require.Error(t, err)

	snvea, err := client.ListServiceNetworkVpcEndpointAssociations(
		ctx,
		&vpclatticesdk.ListServiceNetworkVpcEndpointAssociationsInput{
			ServiceNetworkIdentifier: sn.Id,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, snvea.Items)
}
