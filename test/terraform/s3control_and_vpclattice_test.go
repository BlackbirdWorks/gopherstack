package terraform_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3controlsvc "github.com/aws/aws-sdk-go-v2/service/s3control"
	vpclatticesvc "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_S3controlAndVpclattice provisions S3 Control (access grants instance +
// resource policy + location + grant, access point policy, multi-region
// access point + policy, object Lambda access point + policy, storage lens
// configuration) and VPC Lattice (service, target group + attachment,
// listener + rule, auth policy, resource policy, access log subscription,
// service network VPC/service associations, resource gateway, resource
// configuration + service network resource association) resources via
// Terraform and verifies each through its own SDK client's Get/Describe path.
func TestTerraform_S3controlAndVpclattice(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "s3control-and-vpclattice",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				functionZip := filepath.Join(dir, "s3vl-ol-function.zip")
				writeZipFixture(t, functionZip, "index.py",
					"def handler(event, context):\n    return {'statusCode': 200}\n")

				return map[string]any{
					"FunctionZip": functionZip,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyS3controlAndVpclatticeS3Control(ctx, t)
				verifyS3controlAndVpclatticeVPCLattice(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyS3controlAndVpclatticeS3Control(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createS3ControlClient(t)
	acctID := "000000000000"

	instOut, err := client.GetAccessGrantsInstance(ctx, &s3controlsvc.GetAccessGrantsInstanceInput{
		AccountId: aws.String(acctID),
	})
	require.NoError(t, err, "GetAccessGrantsInstance should succeed")
	require.NotNil(t, instOut.AccessGrantsInstanceArn)

	polOut, err := client.GetAccessGrantsInstanceResourcePolicy(
		ctx,
		&s3controlsvc.GetAccessGrantsInstanceResourcePolicyInput{
			AccountId: aws.String(acctID),
		},
	)
	require.NoError(t, err, "GetAccessGrantsInstanceResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(polOut.Policy), "AllowAccessToS3AccessGrants")

	listLocOut, err := client.ListAccessGrantsLocations(
		ctx,
		&s3controlsvc.ListAccessGrantsLocationsInput{
			AccountId: aws.String(acctID),
		},
	)
	require.NoError(t, err, "ListAccessGrantsLocations should succeed")

	var locationID string

	for _, l := range listLocOut.AccessGrantsLocationsList {
		if aws.ToString(l.IAMRoleArn) != "" {
			locationID = aws.ToString(l.AccessGrantsLocationId)
		}
	}

	require.NotEmpty(t, locationID, "access grants location should be listed")

	locOut, err := client.GetAccessGrantsLocation(ctx, &s3controlsvc.GetAccessGrantsLocationInput{
		AccountId:              aws.String(acctID),
		AccessGrantsLocationId: aws.String(locationID),
	})
	require.NoError(t, err, "GetAccessGrantsLocation should succeed")
	assert.Equal(t, locationID, aws.ToString(locOut.AccessGrantsLocationId))

	listGrantsOut, err := client.ListAccessGrants(ctx, &s3controlsvc.ListAccessGrantsInput{
		AccountId: aws.String(acctID),
		GranteeIdentifier: aws.String(
			"arn:aws:iam::000000000000:role/s3vl-access-grants-role",
		),
	})
	require.NoError(t, err, "ListAccessGrants should succeed")
	require.NotEmpty(t, listGrantsOut.AccessGrantsList)

	grantID := aws.ToString(listGrantsOut.AccessGrantsList[0].AccessGrantId)

	grantOut, err := client.GetAccessGrant(ctx, &s3controlsvc.GetAccessGrantInput{
		AccountId:     aws.String(acctID),
		AccessGrantId: aws.String(grantID),
	})
	require.NoError(t, err, "GetAccessGrant should succeed")
	assert.Equal(t, "READ", string(grantOut.Permission))

	apPolOut, err := client.GetAccessPointPolicy(ctx, &s3controlsvc.GetAccessPointPolicyInput{
		AccountId: aws.String(acctID),
		Name:      aws.String("s3vl-ap"),
	})
	require.NoError(t, err, "GetAccessPointPolicy should succeed")
	assert.Contains(t, aws.ToString(apPolOut.Policy), "GetObjectTagging")

	mrapOut, err := client.GetMultiRegionAccessPoint(
		ctx,
		&s3controlsvc.GetMultiRegionAccessPointInput{
			AccountId: aws.String(acctID),
			Name:      aws.String("s3vl-mrap"),
		},
	)
	require.NoError(t, err, "GetMultiRegionAccessPoint should succeed")
	require.NotNil(t, mrapOut.AccessPoint)

	mrapPolOut, err := client.GetMultiRegionAccessPointPolicy(
		ctx,
		&s3controlsvc.GetMultiRegionAccessPointPolicyInput{
			AccountId: aws.String(acctID),
			Name:      aws.String("s3vl-mrap"),
		},
	)
	require.NoError(t, err, "GetMultiRegionAccessPointPolicy should succeed")
	require.NotNil(t, mrapPolOut.Policy)

	olOut, err := client.GetAccessPointForObjectLambda(
		ctx,
		&s3controlsvc.GetAccessPointForObjectLambdaInput{
			AccountId: aws.String(acctID),
			Name:      aws.String("s3vl-ol"),
		},
	)
	require.NoError(t, err, "GetAccessPointForObjectLambda should succeed")
	assert.Equal(t, "s3vl-ol", aws.ToString(olOut.Name))

	olPolOut, err := client.GetAccessPointPolicyForObjectLambda(
		ctx,
		&s3controlsvc.GetAccessPointPolicyForObjectLambdaInput{
			AccountId: aws.String(acctID),
			Name:      aws.String("s3vl-ol"),
		},
	)
	require.NoError(t, err, "GetAccessPointPolicyForObjectLambda should succeed")
	assert.Contains(t, aws.ToString(olPolOut.Policy), "s3-object-lambda:GetObject")

	lensOut, err := client.GetStorageLensConfiguration(
		ctx,
		&s3controlsvc.GetStorageLensConfigurationInput{
			AccountId: aws.String(acctID),
			ConfigId:  aws.String("s3vl-lens"),
		},
	)
	require.NoError(t, err, "GetStorageLensConfiguration should succeed")
	require.NotNil(t, lensOut.StorageLensConfiguration)
	assert.True(t, lensOut.StorageLensConfiguration.IsEnabled)
}

func verifyS3controlAndVpclatticeVPCLattice(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createVPCLatticeClient(t)

	svcListOut, err := client.ListServices(ctx, &vpclatticesvc.ListServicesInput{})
	require.NoError(t, err, "ListServices should succeed")

	var serviceID string

	for _, s := range svcListOut.Items {
		if aws.ToString(s.Name) == "s3vl-service" {
			serviceID = aws.ToString(s.Id)
		}
	}

	require.NotEmpty(t, serviceID, "service should be listed")

	svcOut, err := client.GetService(ctx, &vpclatticesvc.GetServiceInput{
		ServiceIdentifier: aws.String(serviceID),
	})
	require.NoError(t, err, "GetService should succeed")
	assert.Equal(t, "AWS_IAM", string(svcOut.AuthType))

	tgListOut, err := client.ListTargetGroups(ctx, &vpclatticesvc.ListTargetGroupsInput{})
	require.NoError(t, err, "ListTargetGroups should succeed")

	var targetGroupID string

	for _, tg := range tgListOut.Items {
		if aws.ToString(tg.Name) == "s3vl-target-group" {
			targetGroupID = aws.ToString(tg.Id)
		}
	}

	require.NotEmpty(t, targetGroupID, "target group should be listed")

	tgOut, err := client.GetTargetGroup(ctx, &vpclatticesvc.GetTargetGroupInput{
		TargetGroupIdentifier: aws.String(targetGroupID),
	})
	require.NoError(t, err, "GetTargetGroup should succeed")
	require.NotNil(t, tgOut.Config)

	targetsOut, err := client.ListTargets(ctx, &vpclatticesvc.ListTargetsInput{
		TargetGroupIdentifier: aws.String(targetGroupID),
	})
	require.NoError(t, err, "ListTargets should succeed")
	require.Len(t, targetsOut.Items, 1)
	assert.Equal(t, "10.121.1.10", aws.ToString(targetsOut.Items[0].Id))

	listenersOut, err := client.ListListeners(ctx, &vpclatticesvc.ListListenersInput{
		ServiceIdentifier: aws.String(serviceID),
	})
	require.NoError(t, err, "ListListeners should succeed")
	require.Len(t, listenersOut.Items, 1)

	listenerID := aws.ToString(listenersOut.Items[0].Id)

	listenerOut, err := client.GetListener(ctx, &vpclatticesvc.GetListenerInput{
		ServiceIdentifier:  aws.String(serviceID),
		ListenerIdentifier: aws.String(listenerID),
	})
	require.NoError(t, err, "GetListener should succeed")
	require.NotNil(t, listenerOut.DefaultAction)

	rulesOut, err := client.ListRules(ctx, &vpclatticesvc.ListRulesInput{
		ServiceIdentifier:  aws.String(serviceID),
		ListenerIdentifier: aws.String(listenerID),
	})
	require.NoError(t, err, "ListRules should succeed")

	var ruleID string

	for _, r := range rulesOut.Items {
		if aws.ToString(r.Name) == "s3vl-listener-rule" {
			ruleID = aws.ToString(r.Id)
		}
	}

	require.NotEmpty(t, ruleID, "listener rule should be listed")

	ruleOut, err := client.GetRule(ctx, &vpclatticesvc.GetRuleInput{
		ServiceIdentifier:  aws.String(serviceID),
		ListenerIdentifier: aws.String(listenerID),
		RuleIdentifier:     aws.String(ruleID),
	})
	require.NoError(t, err, "GetRule should succeed")
	require.NotNil(t, ruleOut.Match)

	authOut, err := client.GetAuthPolicy(ctx, &vpclatticesvc.GetAuthPolicyInput{
		ResourceIdentifier: aws.String(serviceID),
	})
	require.NoError(t, err, "GetAuthPolicy should succeed")
	assert.NotNil(t, authOut.Policy)

	snListOut, err := client.ListServiceNetworks(ctx, &vpclatticesvc.ListServiceNetworksInput{})
	require.NoError(t, err, "ListServiceNetworks should succeed")

	var serviceNetworkID string

	for _, sn := range snListOut.Items {
		if aws.ToString(sn.Name) == "s3vl-service-network" {
			serviceNetworkID = aws.ToString(sn.Id)
		}
	}

	require.NotEmpty(t, serviceNetworkID, "service network should be listed")

	resPolOut, err := client.GetResourcePolicy(ctx, &vpclatticesvc.GetResourcePolicyInput{
		ResourceArn: aws.String(
			"arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/" + serviceNetworkID,
		),
	})
	if err != nil {
		// Some resource-policy implementations key on the identifier alone;
		// fall back to it if the constructed ARN doesn't resolve.
		resPolOut, err = client.GetResourcePolicy(ctx, &vpclatticesvc.GetResourcePolicyInput{
			ResourceArn: aws.String(serviceNetworkID),
		})
	}
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.NotNil(t, resPolOut.Policy)

	alsListOut, err := client.ListAccessLogSubscriptions(
		ctx,
		&vpclatticesvc.ListAccessLogSubscriptionsInput{
			ResourceIdentifier: aws.String(serviceNetworkID),
		},
	)
	require.NoError(t, err, "ListAccessLogSubscriptions should succeed")
	require.Len(t, alsListOut.Items, 1)

	alsOut, err := client.GetAccessLogSubscription(
		ctx,
		&vpclatticesvc.GetAccessLogSubscriptionInput{
			AccessLogSubscriptionIdentifier: alsListOut.Items[0].Id,
		},
	)
	require.NoError(t, err, "GetAccessLogSubscription should succeed")
	assert.Contains(t, aws.ToString(alsOut.DestinationArn), "s3vl-access-logs")

	vpcAssocOut, err := client.ListServiceNetworkVpcAssociations(
		ctx,
		&vpclatticesvc.ListServiceNetworkVpcAssociationsInput{
			ServiceNetworkIdentifier: aws.String(serviceNetworkID),
		},
	)
	require.NoError(t, err, "ListServiceNetworkVpcAssociations should succeed")
	require.Len(t, vpcAssocOut.Items, 1)

	vpcAssocGetOut, err := client.GetServiceNetworkVpcAssociation(
		ctx,
		&vpclatticesvc.GetServiceNetworkVpcAssociationInput{
			ServiceNetworkVpcAssociationIdentifier: vpcAssocOut.Items[0].Id,
		},
	)
	require.NoError(t, err, "GetServiceNetworkVpcAssociation should succeed")
	require.NotNil(t, vpcAssocGetOut.Status)

	svcAssocOut, err := client.ListServiceNetworkServiceAssociations(
		ctx,
		&vpclatticesvc.ListServiceNetworkServiceAssociationsInput{
			ServiceNetworkIdentifier: aws.String(serviceNetworkID),
		},
	)
	require.NoError(t, err, "ListServiceNetworkServiceAssociations should succeed")
	require.Len(t, svcAssocOut.Items, 1)

	svcAssocGetOut, err := client.GetServiceNetworkServiceAssociation(
		ctx,
		&vpclatticesvc.GetServiceNetworkServiceAssociationInput{
			ServiceNetworkServiceAssociationIdentifier: svcAssocOut.Items[0].Id,
		},
	)
	require.NoError(t, err, "GetServiceNetworkServiceAssociation should succeed")
	require.NotNil(t, svcAssocGetOut.Status)

	rgListOut, err := client.ListResourceGateways(ctx, &vpclatticesvc.ListResourceGatewaysInput{})
	require.NoError(t, err, "ListResourceGateways should succeed")

	var resourceGatewayID string

	for _, rg := range rgListOut.Items {
		if aws.ToString(rg.Name) == "s3vl-resource-gateway" {
			resourceGatewayID = aws.ToString(rg.Id)
		}
	}

	require.NotEmpty(t, resourceGatewayID, "resource gateway should be listed")

	rgOut, err := client.GetResourceGateway(ctx, &vpclatticesvc.GetResourceGatewayInput{
		ResourceGatewayIdentifier: aws.String(resourceGatewayID),
	})
	require.NoError(t, err, "GetResourceGateway should succeed")
	require.NotNil(t, rgOut.Status)

	rcListOut, err := client.ListResourceConfigurations(
		ctx,
		&vpclatticesvc.ListResourceConfigurationsInput{},
	)
	require.NoError(t, err, "ListResourceConfigurations should succeed")

	var resourceConfigID string

	for _, rc := range rcListOut.Items {
		if aws.ToString(rc.Name) == "s3vl-resource-configuration" {
			resourceConfigID = aws.ToString(rc.Id)
		}
	}

	require.NotEmpty(t, resourceConfigID, "resource configuration should be listed")

	rcOut, err := client.GetResourceConfiguration(ctx, &vpclatticesvc.GetResourceConfigurationInput{
		ResourceConfigurationIdentifier: aws.String(resourceConfigID),
	})
	require.NoError(t, err, "GetResourceConfiguration should succeed")
	require.NotNil(t, rcOut.ResourceConfigurationDefinition)

	snraOut, err := client.ListServiceNetworkResourceAssociations(
		ctx,
		&vpclatticesvc.ListServiceNetworkResourceAssociationsInput{
			ServiceNetworkIdentifier: aws.String(serviceNetworkID),
		},
	)
	require.NoError(t, err, "ListServiceNetworkResourceAssociations should succeed")
	require.Len(t, snraOut.Items, 1)

	snraGetOut, err := client.GetServiceNetworkResourceAssociation(
		ctx,
		&vpclatticesvc.GetServiceNetworkResourceAssociationInput{
			ServiceNetworkResourceAssociationIdentifier: snraOut.Items[0].Id,
		},
	)
	require.NoError(t, err, "GetServiceNetworkResourceAssociation should succeed")
	require.NotNil(t, snraGetOut.Status)
}
