package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsvc48 "github.com/aws/aws-sdk-go-v2/service/appstream"
	opsworkssvc48 "github.com/aws/aws-sdk-go-v2/service/opsworks"
	organizationssvc48 "github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_OrganizationsAndAppstream provisions an OpsWorks stack with every "canned"
// layer type (ECS cluster, Ganglia, HAProxy, Java App, Memcached, MySQL,
// Node.js App, PHP App, Rails App, Static Web), an instance, and an RDS DB
// instance registration; an AppStream directory config, fleet(+stack
// association), image builder, user, and user/stack association; and an
// Organizations account, delegated administrator, policy(+attachment), and
// resource-based delegation policy, via Terraform, verifying each through
// its own SDK client.
func TestTerraform_OrganizationsAndAppstream(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "organizations-and-appstream",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Endpoint": endpoint}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyOrganizationsAndAppstreamOpsWorks(ctx, t)
				verifyOrganizationsAndAppstreamAppstream(ctx, t)
				verifyOrganizationsAndAppstreamOrganizations(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lockOrganizations(t)
			runTFTest(t, tc)
		})
	}
}

func verifyOrganizationsAndAppstreamOpsWorks(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := opsworkssvc48.NewFromConfig(cfg, func(o *opsworkssvc48.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	stacksOut, err := client.DescribeStacks(ctx, &opsworkssvc48.DescribeStacksInput{})
	require.NoError(t, err, "DescribeStacks should succeed")

	var stackID string

	for _, s := range stacksOut.Stacks {
		if aws.ToString(s.Name) == "orap-stack" {
			stackID = aws.ToString(s.StackId)
		}
	}

	require.NotEmpty(t, stackID, "orap opsworks stack should be listed")

	layersOut, err := client.DescribeLayers(ctx, &opsworkssvc48.DescribeLayersInput{StackId: aws.String(stackID)})
	require.NoError(t, err, "DescribeLayers should succeed")

	wantTypes := map[string]bool{
		"ecs-cluster": false, "monitoring-master": false, "lb": false, "java-app": false,
		"memcached": false, "db-master": false, "nodejs-app": false, "php-app": false,
		"rails-app": false, "web": false,
	}

	var railsLayerID string

	for _, l := range layersOut.Layers {
		wantTypes[string(l.Type)] = true

		if l.Type == "rails-app" {
			railsLayerID = aws.ToString(l.LayerId)
		}

		if l.Type == "ecs-cluster" {
			assert.Equal(
				t,
				"arn:aws:ecs:us-east-1:000000000000:cluster/orap-cluster",
				l.Attributes["EcsClusterArn"],
				"ecs cluster layer should echo its EcsClusterArn attribute",
			)
		}
	}

	for typ, found := range wantTypes {
		assert.True(t, found, "layer type %s should be listed", typ)
	}

	require.NotEmpty(t, railsLayerID, "rails-app layer should be listed")

	instOut, err := client.DescribeInstances(ctx, &opsworkssvc48.DescribeInstancesInput{StackId: aws.String(stackID)})
	require.NoError(t, err, "DescribeInstances should succeed")
	require.Len(t, instOut.Instances, 1)
	assert.Contains(t, instOut.Instances[0].LayerIds, railsLayerID)
	assert.Equal(t, "m5.large", aws.ToString(instOut.Instances[0].InstanceType))

	rdsOut, err := client.DescribeRdsDbInstances(
		ctx,
		&opsworkssvc48.DescribeRdsDbInstancesInput{StackId: aws.String(stackID)},
	)
	require.NoError(t, err, "DescribeRdsDbInstances should succeed")
	require.Len(t, rdsOut.RdsDbInstances, 1)
	assert.Equal(t, "orapadmin", aws.ToString(rdsOut.RdsDbInstances[0].DbUser))
}

func verifyOrganizationsAndAppstreamAppstream(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := appstreamsvc48.NewFromConfig(cfg, func(o *appstreamsvc48.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	dcOut, err := client.DescribeDirectoryConfigs(ctx, &appstreamsvc48.DescribeDirectoryConfigsInput{
		DirectoryNames: []string{"orap.example.test"},
	})
	require.NoError(t, err, "DescribeDirectoryConfigs should succeed")
	require.Len(t, dcOut.DirectoryConfigs, 1)

	fleetsOut, err := client.DescribeFleets(ctx, &appstreamsvc48.DescribeFleetsInput{
		Names: []string{"orap-fleet"},
	})
	require.NoError(t, err, "DescribeFleets should succeed")
	require.Len(t, fleetsOut.Fleets, 1)
	assert.Equal(t, "stream.standard.medium", aws.ToString(fleetsOut.Fleets[0].InstanceType))

	assocOut, err := client.ListAssociatedStacks(ctx, &appstreamsvc48.ListAssociatedStacksInput{
		FleetName: aws.String("orap-fleet"),
	})
	require.NoError(t, err, "ListAssociatedStacks should succeed")
	assert.Contains(t, assocOut.Names, "orap-stack")

	ibOut, err := client.DescribeImageBuilders(ctx, &appstreamsvc48.DescribeImageBuildersInput{
		Names: []string{"orap-image-builder"},
	})
	require.NoError(t, err, "DescribeImageBuilders should succeed")
	require.Len(t, ibOut.ImageBuilders, 1)
	assert.Equal(t, "RUNNING", string(ibOut.ImageBuilders[0].State))

	usersOut, err := client.DescribeUsers(ctx, &appstreamsvc48.DescribeUsersInput{
		AuthenticationType: "USERPOOL",
	})
	require.NoError(t, err, "DescribeUsers should succeed")

	var foundUser bool

	for _, u := range usersOut.Users {
		if aws.ToString(u.UserName) == "orap-user@example.test" {
			foundUser = true
		}
	}

	assert.True(t, foundUser, "orap appstream user should be listed")

	usaOut, err := client.DescribeUserStackAssociations(ctx, &appstreamsvc48.DescribeUserStackAssociationsInput{
		StackName:          aws.String("orap-stack"),
		UserName:           aws.String("orap-user@example.test"),
		AuthenticationType: "USERPOOL",
	})
	require.NoError(t, err, "DescribeUserStackAssociations should succeed")
	require.Len(t, usaOut.UserStackAssociations, 1)
}

func verifyOrganizationsAndAppstreamOrganizations(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := organizationssvc48.NewFromConfig(cfg, func(o *organizationssvc48.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	acctsOut, err := client.ListAccounts(ctx, &organizationssvc48.ListAccountsInput{})
	require.NoError(t, err, "ListAccounts should succeed")

	var accountID string

	for _, a := range acctsOut.Accounts {
		if aws.ToString(a.Name) == "orap-account" {
			accountID = aws.ToString(a.Id)
		}
	}

	require.NotEmpty(t, accountID, "orap organizations account should be listed")

	delAdminsOut, err := client.ListDelegatedAdministrators(
		ctx,
		&organizationssvc48.ListDelegatedAdministratorsInput{},
	)
	require.NoError(t, err, "ListDelegatedAdministrators should succeed")

	var foundDelegatedAdmin bool

	for _, a := range delAdminsOut.DelegatedAdministrators {
		if aws.ToString(a.Id) == accountID {
			foundDelegatedAdmin = true
		}
	}

	assert.True(t, foundDelegatedAdmin, "orap account should be a delegated administrator")

	policiesOut, err := client.ListPolicies(ctx, &organizationssvc48.ListPoliciesInput{
		Filter: "SERVICE_CONTROL_POLICY",
	})
	require.NoError(t, err, "ListPolicies should succeed")

	var policyID string

	for _, p := range policiesOut.Policies {
		if aws.ToString(p.Name) == "orap-policy" {
			policyID = aws.ToString(p.Id)
		}
	}

	require.NotEmpty(t, policyID, "orap policy should be listed")

	targetsOut, err := client.ListTargetsForPolicy(
		ctx,
		&organizationssvc48.ListTargetsForPolicyInput{PolicyId: aws.String(policyID)},
	)
	require.NoError(t, err, "ListTargetsForPolicy should succeed")

	var foundAttachment bool

	for _, tgt := range targetsOut.Targets {
		if aws.ToString(tgt.TargetId) == accountID {
			foundAttachment = true
		}
	}

	assert.True(t, foundAttachment, "orap policy should be attached to the account")

	rpOut, err := client.DescribeResourcePolicy(ctx, &organizationssvc48.DescribeResourcePolicyInput{})
	require.NoError(t, err, "DescribeResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(rpOut.ResourcePolicy.Content), "OrganizationsAndAppstreamResourcePolicy")
}
