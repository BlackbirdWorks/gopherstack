package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssvc40 "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes40 "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	emrsvc40 "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes40 "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch40 provisions DMS certificate/replication subnet
// group/S3 source and target endpoints/event subscription/replication
// instance/task/serverless replication config, and EMR block public access
// configuration/security configuration/instance-group and instance-fleet
// clusters with a task instance group, a task instance fleet, and a managed
// scaling policy, plus a studio with a user session mapping, via Terraform,
// verifying each through its own SDK client.
func TestTerraform_MegaBatch40(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-40",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return nil
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch40DMS(ctx, t)
				verifyMegaBatch40EMR(ctx, t)
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

func verifyMegaBatch40DMS(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := dmssvc40.NewFromConfig(cfg, func(o *dmssvc40.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	certOut, err := client.DescribeCertificates(ctx, &dmssvc40.DescribeCertificatesInput{
		Filters: []dmstypes40.Filter{{Name: aws.String("certificate-id"), Values: []string{"mega-batch-40-cert"}}},
	})
	require.NoError(t, err, "DescribeCertificates should succeed")
	require.Len(t, certOut.Certificates, 1)

	sgOut, err := client.DescribeReplicationSubnetGroups(ctx, &dmssvc40.DescribeReplicationSubnetGroupsInput{
		Filters: []dmstypes40.Filter{
			{Name: aws.String("replication-subnet-group-id"), Values: []string{"mega-batch-40-subnet-group"}},
		},
	})
	require.NoError(t, err, "DescribeReplicationSubnetGroups should succeed")
	require.Len(t, sgOut.ReplicationSubnetGroups, 1)
	assert.Len(t, sgOut.ReplicationSubnetGroups[0].Subnets, 2)

	srcOut, err := client.DescribeEndpoints(ctx, &dmssvc40.DescribeEndpointsInput{
		Filters: []dmstypes40.Filter{{Name: aws.String("endpoint-id"), Values: []string{"mega-batch-40-source"}}},
	})
	require.NoError(t, err, "DescribeEndpoints (source) should succeed")
	require.Len(t, srcOut.Endpoints, 1)
	assert.Equal(t, "s3", aws.ToString(srcOut.Endpoints[0].EngineName))
	require.NotNil(t, srcOut.Endpoints[0].S3Settings)
	assert.Equal(t, "mega-batch-40-dms-source-bucket", aws.ToString(srcOut.Endpoints[0].S3Settings.BucketName))

	tgtOut, err := client.DescribeEndpoints(ctx, &dmssvc40.DescribeEndpointsInput{
		Filters: []dmstypes40.Filter{{Name: aws.String("endpoint-id"), Values: []string{"mega-batch-40-target-s3"}}},
	})
	require.NoError(t, err, "DescribeEndpoints (target) should succeed")
	require.Len(t, tgtOut.Endpoints, 1)
	require.NotNil(t, tgtOut.Endpoints[0].S3Settings)
	assert.Equal(t, "mega-batch-40-dms-target-bucket", aws.ToString(tgtOut.Endpoints[0].S3Settings.BucketName))

	evOut, err := client.DescribeEventSubscriptions(ctx, &dmssvc40.DescribeEventSubscriptionsInput{
		Filters: []dmstypes40.Filter{
			{Name: aws.String("event-subscription-arn"), Values: []string{"mega-batch-40-event-subscription"}},
		},
	})
	// Real DMS DescribeEventSubscriptions doesn't filter by name reliably across
	// engines; fall back to an unfiltered describe if the filtered call finds nothing.
	if err != nil || len(evOut.EventSubscriptionsList) == 0 {
		evOut, err = client.DescribeEventSubscriptions(ctx, &dmssvc40.DescribeEventSubscriptionsInput{})
	}

	require.NoError(t, err, "DescribeEventSubscriptions should succeed")

	var foundSub bool

	for _, s := range evOut.EventSubscriptionsList {
		if aws.ToString(s.CustSubscriptionId) == "mega-batch-40-event-subscription" {
			foundSub = true
		}
	}

	assert.True(t, foundSub, "event subscription should be listed")

	riOut, err := client.DescribeReplicationInstances(ctx, &dmssvc40.DescribeReplicationInstancesInput{
		Filters: []dmstypes40.Filter{
			{Name: aws.String("replication-instance-id"), Values: []string{"mega-batch-40-instance"}},
		},
	})
	require.NoError(t, err, "DescribeReplicationInstances should succeed")
	require.Len(t, riOut.ReplicationInstances, 1)

	taskOut, err := client.DescribeReplicationTasks(ctx, &dmssvc40.DescribeReplicationTasksInput{
		Filters: []dmstypes40.Filter{{Name: aws.String("replication-task-id"), Values: []string{"mega-batch-40-task"}}},
	})
	require.NoError(t, err, "DescribeReplicationTasks should succeed")
	require.Len(t, taskOut.ReplicationTasks, 1)

	rcOut, err := client.DescribeReplicationConfigs(ctx, &dmssvc40.DescribeReplicationConfigsInput{})
	require.NoError(t, err, "DescribeReplicationConfigs should succeed")

	var foundConfig bool

	for _, rc := range rcOut.ReplicationConfigs {
		if aws.ToString(rc.ReplicationConfigIdentifier) == "mega-batch-40-serverless" {
			foundConfig = true
			require.NotNil(t, rc.ComputeConfig)
			assert.Equal(t, "mega-batch-40-subnet-group", aws.ToString(rc.ComputeConfig.ReplicationSubnetGroupId))
		}
	}

	assert.True(t, foundConfig, "serverless replication config should be listed")
}

func verifyMegaBatch40EMR(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := emrsvc40.NewFromConfig(cfg, func(o *emrsvc40.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	bpaOut, err := client.GetBlockPublicAccessConfiguration(ctx, &emrsvc40.GetBlockPublicAccessConfigurationInput{})
	require.NoError(t, err, "GetBlockPublicAccessConfiguration should succeed")
	require.NotNil(t, bpaOut.BlockPublicAccessConfiguration)
	assert.True(t, aws.ToBool(bpaOut.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules))

	secOut, err := client.DescribeSecurityConfiguration(ctx, &emrsvc40.DescribeSecurityConfigurationInput{
		Name: aws.String("mega-batch-40-security-config"),
	})
	require.NoError(t, err, "DescribeSecurityConfiguration should succeed")
	assert.Contains(t, aws.ToString(secOut.SecurityConfiguration), "SSE-S3")

	clustersOut, err := client.ListClusters(ctx, &emrsvc40.ListClustersInput{})
	require.NoError(t, err, "ListClusters should succeed")

	var groupsClusterID, fleetsClusterID string

	for _, c := range clustersOut.Clusters {
		switch aws.ToString(c.Name) {
		case "mega-batch-40-emr-groups":
			groupsClusterID = aws.ToString(c.Id)
		case "mega-batch-40-emr-fleets":
			fleetsClusterID = aws.ToString(c.Id)
		}
	}

	require.NotEmpty(t, groupsClusterID, "instance-group cluster should be listed")
	require.NotEmpty(t, fleetsClusterID, "instance-fleet cluster should be listed")

	groupsOut, err := client.ListInstanceGroups(ctx, &emrsvc40.ListInstanceGroupsInput{
		ClusterId: aws.String(groupsClusterID),
	})
	require.NoError(t, err, "ListInstanceGroups should succeed")

	var foundTaskGroup bool

	for _, g := range groupsOut.InstanceGroups {
		if aws.ToString(g.Name) == "mega-batch-40-task-group" {
			foundTaskGroup = true
			assert.Equal(t, emrtypes40.InstanceGroupTypeTask, g.InstanceGroupType)
		}
	}

	assert.True(t, foundTaskGroup, "task instance group should be listed")

	scalingOut, err := client.GetManagedScalingPolicy(ctx, &emrsvc40.GetManagedScalingPolicyInput{
		ClusterId: aws.String(groupsClusterID),
	})
	require.NoError(t, err, "GetManagedScalingPolicy should succeed")
	require.NotNil(t, scalingOut.ManagedScalingPolicy)
	require.NotNil(t, scalingOut.ManagedScalingPolicy.ComputeLimits)
	assert.EqualValues(t, 4, aws.ToInt32(scalingOut.ManagedScalingPolicy.ComputeLimits.MaximumCapacityUnits))

	fleetsOut, err := client.ListInstanceFleets(ctx, &emrsvc40.ListInstanceFleetsInput{
		ClusterId: aws.String(fleetsClusterID),
	})
	require.NoError(t, err, "ListInstanceFleets should succeed")

	var foundTaskFleet bool

	for _, f := range fleetsOut.InstanceFleets {
		if aws.ToString(f.Name) == "mega-batch-40-task-fleet" {
			foundTaskFleet = true
			require.NotEmpty(t, f.InstanceTypeSpecifications)
			assert.Equal(t, "m4.large", aws.ToString(f.InstanceTypeSpecifications[0].InstanceType))
		}
	}

	assert.True(t, foundTaskFleet, "task instance fleet should be listed")

	studiosOut, err := client.ListStudios(ctx, &emrsvc40.ListStudiosInput{})
	require.NoError(t, err, "ListStudios should succeed")

	var studioID string

	for _, s := range studiosOut.Studios {
		if aws.ToString(s.Name) == "mega-batch-40-studio" {
			studioID = aws.ToString(s.StudioId)
		}
	}

	require.NotEmpty(t, studioID, "studio should be listed")

	studioOut, err := client.DescribeStudio(ctx, &emrsvc40.DescribeStudioInput{StudioId: aws.String(studioID)})
	require.NoError(t, err, "DescribeStudio should succeed")
	assert.Equal(t, emrtypes40.AuthModeIam, studioOut.Studio.AuthMode)

	mappingOut, err := client.GetStudioSessionMapping(ctx, &emrsvc40.GetStudioSessionMappingInput{
		StudioId:     aws.String(studioID),
		IdentityType: emrtypes40.IdentityTypeUser,
		IdentityName: aws.String("mega-batch-40-user"),
	})
	require.NoError(t, err, "GetStudioSessionMapping should succeed")
	require.NotNil(t, mappingOut.SessionMapping)
	assert.Contains(t, aws.ToString(mappingOut.SessionMapping.SessionPolicyArn), "mega-batch-40-session-policy")
}
