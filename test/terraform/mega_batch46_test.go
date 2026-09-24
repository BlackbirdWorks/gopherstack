package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysvc46 "github.com/aws/aws-sdk-go-v2/service/amplify"
	codecommitsvc46 "github.com/aws/aws-sdk-go-v2/service/codecommit"
	cesvc46 "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes46 "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	docdbsvc46 "github.com/aws/aws-sdk-go-v2/service/docdb"
	efssvc46 "github.com/aws/aws-sdk-go-v2/service/efs"
	elasticachesvc46 "github.com/aws/aws-sdk-go-v2/service/elasticache"
	eventbridgesvc46 "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	xraysvc46 "github.com/aws/aws-sdk-go-v2/service/xray"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mega46ProviderBlock extends the shared providerBlock with the docdb and
// xray endpoints, which mega-batch-46 needs but providerBlock doesn't list.
func mega46ProviderBlock(addr string) string {
	base := providerBlock(addr)

	return strings.Replace(
		base,
		"endpoints {\n",
		"endpoints {\n    docdb           = "+mega46Quote(addr)+"\n    xray            = "+mega46Quote(addr)+"\n",
		1,
	)
}

func mega46Quote(s string) string {
	return `"` + s + `"`
}

// TestTerraform_MegaBatch46 provisions an EFS access point/backup policy/
// file system policy, X-Ray encryption config/group/resource policy/sampling
// rule, EventBridge bus policy/permission/target, ElastiCache user group
// association/serverless cache, DocumentDB cluster parameter group/snapshot/
// event subscription/global cluster, Cost Explorer anomaly monitor/
// subscription/cost allocation tag, CodeCommit approval rule template(+
// association)/trigger, and Amplify backend environment/domain association/
// webhook via Terraform, verifying each through its own SDK client.
func TestTerraform_MegaBatch46(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "mega-batch-46",
			providerFn: mega46ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch46EFS(ctx, t)
				verifyMegaBatch46XRay(ctx, t)
				verifyMegaBatch46EventBridge(ctx, t)
				verifyMegaBatch46ElastiCache(ctx, t)
				verifyMegaBatch46DocDB(ctx, t)
				verifyMegaBatch46CE(ctx, t)
				verifyMegaBatch46CodeCommit(ctx, t)
				verifyMegaBatch46Amplify(ctx, t)
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

func verifyMegaBatch46EFS(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := efssvc46.NewFromConfig(cfg, func(o *efssvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	fsOut, err := client.DescribeFileSystems(ctx, &efssvc46.DescribeFileSystemsInput{})
	require.NoError(t, err, "DescribeFileSystems should succeed")

	var fsID string

	for _, fs := range fsOut.FileSystems {
		for _, tag := range fs.Tags {
			if aws.ToString(tag.Key) == "Name" && aws.ToString(tag.Value) == "mega-batch-46-efs" {
				fsID = aws.ToString(fs.FileSystemId)
			}
		}
	}

	require.NotEmpty(t, fsID, "mega-batch-46 file system should be listed")

	apOut, err := client.DescribeAccessPoints(ctx, &efssvc46.DescribeAccessPointsInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err, "DescribeAccessPoints should succeed")
	require.Len(t, apOut.AccessPoints, 1)
	assert.Equal(t, int64(1000), aws.ToInt64(apOut.AccessPoints[0].PosixUser.Uid))

	bpOut, err := client.DescribeBackupPolicy(ctx, &efssvc46.DescribeBackupPolicyInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err, "DescribeBackupPolicy should succeed")
	assert.Equal(t, "ENABLED", string(bpOut.BackupPolicy.Status))

	polOut, err := client.DescribeFileSystemPolicy(ctx, &efssvc46.DescribeFileSystemPolicyInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err, "DescribeFileSystemPolicy should succeed")
	assert.Contains(t, aws.ToString(polOut.Policy), "MegaBatch46")
}

func verifyMegaBatch46XRay(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := xraysvc46.NewFromConfig(cfg, func(o *xraysvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	encOut, err := client.GetEncryptionConfig(ctx, &xraysvc46.GetEncryptionConfigInput{})
	require.NoError(t, err, "GetEncryptionConfig should succeed")
	require.NotNil(t, encOut.EncryptionConfig)

	groupOut, err := client.GetGroup(ctx, &xraysvc46.GetGroupInput{
		GroupName: aws.String("mega-batch-46-group"),
	})
	require.NoError(t, err, "GetGroup should succeed")
	assert.Equal(t, "responsetime > 5", aws.ToString(groupOut.Group.FilterExpression))

	polOut, err := client.ListResourcePolicies(ctx, &xraysvc46.ListResourcePoliciesInput{})
	require.NoError(t, err, "ListResourcePolicies should succeed")

	var foundPolicy bool

	for _, p := range polOut.ResourcePolicies {
		if aws.ToString(p.PolicyName) == "mega-batch-46-policy" {
			foundPolicy = true
		}
	}

	assert.True(t, foundPolicy, "mega-batch-46 resource policy should be listed")

	rulesOut, err := client.GetSamplingRules(ctx, &xraysvc46.GetSamplingRulesInput{})
	require.NoError(t, err, "GetSamplingRules should succeed")

	var foundRule bool

	for _, r := range rulesOut.SamplingRuleRecords {
		if aws.ToString(r.SamplingRule.RuleName) == "mega-batch-46-sampling" {
			foundRule = true
		}
	}

	assert.True(t, foundRule, "mega-batch-46 sampling rule should be listed")
}

func verifyMegaBatch46EventBridge(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := eventbridgesvc46.NewFromConfig(cfg, func(o *eventbridgesvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	busOut, err := client.DescribeEventBus(ctx, &eventbridgesvc46.DescribeEventBusInput{
		Name: aws.String("mega-batch-46-bus"),
	})
	require.NoError(t, err, "DescribeEventBus should succeed")
	assert.Contains(t, aws.ToString(busOut.Policy), "MegaBatch46Bus")

	permBusOut, err := client.DescribeEventBus(ctx, &eventbridgesvc46.DescribeEventBusInput{
		Name: aws.String("mega-batch-46-perm-bus"),
	})
	require.NoError(t, err, "DescribeEventBus for perm bus should succeed")
	assert.Contains(t, aws.ToString(permBusOut.Policy), "MegaBatch46Permission")

	targetsOut, err := client.ListTargetsByRule(ctx, &eventbridgesvc46.ListTargetsByRuleInput{
		Rule:         aws.String("mega-batch-46-rule"),
		EventBusName: aws.String("mega-batch-46-bus"),
	})
	require.NoError(t, err, "ListTargetsByRule should succeed")
	require.Len(t, targetsOut.Targets, 1)
	assert.Equal(t, "mega-batch-46-target", aws.ToString(targetsOut.Targets[0].Id))
}

func verifyMegaBatch46ElastiCache(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := elasticachesvc46.NewFromConfig(cfg, func(o *elasticachesvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	ugOut, err := client.DescribeUserGroups(ctx, &elasticachesvc46.DescribeUserGroupsInput{
		UserGroupId: aws.String("mega-batch-46-ug"),
	})
	require.NoError(t, err, "DescribeUserGroups should succeed")
	require.Len(t, ugOut.UserGroups, 1)
	assert.Contains(t, ugOut.UserGroups[0].UserIds, "mega-batch-46-user-extra")

	scOut, err := client.DescribeServerlessCaches(ctx, &elasticachesvc46.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String("mega-batch-46-serverless"),
	})
	require.NoError(t, err, "DescribeServerlessCaches should succeed")
	require.Len(t, scOut.ServerlessCaches, 1)
	assert.Equal(t, "valkey", aws.ToString(scOut.ServerlessCaches[0].Engine))
}

func verifyMegaBatch46DocDB(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := docdbsvc46.NewFromConfig(cfg, func(o *docdbsvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	cpgOut, err := client.DescribeDBClusterParameterGroups(ctx, &docdbsvc46.DescribeDBClusterParameterGroupsInput{
		DBClusterParameterGroupName: aws.String("mega-batch-46-docdb-cpg"),
	})
	require.NoError(t, err, "DescribeDBClusterParameterGroups should succeed")
	require.Len(t, cpgOut.DBClusterParameterGroups, 1)

	snapOut, err := client.DescribeDBClusterSnapshots(ctx, &docdbsvc46.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("mega-batch-46-docdb-snapshot"),
	})
	require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
	require.Len(t, snapOut.DBClusterSnapshots, 1)
	assert.Equal(t, "mega-batch-46-docdb", aws.ToString(snapOut.DBClusterSnapshots[0].DBClusterIdentifier))

	subOut, err := client.DescribeEventSubscriptions(ctx, &docdbsvc46.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("mega-batch-46-docdb-sub"),
	})
	require.NoError(t, err, "DescribeEventSubscriptions should succeed")
	require.Len(t, subOut.EventSubscriptionsList, 1)

	globalOut, err := client.DescribeGlobalClusters(ctx, &docdbsvc46.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("mega-batch-46-docdb-global"),
	})
	require.NoError(t, err, "DescribeGlobalClusters should succeed")
	require.Len(t, globalOut.GlobalClusters, 1)
}

func verifyMegaBatch46CE(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := cesvc46.NewFromConfig(cfg, func(o *cesvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	monOut, err := client.GetAnomalyMonitors(ctx, &cesvc46.GetAnomalyMonitorsInput{})
	require.NoError(t, err, "GetAnomalyMonitors should succeed")

	var monitorARN string

	for _, m := range monOut.AnomalyMonitors {
		if aws.ToString(m.MonitorName) == "mega-batch-46-monitor" {
			monitorARN = aws.ToString(m.MonitorArn)
		}
	}

	require.NotEmpty(t, monitorARN, "mega-batch-46 anomaly monitor should be listed")

	subOut, err := client.GetAnomalySubscriptions(ctx, &cesvc46.GetAnomalySubscriptionsInput{
		MonitorArn: aws.String(monitorARN),
	})
	require.NoError(t, err, "GetAnomalySubscriptions should succeed")
	require.Len(t, subOut.AnomalySubscriptions, 1)
	assert.Equal(t, cetypes46.AnomalySubscriptionFrequencyDaily, subOut.AnomalySubscriptions[0].Frequency)

	tagsOut, err := client.ListCostAllocationTags(ctx, &cesvc46.ListCostAllocationTagsInput{
		TagKeys: []string{"mega-batch-46-tag"},
	})
	require.NoError(t, err, "ListCostAllocationTags should succeed")
	require.Len(t, tagsOut.CostAllocationTags, 1)
	assert.Equal(t, cetypes46.CostAllocationTagStatusActive, tagsOut.CostAllocationTags[0].Status)
}

func verifyMegaBatch46CodeCommit(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := codecommitsvc46.NewFromConfig(cfg, func(o *codecommitsvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	tmplOut, err := client.GetApprovalRuleTemplate(ctx, &codecommitsvc46.GetApprovalRuleTemplateInput{
		ApprovalRuleTemplateName: aws.String("mega-batch-46-approval-template"),
	})
	require.NoError(t, err, "GetApprovalRuleTemplate should succeed")
	require.NotNil(t, tmplOut.ApprovalRuleTemplate)

	assocOut, err := client.ListAssociatedApprovalRuleTemplatesForRepository(
		ctx,
		&codecommitsvc46.ListAssociatedApprovalRuleTemplatesForRepositoryInput{
			RepositoryName: aws.String("mega-batch-46-repo"),
		},
	)
	require.NoError(t, err, "ListAssociatedApprovalRuleTemplatesForRepository should succeed")
	assert.Contains(t, assocOut.ApprovalRuleTemplateNames, "mega-batch-46-approval-template")

	trigOut, err := client.GetRepositoryTriggers(ctx, &codecommitsvc46.GetRepositoryTriggersInput{
		RepositoryName: aws.String("mega-batch-46-repo"),
	})
	require.NoError(t, err, "GetRepositoryTriggers should succeed")
	require.Len(t, trigOut.Triggers, 1)
	assert.Equal(t, "mega-batch-46-trigger", aws.ToString(trigOut.Triggers[0].Name))
}

func verifyMegaBatch46Amplify(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := amplifysvc46.NewFromConfig(cfg, func(o *amplifysvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	appsOut, err := client.ListApps(ctx, &amplifysvc46.ListAppsInput{})
	require.NoError(t, err, "ListApps should succeed")

	var appID string

	for _, a := range appsOut.Apps {
		if aws.ToString(a.Name) == "mega-batch-46-app" {
			appID = aws.ToString(a.AppId)
		}
	}

	require.NotEmpty(t, appID, "mega-batch-46 app should be listed")

	envOut, err := client.GetBackendEnvironment(ctx, &amplifysvc46.GetBackendEnvironmentInput{
		AppId:           aws.String(appID),
		EnvironmentName: aws.String("mbfortysix"),
	})
	require.NoError(t, err, "GetBackendEnvironment should succeed")
	require.NotNil(t, envOut.BackendEnvironment)

	domOut, err := client.GetDomainAssociation(ctx, &amplifysvc46.GetDomainAssociationInput{
		AppId:      aws.String(appID),
		DomainName: aws.String("mega-batch-46.example.test"),
	})
	require.NoError(t, err, "GetDomainAssociation should succeed")
	require.NotNil(t, domOut.DomainAssociation)

	webhooksOut, err := client.ListWebhooks(ctx, &amplifysvc46.ListWebhooksInput{
		AppId: aws.String(appID),
	})
	require.NoError(t, err, "ListWebhooks should succeed")

	var foundWebhook bool

	for _, w := range webhooksOut.Webhooks {
		if aws.ToString(w.Description) == "mega-batch-46 webhook" {
			foundWebhook = true
		}
	}

	assert.True(t, foundWebhook, "mega-batch-46 webhook should be listed")
}
