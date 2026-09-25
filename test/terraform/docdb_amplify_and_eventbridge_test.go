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
// xray endpoints, which dbae needs but providerBlock doesn't list.
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

// TestTerraform_DocdbAmplifyAndEventbridge provisions an EFS access point/backup policy/
// file system policy, X-Ray encryption config/group/resource policy/sampling
// rule, EventBridge bus policy/permission/target, ElastiCache user group
// association/serverless cache, DocumentDB cluster parameter group/snapshot/
// event subscription/global cluster, Cost Explorer anomaly monitor/
// subscription/cost allocation tag, CodeCommit approval rule template(+
// association)/trigger, and Amplify backend environment/domain association/
// webhook via Terraform, verifying each through its own SDK client.
func TestTerraform_DocdbAmplifyAndEventbridge(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "docdb-amplify-and-eventbridge",
			providerFn: mega46ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyDocdbAmplifyAndEventbridgeEFS(ctx, t)
				verifyDocdbAmplifyAndEventbridgeXRay(ctx, t)
				verifyDocdbAmplifyAndEventbridgeEventBridge(ctx, t)
				verifyDocdbAmplifyAndEventbridgeElastiCache(ctx, t)
				verifyDocdbAmplifyAndEventbridgeDocDB(ctx, t)
				verifyDocdbAmplifyAndEventbridgeCE(ctx, t)
				verifyDocdbAmplifyAndEventbridgeCodeCommit(ctx, t)
				verifyDocdbAmplifyAndEventbridgeAmplify(ctx, t)
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

func verifyDocdbAmplifyAndEventbridgeEFS(ctx context.Context, t *testing.T) {
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
			if aws.ToString(tag.Key) == "Name" && aws.ToString(tag.Value) == "dbae-efs" {
				fsID = aws.ToString(fs.FileSystemId)
			}
		}
	}

	require.NotEmpty(t, fsID, "dbae file system should be listed")

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
	assert.Contains(t, aws.ToString(polOut.Policy), "DocdbAmplifyAndEventbridge")
}

func verifyDocdbAmplifyAndEventbridgeXRay(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := xraysvc46.NewFromConfig(cfg, func(o *xraysvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	encOut, err := client.GetEncryptionConfig(ctx, &xraysvc46.GetEncryptionConfigInput{})
	require.NoError(t, err, "GetEncryptionConfig should succeed")
	require.NotNil(t, encOut.EncryptionConfig)

	groupOut, err := client.GetGroup(ctx, &xraysvc46.GetGroupInput{
		GroupName: aws.String("dbae-group"),
	})
	require.NoError(t, err, "GetGroup should succeed")
	assert.Equal(t, "responsetime > 5", aws.ToString(groupOut.Group.FilterExpression))

	polOut, err := client.ListResourcePolicies(ctx, &xraysvc46.ListResourcePoliciesInput{})
	require.NoError(t, err, "ListResourcePolicies should succeed")

	var foundPolicy bool

	for _, p := range polOut.ResourcePolicies {
		if aws.ToString(p.PolicyName) == "dbae-policy" {
			foundPolicy = true
		}
	}

	assert.True(t, foundPolicy, "dbae resource policy should be listed")

	rulesOut, err := client.GetSamplingRules(ctx, &xraysvc46.GetSamplingRulesInput{})
	require.NoError(t, err, "GetSamplingRules should succeed")

	var foundRule bool

	for _, r := range rulesOut.SamplingRuleRecords {
		if aws.ToString(r.SamplingRule.RuleName) == "dbae-sampling" {
			foundRule = true
		}
	}

	assert.True(t, foundRule, "dbae sampling rule should be listed")
}

func verifyDocdbAmplifyAndEventbridgeEventBridge(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := eventbridgesvc46.NewFromConfig(cfg, func(o *eventbridgesvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	busOut, err := client.DescribeEventBus(ctx, &eventbridgesvc46.DescribeEventBusInput{
		Name: aws.String("dbae-bus"),
	})
	require.NoError(t, err, "DescribeEventBus should succeed")
	assert.Contains(t, aws.ToString(busOut.Policy), "DocdbAmplifyAndEventbridgeBus")

	permBusOut, err := client.DescribeEventBus(ctx, &eventbridgesvc46.DescribeEventBusInput{
		Name: aws.String("dbae-perm-bus"),
	})
	require.NoError(t, err, "DescribeEventBus for perm bus should succeed")
	assert.Contains(t, aws.ToString(permBusOut.Policy), "DocdbAmplifyAndEventbridgePermission")

	targetsOut, err := client.ListTargetsByRule(ctx, &eventbridgesvc46.ListTargetsByRuleInput{
		Rule:         aws.String("dbae-rule"),
		EventBusName: aws.String("dbae-bus"),
	})
	require.NoError(t, err, "ListTargetsByRule should succeed")
	require.Len(t, targetsOut.Targets, 1)
	assert.Equal(t, "dbae-target", aws.ToString(targetsOut.Targets[0].Id))
}

func verifyDocdbAmplifyAndEventbridgeElastiCache(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := elasticachesvc46.NewFromConfig(cfg, func(o *elasticachesvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	ugOut, err := client.DescribeUserGroups(ctx, &elasticachesvc46.DescribeUserGroupsInput{
		UserGroupId: aws.String("dbae-ug"),
	})
	require.NoError(t, err, "DescribeUserGroups should succeed")
	require.Len(t, ugOut.UserGroups, 1)
	assert.Contains(t, ugOut.UserGroups[0].UserIds, "dbae-user-extra")

	scOut, err := client.DescribeServerlessCaches(ctx, &elasticachesvc46.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String("dbae-serverless"),
	})
	require.NoError(t, err, "DescribeServerlessCaches should succeed")
	require.Len(t, scOut.ServerlessCaches, 1)
	assert.Equal(t, "valkey", aws.ToString(scOut.ServerlessCaches[0].Engine))
}

func verifyDocdbAmplifyAndEventbridgeDocDB(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := docdbsvc46.NewFromConfig(cfg, func(o *docdbsvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	cpgOut, err := client.DescribeDBClusterParameterGroups(ctx, &docdbsvc46.DescribeDBClusterParameterGroupsInput{
		DBClusterParameterGroupName: aws.String("dbae-docdb-cpg"),
	})
	require.NoError(t, err, "DescribeDBClusterParameterGroups should succeed")
	require.Len(t, cpgOut.DBClusterParameterGroups, 1)

	snapOut, err := client.DescribeDBClusterSnapshots(ctx, &docdbsvc46.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("dbae-docdb-snapshot"),
	})
	require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
	require.Len(t, snapOut.DBClusterSnapshots, 1)
	assert.Equal(t, "dbae-docdb", aws.ToString(snapOut.DBClusterSnapshots[0].DBClusterIdentifier))

	subOut, err := client.DescribeEventSubscriptions(ctx, &docdbsvc46.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("dbae-docdb-sub"),
	})
	require.NoError(t, err, "DescribeEventSubscriptions should succeed")
	require.Len(t, subOut.EventSubscriptionsList, 1)

	globalOut, err := client.DescribeGlobalClusters(ctx, &docdbsvc46.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("dbae-docdb-global"),
	})
	require.NoError(t, err, "DescribeGlobalClusters should succeed")
	require.Len(t, globalOut.GlobalClusters, 1)
}

func verifyDocdbAmplifyAndEventbridgeCE(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := cesvc46.NewFromConfig(cfg, func(o *cesvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	monOut, err := client.GetAnomalyMonitors(ctx, &cesvc46.GetAnomalyMonitorsInput{})
	require.NoError(t, err, "GetAnomalyMonitors should succeed")

	var monitorARN string

	for _, m := range monOut.AnomalyMonitors {
		if aws.ToString(m.MonitorName) == "dbae-monitor" {
			monitorARN = aws.ToString(m.MonitorArn)
		}
	}

	require.NotEmpty(t, monitorARN, "dbae anomaly monitor should be listed")

	subOut, err := client.GetAnomalySubscriptions(ctx, &cesvc46.GetAnomalySubscriptionsInput{
		MonitorArn: aws.String(monitorARN),
	})
	require.NoError(t, err, "GetAnomalySubscriptions should succeed")
	require.Len(t, subOut.AnomalySubscriptions, 1)
	assert.Equal(t, cetypes46.AnomalySubscriptionFrequencyDaily, subOut.AnomalySubscriptions[0].Frequency)

	tagsOut, err := client.ListCostAllocationTags(ctx, &cesvc46.ListCostAllocationTagsInput{
		TagKeys: []string{"dbae-tag"},
	})
	require.NoError(t, err, "ListCostAllocationTags should succeed")
	require.Len(t, tagsOut.CostAllocationTags, 1)
	assert.Equal(t, cetypes46.CostAllocationTagStatusActive, tagsOut.CostAllocationTags[0].Status)
}

func verifyDocdbAmplifyAndEventbridgeCodeCommit(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := codecommitsvc46.NewFromConfig(cfg, func(o *codecommitsvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	tmplOut, err := client.GetApprovalRuleTemplate(ctx, &codecommitsvc46.GetApprovalRuleTemplateInput{
		ApprovalRuleTemplateName: aws.String("dbae-approval-template"),
	})
	require.NoError(t, err, "GetApprovalRuleTemplate should succeed")
	require.NotNil(t, tmplOut.ApprovalRuleTemplate)

	assocOut, err := client.ListAssociatedApprovalRuleTemplatesForRepository(
		ctx,
		&codecommitsvc46.ListAssociatedApprovalRuleTemplatesForRepositoryInput{
			RepositoryName: aws.String("dbae-repo"),
		},
	)
	require.NoError(t, err, "ListAssociatedApprovalRuleTemplatesForRepository should succeed")
	assert.Contains(t, assocOut.ApprovalRuleTemplateNames, "dbae-approval-template")

	trigOut, err := client.GetRepositoryTriggers(ctx, &codecommitsvc46.GetRepositoryTriggersInput{
		RepositoryName: aws.String("dbae-repo"),
	})
	require.NoError(t, err, "GetRepositoryTriggers should succeed")
	require.Len(t, trigOut.Triggers, 1)
	assert.Equal(t, "dbae-trigger", aws.ToString(trigOut.Triggers[0].Name))
}

func verifyDocdbAmplifyAndEventbridgeAmplify(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := amplifysvc46.NewFromConfig(cfg, func(o *amplifysvc46.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	appsOut, err := client.ListApps(ctx, &amplifysvc46.ListAppsInput{})
	require.NoError(t, err, "ListApps should succeed")

	var appID string

	for _, a := range appsOut.Apps {
		if aws.ToString(a.Name) == "dbae-app" {
			appID = aws.ToString(a.AppId)
		}
	}

	require.NotEmpty(t, appID, "dbae app should be listed")

	envOut, err := client.GetBackendEnvironment(ctx, &amplifysvc46.GetBackendEnvironmentInput{
		AppId:           aws.String(appID),
		EnvironmentName: aws.String("mbfortysix"),
	})
	require.NoError(t, err, "GetBackendEnvironment should succeed")
	require.NotNil(t, envOut.BackendEnvironment)

	domOut, err := client.GetDomainAssociation(ctx, &amplifysvc46.GetDomainAssociationInput{
		AppId:      aws.String(appID),
		DomainName: aws.String("dbae.example.test"),
	})
	require.NoError(t, err, "GetDomainAssociation should succeed")
	require.NotNil(t, domOut.DomainAssociation)

	webhooksOut, err := client.ListWebhooks(ctx, &amplifysvc46.ListWebhooksInput{
		AppId: aws.String(appID),
	})
	require.NoError(t, err, "ListWebhooks should succeed")

	var foundWebhook bool

	for _, w := range webhooksOut.Webhooks {
		if aws.ToString(w.Description) == "dbae webhook" {
			foundWebhook = true
		}
	}

	assert.True(t, foundWebhook, "dbae webhook should be listed")
}
