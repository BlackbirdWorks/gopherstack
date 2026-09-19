package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	accessanalyzersvc "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	appmeshsvc "github.com/aws/aws-sdk-go-v2/service/appmesh"
	cleanroomssvc "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	cleanroomstypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	cloudfrontsdkv2 "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cloudfrontkeyvaluestoresvc "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"
	dlmsvc "github.com/aws/aws-sdk-go-v2/service/dlm"
	grafanasvc "github.com/aws/aws-sdk-go-v2/service/grafana"
	inspector2svc "github.com/aws/aws-sdk-go-v2/service/inspector2"
	lightsailsvc "github.com/aws/aws-sdk-go-v2/service/lightsail"
	networkmanagersvc "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch8 provisions accessanalyzer, appmesh, cleanrooms,
// cloudfront key value store, dlm, grafana, lightsail, networkmanager, and
// inspector2 resources via Terraform and verifies each through its own SDK
// client's Get/Describe path.
func TestTerraform_MegaBatch8(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-8",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				aaClient := accessanalyzersvc.NewFromConfig(cfg, func(o *accessanalyzersvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				aaOut, err := aaClient.GetAnalyzer(ctx, &accessanalyzersvc.GetAnalyzerInput{
					AnalyzerName: aws.String("mega-batch-8-analyzer"),
				})
				require.NoError(t, err, "GetAnalyzer should succeed")
				require.NotNil(t, aaOut.Analyzer)
				assert.Equal(t, "mega-batch-8-analyzer", aws.ToString(aaOut.Analyzer.Name))

				meshClient := appmeshsvc.NewFromConfig(cfg, func(o *appmeshsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				meshOut, err := meshClient.DescribeMesh(ctx, &appmeshsvc.DescribeMeshInput{
					MeshName: aws.String("mega-batch-8-mesh"),
				})
				require.NoError(t, err, "DescribeMesh should succeed")
				require.NotNil(t, meshOut.Mesh)
				assert.Equal(t, "mega-batch-8-mesh", aws.ToString(meshOut.Mesh.MeshName))

				routerOut, err := meshClient.DescribeVirtualRouter(ctx, &appmeshsvc.DescribeVirtualRouterInput{
					MeshName:          aws.String("mega-batch-8-mesh"),
					VirtualRouterName: aws.String("mega-batch-8-vrouter"),
				})
				require.NoError(t, err, "DescribeVirtualRouter should succeed")
				require.NotNil(t, routerOut.VirtualRouter)

				crClient := cleanroomssvc.NewFromConfig(cfg, func(o *cleanroomssvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				listOut, err := crClient.ListCollaborations(ctx, &cleanroomssvc.ListCollaborationsInput{
					MemberStatus: cleanroomstypes.FilterableMemberStatusActive,
				})
				require.NoError(t, err, "ListCollaborations should succeed")

				var collabID string

				for _, c := range listOut.CollaborationList {
					if aws.ToString(c.Name) == "mega-batch-8-collab" {
						collabID = aws.ToString(c.Id)
					}
				}

				require.NotEmpty(t, collabID, "collaboration mega-batch-8-collab should be listed")

				getCollabOut, err := crClient.GetCollaboration(ctx, &cleanroomssvc.GetCollaborationInput{
					CollaborationIdentifier: aws.String(collabID),
				})
				require.NoError(t, err, "GetCollaboration should succeed")
				require.NotNil(t, getCollabOut.Collaboration)
				assert.Equal(t, "mega-batch-8-collab", aws.ToString(getCollabOut.Collaboration.Name))

				cfClient := cloudfrontsdkv2.NewFromConfig(cfg, func(o *cloudfrontsdkv2.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				kvsOut, err := cfClient.DescribeKeyValueStore(ctx, &cloudfrontsdkv2.DescribeKeyValueStoreInput{
					Name: aws.String("mega-batch-8-kvs"),
				})
				require.NoError(t, err, "DescribeKeyValueStore should succeed")
				require.NotNil(t, kvsOut.KeyValueStore)

				kvClient := cloudfrontkeyvaluestoresvc.NewFromConfig(cfg, func(o *cloudfrontkeyvaluestoresvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				keyOut, err := kvClient.GetKey(ctx, &cloudfrontkeyvaluestoresvc.GetKeyInput{
					KvsARN: kvsOut.KeyValueStore.ARN,
					Key:    aws.String("mega-batch-8-key"),
				})
				require.NoError(t, err, "GetKey should succeed")
				assert.Equal(t, "mega-batch-8-value", aws.ToString(keyOut.Value))

				dlmClient := dlmsvc.NewFromConfig(cfg, func(o *dlmsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				policiesOut, err := dlmClient.GetLifecyclePolicies(ctx, &dlmsvc.GetLifecyclePoliciesInput{})
				require.NoError(t, err, "GetLifecyclePolicies should succeed")

				var policyID string

				for _, p := range policiesOut.Policies {
					if aws.ToString(p.Description) == "mega batch 8 dlm policy" {
						policyID = aws.ToString(p.PolicyId)
					}
				}

				require.NotEmpty(t, policyID, "dlm policy should be listed")

				getPolicyOut, err := dlmClient.GetLifecyclePolicy(ctx, &dlmsvc.GetLifecyclePolicyInput{
					PolicyId: aws.String(policyID),
				})
				require.NoError(t, err, "GetLifecyclePolicy should succeed")
				require.NotNil(t, getPolicyOut.Policy)
				assert.Equal(
					t,
					"arn:aws:iam::000000000000:role/mega-batch-8-dlm-role",
					aws.ToString(getPolicyOut.Policy.ExecutionRoleArn),
				)

				grafanaClient := grafanasvc.NewFromConfig(cfg, func(o *grafanasvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				workspacesOut, err := grafanaClient.ListWorkspaces(ctx, &grafanasvc.ListWorkspacesInput{})
				require.NoError(t, err, "ListWorkspaces should succeed")

				var workspaceID string

				for _, w := range workspacesOut.Workspaces {
					if aws.ToString(w.Name) == "mega-batch-8-grafana" {
						workspaceID = aws.ToString(w.Id)
					}
				}

				require.NotEmpty(t, workspaceID, "grafana workspace should be listed")

				describeWSOut, err := grafanaClient.DescribeWorkspace(ctx, &grafanasvc.DescribeWorkspaceInput{
					WorkspaceId: aws.String(workspaceID),
				})
				require.NoError(t, err, "DescribeWorkspace should succeed")
				require.NotNil(t, describeWSOut.Workspace)

				lsClient := lightsailsvc.NewFromConfig(cfg, func(o *lightsailsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				instOut, err := lsClient.GetInstance(ctx, &lightsailsvc.GetInstanceInput{
					InstanceName: aws.String("mega-batch-8-instance"),
				})
				require.NoError(t, err, "GetInstance should succeed")
				require.NotNil(t, instOut.Instance)
				assert.Equal(t, "amazon_linux_2023", aws.ToString(instOut.Instance.BlueprintId))
				assert.Equal(t, "nano_3_0", aws.ToString(instOut.Instance.BundleId))

				bucketOut, err := lsClient.GetBuckets(ctx, &lightsailsvc.GetBucketsInput{
					BucketName: aws.String("mega-batch-8-bucket"),
				})
				require.NoError(t, err, "GetBuckets should succeed")
				require.Len(t, bucketOut.Buckets, 1)

				staticIPOut, err := lsClient.GetStaticIp(ctx, &lightsailsvc.GetStaticIpInput{
					StaticIpName: aws.String("mega-batch-8-staticip"),
				})
				require.NoError(t, err, "GetStaticIp should succeed")
				require.NotNil(t, staticIPOut.StaticIp)

				nmClient := networkmanagersvc.NewFromConfig(cfg, func(o *networkmanagersvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				gnOut, err := nmClient.DescribeGlobalNetworks(ctx, &networkmanagersvc.DescribeGlobalNetworksInput{})
				require.NoError(t, err, "DescribeGlobalNetworks should succeed")

				var globalNetworkID string

				for _, gn := range gnOut.GlobalNetworks {
					if aws.ToString(gn.Description) == "mega batch 8 global network" {
						globalNetworkID = aws.ToString(gn.GlobalNetworkId)
					}
				}

				require.NotEmpty(t, globalNetworkID, "global network should be listed")

				sitesOut, err := nmClient.GetSites(ctx, &networkmanagersvc.GetSitesInput{
					GlobalNetworkId: aws.String(globalNetworkID),
				})
				require.NoError(t, err, "GetSites should succeed")
				require.Len(t, sitesOut.Sites, 1)

				linksOut, err := nmClient.GetLinks(ctx, &networkmanagersvc.GetLinksInput{
					GlobalNetworkId: aws.String(globalNetworkID),
				})
				require.NoError(t, err, "GetLinks should succeed")
				require.Len(t, linksOut.Links, 1)
				assert.Equal(t, int32(100), aws.ToInt32(linksOut.Links[0].Bandwidth.DownloadSpeed))

				insp2Client := inspector2svc.NewFromConfig(cfg, func(o *inspector2svc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				statusOut, err := insp2Client.BatchGetAccountStatus(ctx, &inspector2svc.BatchGetAccountStatusInput{
					AccountIds: []string{"000000000000"},
				})
				require.NoError(t, err, "BatchGetAccountStatus should succeed")
				require.Len(t, statusOut.Accounts, 1)
				require.NotNil(t, statusOut.Accounts[0].ResourceState)
				require.NotNil(t, statusOut.Accounts[0].ResourceState.Ecr)
				assert.Equal(t, "ENABLED", string(statusOut.Accounts[0].ResourceState.Ecr.Status))
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
