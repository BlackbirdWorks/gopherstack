package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudfrontsvc2 "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cfkvssvc "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"
	grafanasvc "github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanatypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
	inspector2svc "github.com/aws/aws-sdk-go-v2/service/inspector2"
	networkmanagersvc "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	nmtypes "github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	resiliencehubsvc "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch6 provisions Managed Grafana, Inspector2, Network
// Manager, Resilience Hub, and a CloudFront KeyValueStore via Terraform and
// verifies each through its own SDK client's Describe/List/Get path.
func TestTerraform_MegaBatch6(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-6",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				grafanaClient := grafanasvc.NewFromConfig(cfg, func(o *grafanasvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				wsOut, err := grafanaClient.ListWorkspaces(ctx, &grafanasvc.ListWorkspacesInput{})
				require.NoError(t, err, "ListWorkspaces should succeed")
				findBy(t, wsOut.Workspaces, func(w grafanatypes.WorkspaceSummary) bool {
					return aws.ToString(w.Name) == "mega-batch-6-grafana"
				}, "mega-batch-6-grafana workspace")

				inspClient := inspector2svc.NewFromConfig(cfg, func(o *inspector2svc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				statusOut, err := inspClient.BatchGetAccountStatus(ctx, &inspector2svc.BatchGetAccountStatusInput{
					AccountIds: []string{"000000000000"},
				})
				require.NoError(t, err, "BatchGetAccountStatus should succeed")
				require.Len(t, statusOut.Accounts, 1)
				assert.Equal(t, "ENABLED", string(statusOut.Accounts[0].ResourceState.Ecr.Status))

				nmClient := networkmanagersvc.NewFromConfig(cfg, func(o *networkmanagersvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				nmOut, err := nmClient.DescribeGlobalNetworks(ctx, &networkmanagersvc.DescribeGlobalNetworksInput{})
				require.NoError(t, err, "DescribeGlobalNetworks should succeed")
				findBy(t, nmOut.GlobalNetworks, func(n nmtypes.GlobalNetwork) bool {
					return aws.ToString(n.Description) == "mega-batch-6 global network"
				}, "mega-batch-6 global network")

				rhClient := resiliencehubsvc.NewFromConfig(cfg, func(o *resiliencehubsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				rhOut, err := rhClient.ListResiliencyPolicies(ctx, &resiliencehubsvc.ListResiliencyPoliciesInput{})
				require.NoError(t, err, "ListResiliencyPolicies should succeed")
				require.NotEmpty(t, rhOut.ResiliencyPolicies, "a resiliency policy should exist after apply")
				assert.Equal(t, "megabatch6policy", aws.ToString(rhOut.ResiliencyPolicies[0].PolicyName))

				cfClient := cloudfrontsvc2.NewFromConfig(cfg, func(o *cloudfrontsvc2.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				kvsOut, err := cfClient.DescribeKeyValueStore(ctx, &cloudfrontsvc2.DescribeKeyValueStoreInput{
					Name: aws.String("megabatch6kvs"),
				})
				require.NoError(t, err, "cloudfront DescribeKeyValueStore should succeed")
				require.NotNil(t, kvsOut.KeyValueStore)

				cfkvsClient := cfkvssvc.NewFromConfig(cfg, func(o *cfkvssvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				cfkvsOut, err := cfkvsClient.DescribeKeyValueStore(ctx, &cfkvssvc.DescribeKeyValueStoreInput{
					KvsARN: kvsOut.KeyValueStore.ARN,
				})
				require.NoError(t, err, "cloudfrontkeyvaluestore DescribeKeyValueStore should succeed")
				assert.Equal(t, aws.ToString(kvsOut.KeyValueStore.ARN), aws.ToString(cfkvsOut.KvsARN))
				assert.Equal(t, int32(0), aws.ToInt32(cfkvsOut.ItemCount), "a fresh store has no keys")
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
