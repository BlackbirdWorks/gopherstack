package terraform_test

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	accessanalyzersvc "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	appmeshsvc "github.com/aws/aws-sdk-go-v2/service/appmesh"
	cleanroomssvc "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	cleanroomstypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	daxsvc "github.com/aws/aws-sdk-go-v2/service/dax"
	directconnectsvc "github.com/aws/aws-sdk-go-v2/service/directconnect"
	dxtypes "github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	dlmsvc "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch5 provisions AccessAnalyzer, App Mesh, Clean Rooms,
// DAX, Direct Connect, and DLM resources via Terraform and verifies each
// through its own SDK client's Describe/List/Get path.
func TestTerraform_MegaBatch5(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-5",
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
					AnalyzerName: aws.String("mega-batch-5-analyzer"),
				})
				require.NoError(t, err, "GetAnalyzer should succeed")
				assert.Equal(t, "mega-batch-5-analyzer", aws.ToString(aaOut.Analyzer.Name))

				meshClient := appmeshsvc.NewFromConfig(cfg, func(o *appmeshsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				meshOut, err := meshClient.DescribeMesh(ctx, &appmeshsvc.DescribeMeshInput{
					MeshName: aws.String("mega-batch-5-mesh"),
				})
				require.NoError(t, err, "DescribeMesh should succeed")
				assert.Equal(t, "mega-batch-5-mesh", aws.ToString(meshOut.Mesh.MeshName))

				crClient := cleanroomssvc.NewFromConfig(cfg, func(o *cleanroomssvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				crOut, err := crClient.ListCollaborations(ctx, &cleanroomssvc.ListCollaborationsInput{})
				require.NoError(t, err, "ListCollaborations should succeed")
				findBy(t, crOut.CollaborationList, func(c cleanroomstypes.CollaborationSummary) bool {
					return aws.ToString(c.Name) == "mega-batch-5-collab"
				}, "mega-batch-5-collab collaboration")

				daxClient := daxsvc.NewFromConfig(cfg, func(o *daxsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				daxOut, err := daxClient.DescribeClusters(ctx, &daxsvc.DescribeClustersInput{
					ClusterNames: []string{"mega-batch-5-dax"},
				})
				require.NoError(t, err, "DescribeClusters should succeed")
				require.Len(t, daxOut.Clusters, 1)
				assert.Equal(t, "mega-batch-5-dax", aws.ToString(daxOut.Clusters[0].ClusterName))

				dxClient := directconnectsvc.NewFromConfig(cfg, func(o *directconnectsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				dxOut, err := dxClient.DescribeConnections(ctx, &directconnectsvc.DescribeConnectionsInput{})
				require.NoError(t, err, "DescribeConnections should succeed")
				require.True(t, slices.ContainsFunc(dxOut.Connections, func(c dxtypes.Connection) bool {
					return aws.ToString(c.ConnectionName) == "mega-batch-5-dx"
				}), "the mega-batch-5-dx connection should exist after apply")

				dlmClient := dlmsvc.NewFromConfig(cfg, func(o *dlmsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				dlmOut, err := dlmClient.GetLifecyclePolicies(ctx, &dlmsvc.GetLifecyclePoliciesInput{})
				require.NoError(t, err, "GetLifecyclePolicies should succeed")
				findBy(t, dlmOut.Policies, func(p dlmtypes.LifecyclePolicySummary) bool {
					return aws.ToString(p.Description) == "mega-batch-5 DLM lifecycle policy"
				}, "mega-batch-5 DLM lifecycle policy")
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
