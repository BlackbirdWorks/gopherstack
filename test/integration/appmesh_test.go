package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"
	appmeshtypes "github.com/aws/aws-sdk-go-v2/service/appmesh/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAppMeshClient returns an App Mesh client pointed at the shared test container.
func createAppMeshClient(t *testing.T) *appmeshsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return appmeshsdk.NewFromConfig(cfg, func(o *appmeshsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_AppMesh_MeshLifecycle drives create→describe→list→delete of a mesh.
func TestIntegration_AppMesh_MeshLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		meshName string
	}{
		{name: "full_lifecycle", meshName: "integ-mesh"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppMeshClient(t)

			createOut, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{
				MeshName: aws.String(tt.meshName),
				Spec: &appmeshtypes.MeshSpec{
					EgressFilter: &appmeshtypes.EgressFilter{Type: appmeshtypes.EgressFilterTypeAllowAll},
				},
			})
			require.NoError(t, err, "CreateMesh should succeed")
			require.NotNil(t, createOut.Mesh)
			assert.Equal(t, tt.meshName, aws.ToString(createOut.Mesh.MeshName))

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteMesh(cleanupCtx, &appmeshsdk.DeleteMeshInput{MeshName: aws.String(tt.meshName)})
			})

			descOut, err := client.DescribeMesh(ctx, &appmeshsdk.DescribeMeshInput{MeshName: aws.String(tt.meshName)})
			require.NoError(t, err, "DescribeMesh should succeed")
			require.NotNil(t, descOut.Mesh)
			assert.Equal(t, tt.meshName, aws.ToString(descOut.Mesh.MeshName))

			listOut, err := client.ListMeshes(ctx, &appmeshsdk.ListMeshesInput{})
			require.NoError(t, err, "ListMeshes should succeed")

			found := false
			for _, m := range listOut.Meshes {
				if aws.ToString(m.MeshName) == tt.meshName {
					found = true

					break
				}
			}

			assert.True(t, found, "created mesh should appear in list")

			_, err = client.DeleteMesh(ctx, &appmeshsdk.DeleteMeshInput{MeshName: aws.String(tt.meshName)})
			require.NoError(t, err, "DeleteMesh should succeed")
		})
	}
}

// TestIntegration_AppMesh_VirtualNodeLifecycle drives mesh→virtual-node create→describe→delete.
func TestIntegration_AppMesh_VirtualNodeLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		meshName string
		nodeName string
	}{
		{name: "full_lifecycle", meshName: "integ-vn-mesh", nodeName: "integ-node"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppMeshClient(t)

			_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: aws.String(tt.meshName)})
			require.NoError(t, err, "CreateMesh should succeed")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteMesh(cleanupCtx, &appmeshsdk.DeleteMeshInput{MeshName: aws.String(tt.meshName)})
			})

			_, err = client.CreateVirtualNode(ctx, &appmeshsdk.CreateVirtualNodeInput{
				MeshName:        aws.String(tt.meshName),
				VirtualNodeName: aws.String(tt.nodeName),
				Spec: &appmeshtypes.VirtualNodeSpec{
					Listeners: []appmeshtypes.Listener{
						{
							PortMapping: &appmeshtypes.PortMapping{
								Port:     aws.Int32(8080),
								Protocol: appmeshtypes.PortProtocolHttp,
							},
						},
					},
				},
			})
			require.NoError(t, err, "CreateVirtualNode should succeed")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteVirtualNode(cleanupCtx, &appmeshsdk.DeleteVirtualNodeInput{
					MeshName:        aws.String(tt.meshName),
					VirtualNodeName: aws.String(tt.nodeName),
				})
			})

			descOut, err := client.DescribeVirtualNode(ctx, &appmeshsdk.DescribeVirtualNodeInput{
				MeshName:        aws.String(tt.meshName),
				VirtualNodeName: aws.String(tt.nodeName),
			})
			require.NoError(t, err, "DescribeVirtualNode should succeed")
			require.NotNil(t, descOut.VirtualNode)
			assert.Equal(t, tt.nodeName, aws.ToString(descOut.VirtualNode.VirtualNodeName))

			_, err = client.DeleteVirtualNode(ctx, &appmeshsdk.DeleteVirtualNodeInput{
				MeshName:        aws.String(tt.meshName),
				VirtualNodeName: aws.String(tt.nodeName),
			})
			require.NoError(t, err, "DeleteVirtualNode should succeed")
		})
	}
}
