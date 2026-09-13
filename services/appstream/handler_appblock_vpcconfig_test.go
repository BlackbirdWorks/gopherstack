package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppBlockBuilder_VpcConfig_RealClient drives Create/Describe/Update
// through the real SDK client and asserts VpcConfig round-trips.
// types.AppBlockBuilder.VpcConfig is "This member is required"
// (appstream@v1.64.5 types/types.go:248), but the pre-fix handler never read
// VpcConfig off the request or stored it, so every response silently dropped
// the member -- gopherstack-mven required-output nested-domain-struct sweep.
func TestAppBlockBuilder_VpcConfig_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		vpcConfig   *types.VpcConfig
		wantSubnets []string
		wantSGs     []string
	}{
		{
			name: "subnets and security groups",
			vpcConfig: &types.VpcConfig{
				SubnetIds:        []string{"subnet-1", "subnet-2"},
				SecurityGroupIds: []string{"sg-1"},
			},
			wantSubnets: []string{"subnet-1", "subnet-2"},
			wantSGs:     []string{"sg-1"},
		},
		{
			name:        "empty vpc config still round-trips",
			vpcConfig:   &types.VpcConfig{},
			wantSubnets: nil,
			wantSGs:     nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
			h := appstream.NewHandler(backend)
			client := newTestAppStreamClient(t, h)
			ctx := t.Context()

			created, err := client.CreateAppBlockBuilder(ctx, &appstreamsdk.CreateAppBlockBuilderInput{
				Name:         aws.String("vpc-builder"),
				Platform:     types.AppBlockBuilderPlatformTypeWindowsServer2019,
				InstanceType: aws.String("stream.standard.medium"),
				VpcConfig:    tc.vpcConfig,
			})
			require.NoError(t, err)
			require.NotNil(t, created.AppBlockBuilder.VpcConfig,
				"CreateAppBlockBuilder response dropped the required VpcConfig member")
			assert.Equal(t, tc.wantSubnets, created.AppBlockBuilder.VpcConfig.SubnetIds)
			assert.Equal(t, tc.wantSGs, created.AppBlockBuilder.VpcConfig.SecurityGroupIds)

			described, err := client.DescribeAppBlockBuilders(ctx, &appstreamsdk.DescribeAppBlockBuildersInput{
				Names: []string{"vpc-builder"},
			})
			require.NoError(t, err)
			require.Len(t, described.AppBlockBuilders, 1)
			require.NotNil(t, described.AppBlockBuilders[0].VpcConfig,
				"DescribeAppBlockBuilders response dropped the required VpcConfig member")
			assert.Equal(t, tc.wantSubnets, described.AppBlockBuilders[0].VpcConfig.SubnetIds)
			assert.Equal(t, tc.wantSGs, described.AppBlockBuilders[0].VpcConfig.SecurityGroupIds)
		})
	}
}

// TestAppBlockBuilder_VpcConfig_Update_RealClient checks UpdateAppBlockBuilder
// replaces VpcConfig when supplied and preserves it when omitted (real
// UpdateAppBlockBuilderInput.VpcConfig is optional, api_op_UpdateAppBlockBuilder.go:92-96).
func TestAppBlockBuilder_VpcConfig_Update_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		builderName string
		update      *types.VpcConfig
		wantSubnets []string
		wantSGs     []string
	}{
		{
			name:        "replaces when supplied",
			builderName: "update-vpc-builder-replace",
			update:      &types.VpcConfig{SubnetIds: []string{"subnet-9"}, SecurityGroupIds: []string{"sg-9"}},
			wantSubnets: []string{"subnet-9"},
			wantSGs:     []string{"sg-9"},
		},
		{
			name:        "preserves when omitted",
			builderName: "update-vpc-builder-preserve",
			update:      nil,
			wantSubnets: []string{"subnet-1"},
			wantSGs:     nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
			h := appstream.NewHandler(backend)
			client := newTestAppStreamClient(t, h)
			ctx := t.Context()

			_, err := client.CreateAppBlockBuilder(ctx, &appstreamsdk.CreateAppBlockBuilderInput{
				Name:         aws.String(tc.builderName),
				Platform:     types.AppBlockBuilderPlatformTypeWindowsServer2019,
				InstanceType: aws.String("stream.standard.medium"),
				VpcConfig:    &types.VpcConfig{SubnetIds: []string{"subnet-1"}},
			})
			require.NoError(t, err)

			updated, err := client.UpdateAppBlockBuilder(ctx, &appstreamsdk.UpdateAppBlockBuilderInput{
				Name:        aws.String(tc.builderName),
				Description: aws.String("updated"),
				VpcConfig:   tc.update,
			})
			require.NoError(t, err)
			require.NotNil(t, updated.AppBlockBuilder.VpcConfig)
			assert.Equal(t, tc.wantSubnets, updated.AppBlockBuilder.VpcConfig.SubnetIds)
			assert.Equal(t, tc.wantSGs, updated.AppBlockBuilder.VpcConfig.SecurityGroupIds)
		})
	}
}
