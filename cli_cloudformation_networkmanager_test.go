package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationProvisionsNetworkManager(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"GN": map[string]any{
				"Type":       "AWS::NetworkManager::GlobalNetwork",
				"Properties": map[string]any{"Description": "cfn-gn"},
			},
			"Site": map[string]any{
				"Type": "AWS::NetworkManager::Site",
				"Properties": map[string]any{
					"GlobalNetworkId": map[string]any{"Ref": "GN"}, "Description": "cfn-site",
					"Location": map[string]any{"Address": "1 Main St"},
				},
			},
			"Device": map[string]any{
				"Type": "AWS::NetworkManager::Device",
				"Properties": map[string]any{
					"GlobalNetworkId": map[string]any{"Ref": "GN"}, "SiteId": map[string]any{"Ref": "Site"},
					"Vendor": "acme",
				},
			},
			"Link": map[string]any{
				"Type": "AWS::NetworkManager::Link",
				"Properties": map[string]any{
					"GlobalNetworkId": map[string]any{"Ref": "GN"}, "SiteId": map[string]any{"Ref": "Site"},
					"Bandwidth": map[string]any{"UploadSpeed": 10, "DownloadSpeed": 20},
				},
			},
			"Assoc": map[string]any{
				"Type": "AWS::NetworkManager::LinkAssociation",
				"Properties": map[string]any{
					"GlobalNetworkId": map[string]any{"Ref": "GN"}, "DeviceId": map[string]any{"Ref": "Device"},
					"LinkId": map[string]any{"Ref": "Link"},
				},
			},
		},
		"Outputs": map[string]any{
			"SiteArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Site", "SiteArn"}}},
			"GNArn":   map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"GN", "Arn"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	nm := networkmanager.NewFromConfig(fx.cfg, func(o *networkmanager.Options) { o.Region = "us-west-2" })

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("nm-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	var stack cfntypes.Stack

	require.Eventually(t, func() bool {
		out, descErr := cfn.DescribeStacks(
			t.Context(),
			&cloudformation.DescribeStacksInput{StackName: aws.String("nm-stack")},
		)
		if descErr != nil || len(out.Stacks) == 0 {
			return false
		}

		stack = out.Stacks[0]

		return stack.StackStatus != cfntypes.StackStatusCreateInProgress
	}, 60*time.Second, 50*time.Millisecond)

	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))

	outputs := map[string]string{}
	for _, o := range stack.Outputs {
		outputs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	assert.Contains(t, outputs["SiteArn"], ":site/")
	assert.Contains(t, outputs["GNArn"], ":global-network/")

	gns, err := nm.DescribeGlobalNetworks(t.Context(), &networkmanager.DescribeGlobalNetworksInput{})
	require.NoError(t, err)
	require.Len(t, gns.GlobalNetworks, 1)

	gnID := aws.ToString(gns.GlobalNetworks[0].GlobalNetworkId)
	links, err := nm.GetLinks(t.Context(), &networkmanager.GetLinksInput{GlobalNetworkId: aws.String(gnID)})
	require.NoError(t, err)
	require.Len(t, links.Links, 1)
	assert.EqualValues(t, 20, aws.ToInt32(links.Links[0].Bandwidth.DownloadSpeed))

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("nm-stack")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, descErr := nm.DescribeGlobalNetworks(t.Context(), &networkmanager.DescribeGlobalNetworksInput{})

		return descErr == nil && len(out.GlobalNetworks) == 0
	}, 60*time.Second, 50*time.Millisecond)
}
