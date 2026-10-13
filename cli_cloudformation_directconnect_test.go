package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationProvisionsDirectConnect(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Conn": map[string]any{
				"Type": "AWS::DirectConnect::Connection",
				"Properties": map[string]any{
					"ConnectionName": "cfn-conn", "Bandwidth": "1Gbps", "Location": "EqDC2",
					"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
				},
			},
			"Lag": map[string]any{
				"Type": "AWS::DirectConnect::Lag",
				"Properties": map[string]any{
					"LagName": "cfn-lag", "ConnectionsBandwidth": "1Gbps", "Location": "EqDC2",
				},
			},
			"Gateway": map[string]any{
				"Type":       "AWS::DirectConnect::DirectConnectGateway",
				"Properties": map[string]any{"DirectConnectGatewayName": "cfn-dxgw", "AmazonSideAsn": 64512},
			},
			"Vif": map[string]any{
				"Type": "AWS::DirectConnect::TransitVirtualInterface",
				"Properties": map[string]any{
					"ConnectionId": map[string]any{"Ref": "Conn"}, "VirtualInterfaceName": "cfn-vif", "Vlan": 101,
					"DirectConnectGatewayId": map[string]any{"Ref": "Gateway"},
					"BgpPeers":               []any{map[string]any{"AddressFamily": "ipv4", "Asn": "65000"}},
				},
			},
		},
		"Outputs": map[string]any{
			"ConnArn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Conn", "ConnectionArn"}}},
			"VifArn":  map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Vif", "VirtualInterfaceArn"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	dx := directconnect.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("dx-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	var stack cfntypes.Stack

	require.Eventually(t, func() bool {
		out, descErr := cfn.DescribeStacks(
			t.Context(),
			&cloudformation.DescribeStacksInput{StackName: aws.String("dx-stack")},
		)
		if descErr != nil || len(out.Stacks) == 0 {
			return false
		}

		stack = out.Stacks[0]

		return stack.StackStatus != cfntypes.StackStatusCreateInProgress
	}, 20*time.Second, 50*time.Millisecond)

	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))

	outputs := map[string]string{}
	for _, o := range stack.Outputs {
		outputs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	assert.Contains(t, outputs["ConnArn"], "arn:aws:directconnect:")
	assert.Contains(t, outputs["VifArn"], "arn:aws:directconnect:")

	conns, err := dx.DescribeConnections(t.Context(), &directconnect.DescribeConnectionsInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(conns.Connections))
	for _, c := range conns.Connections {
		names = append(names, aws.ToString(c.ConnectionName))
	}

	assert.Contains(t, names, "cfn-conn")

	lags, err := dx.DescribeLags(t.Context(), &directconnect.DescribeLagsInput{})
	require.NoError(t, err)
	require.Len(t, lags.Lags, 1)
	assert.Equal(t, "cfn-lag", aws.ToString(lags.Lags[0].LagName))

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("dx-stack")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, descErr := dx.DescribeLags(t.Context(), &directconnect.DescribeLagsInput{})

		return descErr == nil && len(out.Lags) == 1 && string(out.Lags[0].LagState) == "deleted"
	}, 20*time.Second, 50*time.Millisecond)
}
