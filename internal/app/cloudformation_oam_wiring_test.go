package app

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/oam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationProvisionsOAMSink(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Sink": map[string]any{
				"Type":       "AWS::Oam::Sink",
				"Properties": map[string]any{"Name": "cfn-sink", "Tags": map[string]string{"env": "cfn"}},
			},
		},
		"Outputs": map[string]any{
			"Arn": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Sink", "Arn"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	oamClient := oam.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("oam-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	stack := waitStack(t, cfn, "oam-stack")
	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))
	require.Len(t, stack.Outputs, 1)

	got, err := oamClient.GetSink(t.Context(), &oam.GetSinkInput{
		Identifier: stack.Outputs[0].OutputValue, IncludeTags: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "cfn-sink", aws.ToString(got.Name))
	assert.Equal(t, map[string]string{"env": "cfn"}, got.Tags)

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("oam-stack")})
	require.NoError(t, err)

	sinks, err := oamClient.ListSinks(t.Context(), &oam.ListSinksInput{})
	require.NoError(t, err)
	assert.Empty(t, sinks.Items)
}
