package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationProvisionsRUMAppMonitor(t *testing.T) {
	t.Parallel()

	template, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Mon": map[string]any{
				"Type": "AWS::RUM::AppMonitor",
				"Properties": map[string]any{
					"Name": "cfn-mon", "Domain": "example.com",
					"Tags": []any{map[string]any{"Key": "env", "Value": "cfn"}},
				},
			},
		},
		"Outputs": map[string]any{
			"Name": map[string]any{"Value": map[string]any{"Ref": "Mon"}},
			"Id":   map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Mon", "Id"}}},
		},
	})
	require.NoError(t, err)

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	rumClient := rum.NewFromConfig(fx.cfg)

	_, err = cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
		StackName: aws.String("rum-stack"), TemplateBody: aws.String(string(template)),
	})
	require.NoError(t, err)

	stack := waitStack(t, cfn, "rum-stack")
	require.Equal(t, cfntypes.StackStatusCreateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))

	outputs := map[string]string{}
	for _, o := range stack.Outputs {
		outputs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	assert.Equal(t, "cfn-mon", outputs["Name"])

	got, err := rumClient.GetAppMonitor(t.Context(), &rum.GetAppMonitorInput{Name: aws.String("cfn-mon")})
	require.NoError(t, err)
	assert.Equal(t, outputs["Id"], aws.ToString(got.AppMonitor.Id))
	assert.Equal(t, map[string]string{"env": "cfn"}, got.AppMonitor.Tags)

	_, err = cfn.DeleteStack(t.Context(), &cloudformation.DeleteStackInput{StackName: aws.String("rum-stack")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, gerr := rumClient.GetAppMonitor(t.Context(), &rum.GetAppMonitorInput{Name: aws.String("cfn-mon")})

		return gerr != nil
	}, 30*time.Second, 50*time.Millisecond)
}
