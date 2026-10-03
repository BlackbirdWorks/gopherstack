package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ActivateTypeOptionsVisibleInDescribeType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		wantRole     string
		wantLogGroup string
		wantLogRole  string
		input        cfnsdk.ActivateTypeInput
		wantAuto     bool
	}{
		{
			name:     "defaults",
			input:    cfnsdk.ActivateTypeInput{},
			wantAuto: true,
		},
		{
			name: "explicit",
			input: cfnsdk.ActivateTypeInput{
				AutoUpdate:       aws.Bool(false),
				ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/exec"),
				LoggingConfig: &types.LoggingConfig{
					LogGroupName: aws.String("/cfn/ext"),
					LogRoleArn:   aws.String("arn:aws:iam::123456789012:role/log"),
				},
			},
			wantRole:     "arn:aws:iam::123456789012:role/exec",
			wantLogGroup: "/cfn/ext",
			wantLogRole:  "arn:aws:iam::123456789012:role/log",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			in := tt.input
			in.TypeName = aws.String("AWS::ActivateOpts::Type")

			act, err := client.ActivateType(t.Context(), &in)
			require.NoError(t, err)

			out, err := client.DescribeType(t.Context(), &cfnsdk.DescribeTypeInput{Arn: act.Arn})
			require.NoError(t, err)

			require.NotNil(t, out.AutoUpdate)
			assert.Equal(t, tt.wantAuto, *out.AutoUpdate)
			assert.Equal(t, tt.wantRole, aws.ToString(out.ExecutionRoleArn))

			if tt.wantLogGroup == "" {
				assert.Nil(t, out.LoggingConfig)

				return
			}

			require.NotNil(t, out.LoggingConfig)
			assert.Equal(t, tt.wantLogGroup, aws.ToString(out.LoggingConfig.LogGroupName))
			assert.Equal(t, tt.wantLogRole, aws.ToString(out.LoggingConfig.LogRoleArn))
		})
	}
}
