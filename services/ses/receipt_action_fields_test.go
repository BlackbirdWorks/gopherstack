package ses_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ReceiptActionOptionalMembersRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action  types.ReceiptAction
		check   func(t *testing.T, a types.ReceiptAction)
		name    string
		wantErr bool
	}{
		{
			name: "s3_role_and_kms",
			action: types.ReceiptAction{S3Action: &types.S3Action{
				BucketName: aws.String("b"),
				IamRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				KmsKeyArn:  aws.String("arn:aws:kms:us-east-1:000000000000:key/k"),
			}},
			check: func(t *testing.T, a types.ReceiptAction) {
				t.Helper()
				assert.Equal(t, "arn:aws:iam::000000000000:role/r", aws.ToString(a.S3Action.IamRoleArn))
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k", aws.ToString(a.S3Action.KmsKeyArn))
			},
		},
		{
			name: "lambda_invocation_type",
			action: types.ReceiptAction{LambdaAction: &types.LambdaAction{
				FunctionArn:    aws.String("arn:aws:lambda:us-east-1:000000000000:function:f"),
				InvocationType: types.InvocationTypeRequestResponse,
			}},
			check: func(t *testing.T, a types.ReceiptAction) {
				t.Helper()
				assert.Equal(t, types.InvocationTypeRequestResponse, a.LambdaAction.InvocationType)
			},
		},
		{
			name: "sns_encoding",
			action: types.ReceiptAction{SNSAction: &types.SNSAction{
				TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t"),
				Encoding: types.SNSActionEncodingBase64,
			}},
			check: func(t *testing.T, a types.ReceiptAction) {
				t.Helper()
				assert.Equal(t, types.SNSActionEncodingBase64, a.SNSAction.Encoding)
			},
		},
		{
			name: "workmail",
			action: types.ReceiptAction{WorkmailAction: &types.WorkmailAction{
				OrganizationArn: aws.String("arn:aws:workmail:us-east-1:000000000000:organization/m-1"),
				TopicArn:        aws.String("arn:aws:sns:us-east-1:000000000000:t"),
			}},
			check: func(t *testing.T, a types.ReceiptAction) {
				t.Helper()
				assert.Equal(t, "arn:aws:workmail:us-east-1:000000000000:organization/m-1",
					aws.ToString(a.WorkmailAction.OrganizationArn))
				assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:t", aws.ToString(a.WorkmailAction.TopicArn))
			},
		},
		{
			name: "connect",
			action: types.ReceiptAction{ConnectAction: &types.ConnectAction{
				IAMRoleARN:  aws.String("arn:aws:iam::000000000000:role/r"),
				InstanceARN: aws.String("arn:aws:connect:us-east-1:000000000000:instance/i"),
			}},
			check: func(t *testing.T, a types.ReceiptAction) {
				t.Helper()
				assert.Equal(t, "arn:aws:iam::000000000000:role/r", aws.ToString(a.ConnectAction.IAMRoleARN))
				assert.Equal(t, "arn:aws:connect:us-east-1:000000000000:instance/i",
					aws.ToString(a.ConnectAction.InstanceARN))
			},
		},
		{
			name: "workmail_missing_org",
			action: types.ReceiptAction{WorkmailAction: &types.WorkmailAction{
				TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t"),
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			_, err := c.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
				RuleSetName: aws.String("rs"),
			})
			require.NoError(t, err)

			_, err = c.CreateReceiptRule(t.Context(), &sessdk.CreateReceiptRuleInput{
				RuleSetName: aws.String("rs"),
				Rule: &types.ReceiptRule{
					Name:    aws.String("r"),
					Enabled: true,
					Actions: []types.ReceiptAction{tt.action},
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			out, err := c.DescribeReceiptRule(t.Context(), &sessdk.DescribeReceiptRuleInput{
				RuleSetName: aws.String("rs"),
				RuleName:    aws.String("r"),
			})
			require.NoError(t, err)
			require.Len(t, out.Rule.Actions, 1)
			tt.check(t, out.Rule.Actions[0])
		})
	}
}
