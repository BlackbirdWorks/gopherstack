package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/require"
)

const (
	denyModifyMyQueue = `{"Statement":[` +
		`{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"Update:Modify","Principal":"*","Resource":"LogicalResourceId/MyQueue"}]}`
	queueTimeout600 = `{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":600}}}}`
	queueTimeout30  = `{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}}}}`
)

func TestCreateUpdateStack_StackPolicyBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *cfnsdk.Client)
		name string
	}{
		{name: "create stores policy", run: func(t *testing.T, client *cfnsdk.Client) {
			t.Helper()

			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout30),
				StackPolicyBody: aws.String(denyModifyMyQueue),
			})
			require.NoError(t, err)

			got, err := client.GetStackPolicy(t.Context(), &cfnsdk.GetStackPolicyInput{StackName: aws.String("pb")})
			require.NoError(t, err)
			require.JSONEq(t, denyModifyMyQueue, aws.ToString(got.StackPolicyBody))

			_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout600),
			})
			require.ErrorContains(t, err, "Update:Modify")
		}},
		{name: "create rejects malformed policy", run: func(t *testing.T, client *cfnsdk.Client) {
			t.Helper()

			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout30),
				StackPolicyBody: aws.String("{not json"),
			})
			require.ErrorContains(t, err, "malformed stack policy")

			_, err = client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("pb")})
			require.Error(t, err)
		}},
		{name: "update replaces policy", run: func(t *testing.T, client *cfnsdk.Client) {
			t.Helper()

			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout30),
			})
			require.NoError(t, err)

			_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout30),
				StackPolicyBody: aws.String(denyModifyMyQueue),
			})
			require.NoError(t, err)

			got, err := client.GetStackPolicy(t.Context(), &cfnsdk.GetStackPolicyInput{StackName: aws.String("pb")})
			require.NoError(t, err)
			require.JSONEq(t, denyModifyMyQueue, aws.ToString(got.StackPolicyBody))

			_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout600),
			})
			require.ErrorContains(t, err, "Update:Modify")
		}},
		{name: "update rejects malformed policy", run: func(t *testing.T, client *cfnsdk.Client) {
			t.Helper()

			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout30),
			})
			require.NoError(t, err)

			_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("pb"), TemplateBody: aws.String(queueTimeout600),
				StackPolicyBody: aws.String("{not json"),
			})
			require.ErrorContains(t, err, "malformed stack policy")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestHandlerAndClient(t))
		})
	}
}
