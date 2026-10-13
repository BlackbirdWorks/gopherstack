package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateChangeSet_CreateTypeReviewLifecycle(t *testing.T) {
	t.Parallel()

	const tmpl = `{"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`

	tests := []struct {
		name    string
		want    cfnsdktypes.StackStatus
		execute bool
	}{
		{name: "before_execute", execute: false, want: cfnsdktypes.StackStatusReviewInProgress},
		{name: "after_execute", execute: true, want: cfnsdktypes.StackStatusCreateComplete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			cs, err := client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
				StackName: aws.String("rev"), ChangeSetName: aws.String("cs"), TemplateBody: aws.String(tmpl),
				ChangeSetType: cfnsdktypes.ChangeSetTypeCreate,
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(cs.StackId))

			_, err = client.UpdateStack(ctx, &cfnsdk.UpdateStackInput{
				StackName: aws.String("rev"), TemplateBody: aws.String(tmpl),
			})
			require.ErrorContains(t, err, "REVIEW_IN_PROGRESS state and can not be updated")

			if tt.execute {
				_, err = client.ExecuteChangeSet(ctx, &cfnsdk.ExecuteChangeSetInput{
					StackName: aws.String("rev"), ChangeSetName: aws.String("cs"),
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{StackName: aws.String("rev")})
			require.NoError(t, err)
			require.Len(t, out.Stacks, 1)
			assert.Equal(t, tt.want, out.Stacks[0].StackStatus)
			assert.Equal(t, aws.ToString(cs.StackId), aws.ToString(out.Stacks[0].StackId))
		})
	}
}
