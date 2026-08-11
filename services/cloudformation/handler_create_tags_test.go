package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOpsWithTags_RoundTrip drives every cloudformation Create* op
// whose real Input struct accepts Tags (cloudformation@v1.76.1:
// api_op_CreateStack.go, api_op_CreateStackSet.go, api_op_CreateChangeSet.go,
// all `Tags []types.Tag`) through the real SDK client. CloudFormation has no
// TagResource/ListTagsForResource API -- tags are read back only through each
// resource's own Describe* response, so each subtest asserts against that
// (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	wantTags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	t.Run("stack", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName:    aws.String("tagged-stack"),
			TemplateBody: aws.String(simpleTemplate),
			Tags:         wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{
			StackName: aws.String("tagged-stack"),
		})
		require.NoError(t, err)
		require.Len(t, got.Stacks, 1)
		assert.Equal(t, wantTags, got.Stacks[0].Tags)
	})

	t.Run("stack set", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
			StackSetName: aws.String("tagged-stackset"),
			TemplateBody: aws.String(simpleTemplate),
			Tags:         wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeStackSet(t.Context(), &cfnsdk.DescribeStackSetInput{
			StackSetName: aws.String("tagged-stackset"),
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.StackSet.Tags)
	})

	t.Run("change set", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
			StackName:     aws.String("tagged-cs-stack"),
			ChangeSetName: aws.String("tagged-cs"),
			TemplateBody:  aws.String(simpleTemplate),
			ChangeSetType: types.ChangeSetTypeCreate,
			Tags:          wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
			ChangeSetName: aws.String("tagged-cs"),
			StackName:     aws.String("tagged-cs-stack"),
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})
}
