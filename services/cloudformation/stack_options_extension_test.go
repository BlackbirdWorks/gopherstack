package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateStackRetainExceptOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		retainExceptOnCreate bool
		wantBucketSurvives   bool
	}{
		{name: "default retains the new Retain resource", wantBucketSurvives: true},
		{name: "retain except on create deletes it", retainExceptOnCreate: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, backends, client := newQueueFailingClient(t)

			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName:    aws.String("update-retain"),
				TemplateBody: aws.String(`{"Resources":{"Topic":{"Type":"AWS::SNS::Topic","Properties":{}}}}`),
			})
			require.NoError(t, err)

			tmpl := `{"Resources":{"Topic":{"Type":"AWS::SNS::Topic","Properties":{}},` +
				`"MyBucket":{"Type":"AWS::S3::Bucket","DeletionPolicy":"Retain","Properties":{}},` +
				`"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`

			_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName: aws.String("update-retain"), TemplateBody: aws.String(tmpl),
				RetainExceptOnCreate: aws.Bool(tt.retainExceptOnCreate),
			})
			require.NoError(t, err)

			out, err := client.DescribeStacks(
				t.Context(),
				&cfnsdk.DescribeStacksInput{StackName: aws.String("update-retain")},
			)
			require.NoError(t, err)
			require.Len(t, out.Stacks, 1)
			assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", string(out.Stacks[0].StackStatus))

			events, err := backend.DescribeEvents("update-retain", "", false)
			require.NoError(t, err)

			var bucket string

			for _, e := range events.Data {
				if e.LogicalResourceID == "MyBucket" && e.ResourceStatus == "CREATE_COMPLETE" {
					bucket = e.PhysicalResourceID
				}
			}

			require.NotEmpty(t, bucket)

			_, headErr := backends.S3.Backend.HeadBucket(
				t.Context(),
				&awss3.HeadBucketInput{Bucket: aws.String(bucket)},
			)
			if tt.wantBucketSurvives {
				assert.NoError(t, headErr)
			} else {
				assert.Error(t, headErr)
			}
		})
	}
}

func TestActivateTypeAliasAndValidation(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	activate := func(in *cfnsdk.ActivateTypeInput) (*cfnsdk.ActivateTypeOutput, error) {
		return client.ActivateType(t.Context(), in)
	}

	out, err := activate(&cfnsdk.ActivateTypeInput{
		Type: "RESOURCE", TypeName: aws.String("Acme::Widgets::Gadget"), PublisherId: aws.String("abcdef123456"),
		TypeNameAlias: aws.String("Mine::Widgets::Gadget"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.Arn), "Mine-Widgets-Gadget")

	desc, err := client.DescribeType(t.Context(), &cfnsdk.DescribeTypeInput{Arn: out.Arn})
	require.NoError(t, err)
	assert.Equal(t, "Mine::Widgets::Gadget", aws.ToString(desc.TypeName))
	assert.Equal(t, "Acme::Widgets::Gadget", aws.ToString(desc.OriginalTypeName))
	assert.NotEmpty(t, aws.ToString(desc.OriginalTypeArn))

	tests := []struct {
		name string
		in   *cfnsdk.ActivateTypeInput
		code string
	}{
		{name: "alias already used", code: "CFNRegistryException", in: &cfnsdk.ActivateTypeInput{
			TypeName: aws.String("Acme::Widgets::Other"), TypeNameAlias: aws.String("Mine::Widgets::Gadget"),
		}},
		{name: "bad alias", code: "ValidationError", in: &cfnsdk.ActivateTypeInput{
			TypeName: aws.String("Acme::Widgets::Other"), TypeNameAlias: aws.String("nope"),
		}},
		{name: "major and bump", code: "ValidationError", in: &cfnsdk.ActivateTypeInput{
			TypeName: aws.String("Acme::Widgets::Other"), MajorVersion: aws.Int64(2), VersionBump: "MINOR",
		}},
		{name: "major out of range", code: "ValidationError", in: &cfnsdk.ActivateTypeInput{
			TypeName: aws.String("Acme::Widgets::Other"), MajorVersion: aws.Int64(0),
		}},
	}

	for _, tt := range tests {
		_, err = activate(tt.in)
		require.Error(t, err, tt.name)
		assert.Contains(t, err.Error(), tt.code, tt.name)
	}
}
