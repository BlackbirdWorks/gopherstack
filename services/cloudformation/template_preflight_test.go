package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_SynchronousValidation(t *testing.T) {
	t.Parallel()

	const bucket = `{"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`

	tests := []struct {
		name     string
		stack    string
		template string
		wantMsg  string
		params   []cfnsdktypes.Parameter
	}{
		{name: "bad_name", stack: "1bad_name", template: bucket, wantMsg: "failed to satisfy constraint"},
		{
			name:     "no_template",
			stack:    "ok",
			template: "",
			wantMsg:  "Either Template URL or Template Body must be specified.",
		},
		{
			name:     "bad_json",
			stack:    "ok",
			template: "{bad",
			wantMsg:  "Template format error: JSON not well-formed. (line 1, column 2)",
		},
		{
			name:     "no_resources",
			stack:    "ok",
			template: `{"Parameters":{}}`,
			wantMsg:  "At least one Resources member must be defined.",
		},
		{
			name: "missing_type", stack: "ok", template: `{"Resources":{"A":{}}}`,
			wantMsg: "[/Resources/A] Every Resources object must contain a Type member.",
		},
		{
			name: "unresolved_ref", stack: "ok",
			template: `{"Resources":{"A":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":{"Ref":"Nope"}}}}}`,
			wantMsg:  "Unresolved resource dependencies [Nope] in the Resources block of the template",
		},
		{
			name: "unresolved_depends_on", stack: "ok",
			template: `{"Resources":{"A":{"Type":"AWS::SNS::Topic","DependsOn":"Zed"}}}`,
			wantMsg:  "Unresolved resource dependencies [Zed]",
		},
		{
			name:  "circular",
			stack: "ok",
			template: `{"Resources":{"A":{"Type":"AWS::SNS::Topic","DependsOn":"B"},` +
				`"B":{"Type":"AWS::SNS::Topic","DependsOn":"A"}}}`,
			wantMsg: "Circular dependency between resources: [A, B]",
		},
		{
			name: "missing_param", stack: "ok",
			template: `{"Parameters":{"P":{"Type":"String"}},"Resources":{"A":{"Type":"AWS::S3::Bucket"}}}`,
			wantMsg:  "Parameters: [P] must have values",
		},
		{
			name: "extra_param", stack: "ok", template: bucket,
			params:  []cfnsdktypes.Parameter{{ParameterKey: aws.String("Z"), ParameterValue: aws.String("1")}},
			wantMsg: "Parameters: [Z] do not exist in the template",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			in := &cfnsdk.CreateStackInput{StackName: aws.String(tt.stack), Parameters: tt.params}
			if tt.template != "" {
				in.TemplateBody = aws.String(tt.template)
			}

			_, err := client.CreateStack(t.Context(), in)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationError", apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)

			_, err = client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(tt.stack)})
			require.Error(t, err, "a rejected CreateStack must not leave a stack behind")
		})
	}
}

func TestUpdateStack_Realism(t *testing.T) {
	t.Parallel()

	const (
		v1 = `{"Parameters":{"P":{"Type":"String","Default":"d"}},"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`
		v2 = `{"Description":"v2","Parameters":{"P":{"Type":"String","Default":"d"}},` +
			`"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`
	)

	tests := []struct {
		name    string
		stack   string
		update  *cfnsdk.UpdateStackInput
		wantMsg string
	}{
		{
			name:    "no_changes",
			stack:   "up",
			update:  &cfnsdk.UpdateStackInput{TemplateBody: aws.String(v1)},
			wantMsg: "No updates are to be performed.",
		},
		{
			name:    "missing_stack",
			stack:   "nope",
			update:  &cfnsdk.UpdateStackInput{TemplateBody: aws.String(v1)},
			wantMsg: "Stack [nope] does not exist",
		},
		{
			name:    "no_template",
			stack:   "up",
			update:  &cfnsdk.UpdateStackInput{},
			wantMsg: "Either Template URL or Template Body must be specified.",
		},
		{
			name:    "empty_resources",
			stack:   "up",
			update:  &cfnsdk.UpdateStackInput{TemplateBody: aws.String(`{"Resources":{}}`)},
			wantMsg: "Template format error: At least one Resources member must be defined.",
		},
		{
			name:  "use_previous_unknown_param",
			stack: "up",
			update: &cfnsdk.UpdateStackInput{
				TemplateBody: aws.String(v2),
				Parameters: []cfnsdktypes.Parameter{
					{ParameterKey: aws.String("Q"), UsePreviousValue: aws.Bool(true)},
				},
			},
			wantMsg: "Parameters: [Q] must have values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("up"), TemplateBody: aws.String(v1),
			})
			require.NoError(t, err)

			tt.update.StackName = aws.String(tt.stack)
			_, err = client.UpdateStack(t.Context(), tt.update)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationError", apiErr.ErrorCode())
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

func TestUpdateStack_UsePreviousValueKept(t *testing.T) {
	t.Parallel()

	const tmpl = `{"Parameters":{"P":{"Type":"String","Default":"d"}},"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`

	client := newTestHandlerAndClient(t)
	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String("keep"), TemplateBody: aws.String(tmpl),
		Parameters: []cfnsdktypes.Parameter{{ParameterKey: aws.String("P"), ParameterValue: aws.String("orig")}},
	})
	require.NoError(t, err)

	_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String("keep"), TemplateBody: aws.String(withDescription(tmpl)),
		Parameters: []cfnsdktypes.Parameter{{ParameterKey: aws.String("P"), UsePreviousValue: aws.Bool(true)}},
	})
	require.NoError(t, err)

	out, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("keep")})
	require.NoError(t, err)
	require.Len(t, out.Stacks, 1)
	require.Len(t, out.Stacks[0].Parameters, 1)
	assert.Equal(t, "orig", aws.ToString(out.Stacks[0].Parameters[0].ParameterValue))
}

func withDescription(tmpl string) string {
	return `{"Description":"changed",` + tmpl[1:]
}

func TestNotFoundMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		call    func(c *cfnsdk.Client) error
		wantMsg string
	}{
		{
			name: "describe_stacks",
			call: func(c *cfnsdk.Client) error {
				_, err := c.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("nope")})

				return err
			},
			wantMsg: "Stack with id nope does not exist",
		},
		{
			name: "describe_resource",
			call: func(c *cfnsdk.Client) error {
				_, err := c.DescribeStackResource(t.Context(), &cfnsdk.DescribeStackResourceInput{
					StackName: aws.String("s"), LogicalResourceId: aws.String("Zed"),
				})

				return err
			},
			wantMsg: "Resource Zed does not exist for stack s",
		},
		{
			name: "describe_change_set",
			call: func(c *cfnsdk.Client) error {
				_, err := c.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
					StackName: aws.String("s"), ChangeSetName: aws.String("cs"),
				})

				return err
			},
			wantMsg: "ChangeSet [cs] does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("s"), TemplateBody: aws.String(`{"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`),
			})
			require.NoError(t, err)

			err = tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

func TestListStacks_RejectsUnknownStatusFilter(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.ListStacks(t.Context(), &cfnsdk.ListStacksInput{
		StackStatusFilter: []cfnsdktypes.StackStatus{"BOGUS"},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationError", apiErr.ErrorCode())
	assert.Contains(t, apiErr.ErrorMessage(), "stackStatusFilter")
}
