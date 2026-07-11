package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestCreateStack_IntrinsicErrorPropagation verifies that templates referencing
// undefined resources via Fn::GetAtt / Fn::Sub, or using an unsupported resource
// type, fail the stack (ROLLBACK_COMPLETE with an accurate StatusReason and a
// CREATE_FAILED event) instead of silently succeeding — while valid templates
// still reach CREATE_COMPLETE.
func TestCreateStack_IntrinsicErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		template       string
		wantStatus     string
		wantReasonPart string
		wantEvent      string
	}{
		{
			name: "getatt_undefined_resource_fails",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Bucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": {"Fn::GetAtt": ["NonExistent", "Arn"]}}
}
}
}`,
			wantStatus:     "ROLLBACK_COMPLETE",
			wantReasonPart: "Fn::GetAtt references undefined resource",
			wantEvent:      "CREATE_FAILED",
		},
		{
			name: "sub_undefined_resource_fails",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Bucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": {"Fn::Sub": "name-${Missing.Arn}"}}
}
}
}`,
			wantStatus:     "ROLLBACK_COMPLETE",
			wantReasonPart: "Fn::Sub references undefined resource",
			wantEvent:      "CREATE_FAILED",
		},
		{
			name: "unsupported_resource_type_fails",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Thing": {
"Type": "NotAValidType",
"Properties": {}
}
}
}`,
			wantStatus:     "ROLLBACK_COMPLETE",
			wantReasonPart: "unsupported resource type",
			wantEvent:      "CREATE_FAILED",
		},
		{
			name: "getatt_defined_resource_succeeds",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "valid-getatt-bucket"}},
"Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": {"Fn::GetAtt": ["Bucket", "Arn"]}}}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "sub_defined_resource_and_pseudo_param_succeeds",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "valid-sub-bucket"}},
"Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": {"Fn::Sub": "${AWS::Region}-${Bucket.Arn}"}}}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "sub_parameter_ref_succeeds",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Parameters": {"Env": {"Type": "String", "Default": "prod"}},
"Resources": {
"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": {"Fn::Sub": "${Env}-bucket"}}}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "sub_two_arg_local_var_succeeds",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": {"Fn::Sub": ["${Local}-bucket", {"Local": "x"}]}}}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "custom_resource_type_succeeds",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyCustom": {
"Type": "Custom::MyThing",
"Properties": {"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:x"}
}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "valid_but_unmodeled_type_still_succeeds",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"Thing": {"Type": "AWS::SomeFuture::Widget", "Properties": {}}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stack, err := b.CreateStack(t.Context(), tt.name, tt.template, nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, stack.StackStatus)

			if tt.wantReasonPart != "" {
				assert.Contains(t, stack.StackStatusReason, tt.wantReasonPart)
			}

			if tt.wantEvent != "" {
				evtPage, evErr := b.DescribeStackEvents(tt.name, "")
				require.NoError(t, evErr)
				events := evtPage.Data
				statuses := make([]string, len(events))
				for i, e := range events {
					statuses[i] = e.ResourceStatus
				}
				assert.Contains(t, statuses, tt.wantEvent)
			}
		})
	}
}

// TestUpdateStack_IntrinsicErrorPropagation verifies the same validation runs on
// UpdateStack and rolls the update back when an intrinsic references an
// undefined resource.
func TestUpdateStack_IntrinsicErrorPropagation(t *testing.T) {
	t.Parallel()

	good := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "upd-intrinsic-bucket"}}}
}`
	bad := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": {"Fn::GetAtt": ["Ghost", "Arn"]}}}}
}`

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "upd-intrinsic", good, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	updated, err := b.UpdateStack(t.Context(), "upd-intrinsic", bad, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", updated.StackStatus)
	assert.Contains(t, updated.StackStatusReason, "Fn::GetAtt references undefined resource")
}
