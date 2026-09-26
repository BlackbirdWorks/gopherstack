package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateStack_GetAttAttributeValidation is the regression suite for
// gopherstack-p7pvq: Fn::GetAtt on an attribute a known resource type
// doesn't support used to silently fall back to the resource's physical ID.
// Real CreateStack rejects this synchronously with a ValidationError
// ("Template error: resource <X> does not support attribute type <Y> in
// Fn::GetAtt"); AWS::Lambda::CodeSigningConfig is one of the table's listed
// types (cfn_attributes_gen.go), with CodeSigningConfigArn/CodeSigningConfigId
// as its only documented attributes.
func TestCreateStack_GetAttAttributeValidation(t *testing.T) {
	t.Parallel()

	t.Run("unknown_attribute_on_listed_type_fails", func(t *testing.T) {
		t.Parallel()

		_, client := newNewerTypesTestClient(t)

		tmpl := `{
"Resources": {"CSC": {"Type": "AWS::Lambda::CodeSigningConfig", "Properties": {
  "AllowedPublishers": {"SigningProfileVersionArns": ["arn:aws:signer:us-east-1:000000000000:/signing-profiles/prof1"]}
}}},
"Outputs": {"Bad": {"Value": {"Fn::GetAtt": ["CSC", "NotARealAttribute"]}}}
}`

		_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName:    aws.String("csc-bad-attr-stack"),
			TemplateBody: aws.String(tmpl),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error")
		assert.Equal(t, "ValidationError", apiErr.ErrorCode())
		assert.Contains(t, apiErr.ErrorMessage(), "does not support attribute type")
		assert.Contains(t, apiErr.ErrorMessage(), "NotARealAttribute")
	})

	t.Run("documented_attribute_on_listed_type_succeeds", func(t *testing.T) {
		t.Parallel()

		_, client := newNewerTypesTestClient(t)

		tmpl := `{
"Resources": {"CSC": {"Type": "AWS::Lambda::CodeSigningConfig", "Properties": {
  "AllowedPublishers": {"SigningProfileVersionArns": ["arn:aws:signer:us-east-1:000000000000:/signing-profiles/prof1"]}
}}},
"Outputs": {"Id": {"Value": {"Fn::GetAtt": ["CSC", "CodeSigningConfigId"]}}}
}`

		outputs := createStackAndGetOutputs(t, client, "csc-good-attr-stack", tmpl)
		assert.NotEmpty(t, outputs["Id"])
	})

	t.Run("unknown_attribute_on_unlisted_type_falls_back", func(t *testing.T) {
		t.Parallel()

		_, client := newNewerTypesTestClient(t)

		// AWS::CodeArtifact::Domain isn't in cfn_attributes_gen.go's table
		// (see PARITY.md's gopherstack-p7pvq note: it's dropped whole rather
		// than partially, since not every attribute this backend stashes for
		// it can be safely re-quoted there) -- any attribute on it must still
		// fall back to the resource's physical ID, not error.
		tmpl := `{
"Resources": {
  "Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "gaa-domain"}}
},
"Outputs": {"Fallback": {"Value": {"Fn::GetAtt": ["Dom", "SomeFieldThisBackendDoesNotModel"]}}}
}`

		outputs := createStackAndGetOutputs(t, client, "gaa-fallback-stack", tmpl)
		assert.NotEmpty(t, outputs["Fallback"])
	})
}
