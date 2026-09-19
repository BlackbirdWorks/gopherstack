package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests prove the zeroguard fix (cmd/zeroguard): several Update*Input
// fields are pointers on the real SDK, so an omitted PATCH member must
// preserve the stored value and an explicit empty string must clear it.
// Before the fix each field below was plain string guarded by `!= ""`, so
// omitted and explicit-empty were indistinguishable.
func TestUpdateFunctionConfiguration_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)

	fnName := "update-omit-fn"
	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/original"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		Description:  aws.String("first desc"),
		Role:         aws.String("arn:aws:iam:::role/first"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first desc", aws.ToString(updated.Description))
	assert.Equal(t, "arn:aws:iam:::role/first", aws.ToString(updated.Role))

	// Omitted update must preserve every prior value.
	preserved, err := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
	})
	require.NoError(t, err)
	assert.Equal(t, "first desc", aws.ToString(preserved.Description), "omitted description must survive")
	assert.Equal(t, "arn:aws:iam:::role/first", aws.ToString(preserved.Role), "omitted role must survive")

	// Explicit empty string clears description; unrelated fields untouched.
	cleared, err := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		Description:  aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Equal(t, "arn:aws:iam:::role/first", aws.ToString(cleared.Role), "unrelated field untouched")
}

func TestUpdateAlias_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)

	fnName := "update-omit-alias-fn"
	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	v1, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{FunctionName: aws.String(fnName)})
	require.NoError(t, err)

	v2, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{FunctionName: aws.String(fnName)})
	require.NoError(t, err)

	_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
		FunctionName:    aws.String(fnName),
		Name:            aws.String("live"),
		FunctionVersion: v1.Version,
		Description:     aws.String("first desc"),
	})
	require.NoError(t, err)

	// Omitted update must preserve prior FunctionVersion and Description.
	preserved, err := client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
		FunctionName: aws.String(fnName),
		Name:         aws.String("live"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(v1.Version), aws.ToString(preserved.FunctionVersion), "omitted version must survive")
	assert.Equal(t, "first desc", aws.ToString(preserved.Description), "omitted description must survive")

	// Explicit values move the alias and clear the description.
	moved, err := client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
		FunctionName:    aws.String(fnName),
		Name:            aws.String("live"),
		FunctionVersion: v2.Version,
		Description:     aws.String(""),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(v2.Version), aws.ToString(moved.FunctionVersion))
	assert.Empty(t, aws.ToString(moved.Description))
}

func TestUpdateCodeSigningConfig_PreservesOmittedDescription(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)

	created, err := client.CreateCodeSigningConfig(t.Context(), &lambdasdk.CreateCodeSigningConfigInput{
		AllowedPublishers: &types.AllowedPublishers{
			SigningProfileVersionArns: []string{"arn:aws:signer:us-east-1:111122223333:/signing-profiles/p"},
		},
		Description: aws.String("first desc"),
	})
	require.NoError(t, err)

	preserved, err := client.UpdateCodeSigningConfig(t.Context(), &lambdasdk.UpdateCodeSigningConfigInput{
		CodeSigningConfigArn: created.CodeSigningConfig.CodeSigningConfigArn,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"first desc",
		aws.ToString(preserved.CodeSigningConfig.Description),
		"omitted description must survive",
	)

	cleared, err := client.UpdateCodeSigningConfig(t.Context(), &lambdasdk.UpdateCodeSigningConfigInput{
		CodeSigningConfigArn: created.CodeSigningConfig.CodeSigningConfigArn,
		Description:          aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.CodeSigningConfig.Description))
}

func TestUpdateEventSourceMapping_PreservesOmittedKMSKeyArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)

	fnName := "update-omit-esm-fn"
	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	esm, err := client.CreateEventSourceMapping(t.Context(), &lambdasdk.CreateEventSourceMappingInput{
		FunctionName:   aws.String(fnName),
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:111122223333:queue"),
		KMSKeyArn:      aws.String("arn:aws:kms:us-east-1:111122223333:key/first"),
	})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:kms:us-east-1:111122223333:key/first", aws.ToString(esm.KMSKeyArn))

	preserved, err := client.UpdateEventSourceMapping(t.Context(), &lambdasdk.UpdateEventSourceMappingInput{
		UUID: esm.UUID,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:111122223333:key/first", aws.ToString(preserved.KMSKeyArn),
		"omitted KMSKeyArn must survive")

	cleared, err := client.UpdateEventSourceMapping(t.Context(), &lambdasdk.UpdateEventSourceMappingInput{
		UUID:      esm.UUID,
		KMSKeyArn: aws.String(""),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.KMSKeyArn))
}
