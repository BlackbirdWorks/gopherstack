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

// TestUpdateFunctionConfiguration_MemorySizeTimeout_PreserveOmitRejectZero
// proves the zeroguard-widening fix (cmd/zeroguard's int/int32 kind
// widening, gopherstack-2470): the real SDK declares both MemorySize and
// Timeout *int32, but gopherstack's UpdateFunctionConfigurationInput had
// them as plain int guarded by `> 0`, so an omitted update field and an
// explicit 0 were indistinguishable, and neither was ever range-validated
// on Update.
func TestUpdateFunctionConfiguration_MemorySizeTimeout_PreserveOmitRejectZero(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)

	fnName := "update-omit-memory-timeout-fn"
	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
		MemorySize:   aws.Int32(256),
		Timeout:      aws.Int32(30),
	})
	require.NoError(t, err)

	withValues, err := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		MemorySize:   aws.Int32(512),
		Timeout:      aws.Int32(60),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(512), aws.ToInt32(withValues.MemorySize))
	assert.Equal(t, int32(60), aws.ToInt32(withValues.Timeout))

	// Omitted MemorySize/Timeout must preserve the stored values.
	preserved, err := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		Description:  aws.String("unrelated field changed"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(512), aws.ToInt32(preserved.MemorySize), "omitted MemorySize must survive")
	assert.Equal(t, int32(60), aws.ToInt32(preserved.Timeout), "omitted Timeout must survive")

	// An explicit zero is not "use default" on Update -- it must be
	// rejected with the SDK's declared InvalidParameterValueException, and
	// leave the stored configuration untouched.
	_, err = client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		MemorySize:   aws.Int32(0),
	})
	var ipve *types.InvalidParameterValueException
	require.ErrorAs(t, err, &ipve)

	_, err = client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
		Timeout:      aws.Int32(0),
	})
	require.ErrorAs(t, err, &ipve)

	unchanged, err := client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
		FunctionName: aws.String(fnName),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(512), aws.ToInt32(unchanged.MemorySize), "rejected update must not mutate MemorySize")
	assert.Equal(t, int32(60), aws.ToInt32(unchanged.Timeout), "rejected update must not mutate Timeout")
}

func TestUpdateEventSourceMapping_PreservesOmittedBatchSize(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)

	fnName := "update-omit-esm-batchsize-fn"
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
		BatchSize:      aws.Int32(7),
	})
	require.NoError(t, err)
	require.Equal(t, int32(7), aws.ToInt32(esm.BatchSize))

	preserved, err := client.UpdateEventSourceMapping(t.Context(), &lambdasdk.UpdateEventSourceMappingInput{
		UUID:    esm.UUID,
		Enabled: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(7), aws.ToInt32(preserved.BatchSize), "omitted BatchSize must survive")

	changed, err := client.UpdateEventSourceMapping(t.Context(), &lambdasdk.UpdateEventSourceMappingInput{
		UUID:      esm.UUID,
		BatchSize: aws.Int32(42),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(42), aws.ToInt32(changed.BatchSize))
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
