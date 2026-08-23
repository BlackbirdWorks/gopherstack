package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteFunction_Qualifier is a regression test: DeleteFunctionInput's
// Qualifier is a real query-bound member (lambda@v1.101.2 serializers.go
// awsRestjson1_serializeOpHttpBindingsDeleteFunctionInput,
// encoder.SetQuery("Qualifier")) that handleDeleteFunction never read at
// all -- every DeleteFunction call deleted the whole function (all versions
// and aliases), regardless of what Qualifier the client sent. Real AWS
// (api_op_DeleteFunction.go doc comment): "To delete a specific function
// version, use the Qualifier parameter. Otherwise, all versions and aliases
// are deleted" and "You can't delete a version that an alias references."
// The backend already tracked versions (b.versionIndex/b.versions) and
// aliases (b.aliases/b.aliasesByFunction) -- only DeleteFunction's
// enforcement/dispatch ignored the qualifier entirely.
func TestDeleteFunction_Qualifier(t *testing.T) {
	t.Parallel()

	t.Run("qualified_delete_removes_only_that_version", func(t *testing.T) {
		t.Parallel()

		h, _ := newInMemoryHandler(t)
		client := newTestLambdaClient(t, h)

		fnName := "qualified-delete-fn"
		_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
			FunctionName: aws.String(fnName),
			PackageType:  types.PackageTypeImage,
			Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
			Role:         aws.String("arn:aws:iam:::role/r"),
		})
		require.NoError(t, err)

		v1, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{
			FunctionName: aws.String(fnName),
		})
		require.NoError(t, err)

		v2, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{
			FunctionName: aws.String(fnName),
		})
		require.NoError(t, err)

		_, err = client.DeleteFunction(t.Context(), &lambdasdk.DeleteFunctionInput{
			FunctionName: aws.String(fnName),
			Qualifier:    v1.Version,
		})
		require.NoError(t, err)

		// The targeted version is gone.
		_, err = client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
			FunctionName: aws.String(fnName),
			Qualifier:    v1.Version,
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())

		// $LATEST and the other version survive.
		_, err = client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
			FunctionName: aws.String(fnName),
		})
		require.NoError(t, err)

		_, err = client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
			FunctionName: aws.String(fnName),
			Qualifier:    v2.Version,
		})
		require.NoError(t, err)
	})

	t.Run("qualified_delete_blocked_when_alias_references_version", func(t *testing.T) {
		t.Parallel()

		h, _ := newInMemoryHandler(t)
		client := newTestLambdaClient(t, h)

		fnName := "qualified-delete-blocked-fn"
		_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
			FunctionName: aws.String(fnName),
			PackageType:  types.PackageTypeImage,
			Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
			Role:         aws.String("arn:aws:iam:::role/r"),
		})
		require.NoError(t, err)

		v1, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{
			FunctionName: aws.String(fnName),
		})
		require.NoError(t, err)

		_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
			FunctionName:    aws.String(fnName),
			Name:            aws.String("live"),
			FunctionVersion: v1.Version,
		})
		require.NoError(t, err)

		_, err = client.DeleteFunction(t.Context(), &lambdasdk.DeleteFunctionInput{
			FunctionName: aws.String(fnName),
			Qualifier:    v1.Version,
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ResourceConflictException", apiErr.ErrorCode())

		// The version must still exist since the delete was rejected.
		_, err = client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
			FunctionName: aws.String(fnName),
			Qualifier:    v1.Version,
		})
		require.NoError(t, err)
	})

	t.Run("unqualified_delete_still_removes_whole_function", func(t *testing.T) {
		t.Parallel()

		h, _ := newInMemoryHandler(t)
		client := newTestLambdaClient(t, h)

		fnName := "unqualified-delete-fn"
		_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
			FunctionName: aws.String(fnName),
			PackageType:  types.PackageTypeImage,
			Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
			Role:         aws.String("arn:aws:iam:::role/r"),
		})
		require.NoError(t, err)

		_, err = client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{
			FunctionName: aws.String(fnName),
		})
		require.NoError(t, err)

		_, err = client.DeleteFunction(t.Context(), &lambdasdk.DeleteFunctionInput{
			FunctionName: aws.String(fnName),
		})
		require.NoError(t, err)

		_, err = client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
			FunctionName: aws.String(fnName),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	})
}
