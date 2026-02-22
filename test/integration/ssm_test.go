package integration_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_SSM_GetParameterHistory_InitialVersion(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "test-param-" + uuid.NewString()

	// Create initial parameter
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("version-1"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	// Get history for initial version
	historyResp, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name: aws.String(paramName),
	})
	require.NoError(t, err)
	require.NotNil(t, historyResp.Parameters)
	require.Len(t, historyResp.Parameters, 1)
	assert.Equal(t, paramName, *historyResp.Parameters[0].Name)
	assert.Equal(t, "version-1", *historyResp.Parameters[0].Value)
}

func TestIntegration_SSM_GetParameterHistory_MultipleVersions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "test-param-multi-" + uuid.NewString()

	// Create and update parameter multiple times
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("version-1"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(paramName),
		Value:     aws.String("version-2"),
		Type:      types.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(paramName),
		Value:     aws.String("version-3"),
		Type:      types.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	// Get history - should have all 3 versions
	historyResp, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name: aws.String(paramName),
	})
	require.NoError(t, err)
	require.NotNil(t, historyResp.Parameters)
	require.Len(t, historyResp.Parameters, 3)

	// Verify reverse order (newest first)
	assert.Equal(t, int64(3), historyResp.Parameters[0].Version)
	assert.Equal(t, "version-3", *historyResp.Parameters[0].Value)

	assert.Equal(t, int64(2), historyResp.Parameters[1].Version)
	assert.Equal(t, "version-2", *historyResp.Parameters[1].Value)

	assert.Equal(t, int64(1), historyResp.Parameters[2].Version)
	assert.Equal(t, "version-1", *historyResp.Parameters[2].Value)
}

func TestIntegration_SSM_GetParameterHistory_WithMaxResults(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "test-param-maxresults-" + uuid.NewString()

	// Create multiple versions (5 versions)
	for i := 1; i <= 5; i++ {
		overwrite := i > 1
		_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      aws.String(paramName),
			Value:     aws.String("version-" + strconv.Itoa(i)),
			Type:      types.ParameterTypeString,
			Overwrite: aws.Bool(overwrite),
		})
		require.NoError(t, err)
	}

	// Get history with MaxResults limit
	maxResults := int32(2)
	historyResp, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name:       aws.String(paramName),
		MaxResults: aws.Int32(maxResults),
	})
	require.NoError(t, err)
	require.NotNil(t, historyResp.Parameters)
	require.Len(t, historyResp.Parameters, 2)

	// Should return the latest 2 versions
	assert.Equal(t, int64(5), historyResp.Parameters[0].Version)
	assert.Equal(t, "version-5", *historyResp.Parameters[0].Value)

	assert.Equal(t, int64(4), historyResp.Parameters[1].Version)
	assert.Equal(t, "version-4", *historyResp.Parameters[1].Value)
}

func TestIntegration_SSM_GetParameterHistory_ParameterNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "nonexistent-param-" + uuid.NewString()

	// Try to get history for non-existent parameter
	_, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name: aws.String(paramName),
	})
	require.Error(t, err)
	// The error should be a ParameterNotFound error
	assert.Contains(t, err.Error(), "ParameterNotFound")
}

func TestIntegration_SSM_SecureString_PutAndGetEncrypted(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "secure-param-" + uuid.NewString()

	// Create a SecureString parameter
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("super-secret-value"),
		Type:  types.ParameterTypeSecureString,
	})
	require.NoError(t, err)

	// Get without decryption - should be encrypted
	getResp, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(paramName),
		WithDecryption: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ParameterTypeSecureString, getResp.Parameter.Type)
	assert.NotEqual(t, "super-secret-value", *getResp.Parameter.Value)
	assert.NotEmpty(t, *getResp.Parameter.Value) // Should be encrypted
}

func TestIntegration_SSM_SecureString_GetWithDecryption(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "secure-param-" + uuid.NewString()

	// Create a SecureString parameter
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("super-secret-value"),
		Type:  types.ParameterTypeSecureString,
	})
	require.NoError(t, err)

	// Get with decryption - should be decrypted
	getResp, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(paramName),
		WithDecryption: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ParameterTypeSecureString, getResp.Parameter.Type)
	assert.Equal(t, "super-secret-value", *getResp.Parameter.Value)
}

func TestIntegration_SSM_SecureString_GetMultipleParameters(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	// Create mixed parameter types
	secureParam := "secure-" + uuid.NewString()
	stringParam := "string-" + uuid.NewString()

	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(secureParam),
		Value: aws.String("secure-value"),
		Type:  types.ParameterTypeSecureString,
	})
	require.NoError(t, err)

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(stringParam),
		Value: aws.String("plain-value"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	// Get both with decryption
	getResp, err := client.GetParameters(ctx, &ssm.GetParametersInput{
		Names:          []string{secureParam, stringParam},
		WithDecryption: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, getResp.Parameters, 2)

	// Verify both are decrypted/plain
	for _, param := range getResp.Parameters {
		switch *param.Name {
		case secureParam:
			assert.Equal(t, "secure-value", *param.Value)
		case stringParam:
			assert.Equal(t, "plain-value", *param.Value)
		}
	}
}
