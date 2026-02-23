package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_SecretsManager_SecretLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-secret-" + uuid.NewString()
	secretValue := "my-super-secret-" + uuid.NewString()

	// CreateSecret
	createOut, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(secretValue),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ARN)
	assert.Contains(t, *createOut.ARN, secretName)

	// DescribeSecret
	descOut, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Equal(t, secretName, *descOut.Name)

	// ListSecrets — secret should appear
	listOut, err := client.ListSecrets(ctx, &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	found := false
	for _, s := range listOut.SecretList {
		if *s.Name == secretName {
			found = true

			break
		}
	}
	assert.True(t, found, "created secret should appear in ListSecrets")

	// DeleteSecret (force immediate deletion)
	_, err = client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String(secretName),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	require.NoError(t, err)

	// Verify gone
	listOut2, err := client.ListSecrets(ctx, &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	for _, s := range listOut2.SecretList {
		assert.NotEqual(t, secretName, *s.Name, "deleted secret should not appear in ListSecrets")
	}
}

func TestIntegration_SecretsManager_PutGetSecretValue(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-pgv-" + uuid.NewString()
	initialValue := "initial-" + uuid.NewString()

	// Create secret with initial string value
	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(initialValue),
	})
	require.NoError(t, err)

	// GetSecretValue — string
	getOut, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.SecretString)
	assert.Equal(t, initialValue, *getOut.SecretString)

	// PutSecretValue — update value
	updatedValue := "updated-" + uuid.NewString()
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretName),
		SecretString: aws.String(updatedValue),
	})
	require.NoError(t, err)

	// GetSecretValue — should return updated value
	getOut2, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut2.SecretString)
	assert.Equal(t, updatedValue, *getOut2.SecretString)
}

func TestIntegration_SecretsManager_BinarySecret(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-bin-" + uuid.NewString()
	binaryData := []byte{0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD}

	// CreateSecret with binary value
	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretBinary: binaryData,
	})
	require.NoError(t, err)

	// GetSecretValue — binary
	getOut, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Equal(t, binaryData, getOut.SecretBinary)
	assert.Nil(t, getOut.SecretString, "binary secret should not have SecretString")

	// PutSecretValue with new binary
	newBinary := []byte{0xAA, 0xBB, 0xCC}
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretName),
		SecretBinary: newBinary,
	})
	require.NoError(t, err)

	getOut2, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Equal(t, newBinary, getOut2.SecretBinary)
}

func TestIntegration_SecretsManager_VersionManagement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-versions-" + uuid.NewString()

	// Create with v1
	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("version-1"),
	})
	require.NoError(t, err)

	// Put v2
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretName),
		SecretString: aws.String("version-2"),
	})
	require.NoError(t, err)

	// Put v3
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretName),
		SecretString: aws.String("version-3"),
	})
	require.NoError(t, err)

	// GetSecretValue — should return current (v3)
	getOut, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Equal(t, "version-3", *getOut.SecretString)

	// DescribeSecret — verify version IDs are tracked
	descOut, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.VersionIdsToStages, "versions should be tracked")
}

func TestIntegration_SecretsManager_RestoreAfterSoftDelete(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-restore-" + uuid.NewString()

	// Create secret
	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("restore-me"),
	})
	require.NoError(t, err)

	// Soft delete (schedule for deletion with recovery window)
	_, err = client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:             aws.String(secretName),
		RecoveryWindowInDays: aws.Int64(7),
	})
	require.NoError(t, err)

	// DescribeSecret — should show deletion date set
	descOut, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.NotNil(t, descOut.DeletedDate, "deleted secret should have DeletedDate set")

	// RestoreSecret
	restoreOut, err := client.RestoreSecret(ctx, &secretsmanager.RestoreSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Equal(t, secretName, *restoreOut.Name)

	// DescribeSecret — deletion date should be cleared
	descOut2, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Nil(t, descOut2.DeletedDate, "restored secret should not have DeletedDate")
}
