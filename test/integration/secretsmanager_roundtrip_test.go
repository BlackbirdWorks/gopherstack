package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SecretsManager_ResourcePolicyRoundTrip drives PutResourcePolicy,
// GetResourcePolicy and DeleteResourcePolicy through a real client -- none of the
// three had ever been called by a typed client before this test.
func TestIntegration_SecretsManager_ResourcePolicyRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-respol-" + uuid.NewString()

	createOut, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("respol-value"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteSecret(cleanupCtx, &secretsmanager.DeleteSecretInput{
			SecretId: aws.String(secretName), ForceDeleteWithoutRecovery: aws.Bool(true),
		})
	})

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::123456789012:root"},` +
		`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`

	putOut, err := client.PutResourcePolicy(ctx, &secretsmanager.PutResourcePolicyInput{
		SecretId:       aws.String(secretName),
		ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(createOut.ARN), aws.ToString(putOut.ARN))

	getOut, err := client.GetResourcePolicy(ctx, &secretsmanager.GetResourcePolicyInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ResourcePolicy)
	assert.JSONEq(t, policy, aws.ToString(getOut.ResourcePolicy))

	_, err = client.DeleteResourcePolicy(ctx, &secretsmanager.DeleteResourcePolicyInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)

	getOut2, err := client.GetResourcePolicy(ctx, &secretsmanager.GetResourcePolicyInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(getOut2.ResourcePolicy), "DeleteResourcePolicy should clear the policy")
}

// TestIntegration_SecretsManager_TagResourceRoundTrip drives TagResource and
// UntagResource through a real client and reads the effect back via DescribeSecret,
// asserting the actual tag set, not just a 200. Neither op had ever been called by
// a typed client before this test.
func TestIntegration_SecretsManager_TagResourceRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-tags-" + uuid.NewString()

	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("tag-value"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteSecret(cleanupCtx, &secretsmanager.DeleteSecretInput{
			SecretId: aws.String(secretName), ForceDeleteWithoutRecovery: aws.Bool(true),
		})
	})

	_, err = client.TagResource(ctx, &secretsmanager.TagResourceInput{
		SecretId: aws.String(secretName),
		Tags: []smtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("gopherstack")},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)

	got := make(map[string]string, len(descOut.Tags))
	for _, tag := range descOut.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(t, map[string]string{"env": "test", "team": "gopherstack"}, got)

	_, err = client.UntagResource(ctx, &secretsmanager.UntagResourceInput{
		SecretId: aws.String(secretName),
		TagKeys:  []string{"env"},
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)

	got2 := make(map[string]string, len(descOut2.Tags))
	for _, tag := range descOut2.Tags {
		got2[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(t, map[string]string{"team": "gopherstack"}, got2, "UntagResource should drop only the removed key")
}

// TestIntegration_SecretsManager_UpdateSecretVersionStageRoundTrip creates two
// versions of a secret, moves the AWSCURRENT stage back to the first version via
// UpdateSecretVersionStage, and confirms both GetSecretValue and
// ListSecretVersionIds observe the move. Neither op had ever been called by a
// typed client before this test.
func TestIntegration_SecretsManager_UpdateSecretVersionStageRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-stage-" + uuid.NewString()

	createOut, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("version-1"),
	})
	require.NoError(t, err)
	v1 := aws.ToString(createOut.VersionId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteSecret(cleanupCtx, &secretsmanager.DeleteSecretInput{
			SecretId: aws.String(secretName), ForceDeleteWithoutRecovery: aws.Bool(true),
		})
	})

	putOut, err := client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretName),
		SecretString: aws.String("version-2"),
	})
	require.NoError(t, err)
	v2 := aws.ToString(putOut.VersionId)
	require.NotEqual(t, v1, v2)

	getOut, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretName)})
	require.NoError(t, err)
	require.Equal(t, "version-2", aws.ToString(getOut.SecretString), "AWSCURRENT should be the just-put version")

	_, err = client.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
		SecretId:            aws.String(secretName),
		VersionStage:        aws.String("AWSCURRENT"),
		MoveToVersionId:     aws.String(v1),
		RemoveFromVersionId: aws.String(v2),
	})
	require.NoError(t, err)

	getOut2, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretName)})
	require.NoError(t, err)
	assert.Equal(t, "version-1", aws.ToString(getOut2.SecretString), "AWSCURRENT should have moved to v1")

	listOut, err := client.ListSecretVersionIds(ctx, &secretsmanager.ListSecretVersionIdsInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)

	stages := make(map[string][]string, len(listOut.Versions))
	for _, v := range listOut.Versions {
		stages[aws.ToString(v.VersionId)] = v.VersionStages
	}

	assert.Contains(t, stages[v1], "AWSCURRENT", "v1 should now carry AWSCURRENT")
	assert.Contains(t, stages[v2], "AWSPREVIOUS", "v2 should have been demoted to AWSPREVIOUS")
	assert.NotContains(t, stages[v2], "AWSCURRENT", "v2 should no longer carry AWSCURRENT")
}

// TestIntegration_SecretsManager_UpdateSecretRoundTrip drives UpdateSecret
// through a real client -- flagged by gopherstack-n3zi as a high-traffic op
// with zero typed-client coverage -- and reads the effect back via
// DescribeSecret and GetSecretValue. It covers both of UpdateSecret's two
// independent effects: updating Description (metadata-only, no new version)
// and supplying a new SecretString (creates a version and moves AWSCURRENT,
// demoting the prior version to AWSPREVIOUS -- the same mechanism
// UpdateSecretVersionStage exercises explicitly).
func TestIntegration_SecretsManager_UpdateSecretRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSecretsManagerClient(t)
	ctx := t.Context()

	secretName := "test-updatesecret-" + uuid.NewString()

	createOut, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("original-value"),
	})
	require.NoError(t, err)
	v1 := aws.ToString(createOut.VersionId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteSecret(cleanupCtx, &secretsmanager.DeleteSecretInput{
			SecretId: aws.String(secretName), ForceDeleteWithoutRecovery: aws.Bool(true),
		})
	})

	updateDescOut, err := client.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretId:    aws.String(secretName),
		Description: aws.String("updated via UpdateSecret"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(createOut.ARN), aws.ToString(updateDescOut.ARN))
	assert.Empty(t, aws.ToString(updateDescOut.VersionId), "description-only update should not create a version")

	descOut, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(secretName)})
	require.NoError(t, err)
	assert.Equal(t, "updated via UpdateSecret", aws.ToString(descOut.Description))

	updateValOut, err := client.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretId:     aws.String(secretName),
		SecretString: aws.String("new-value"),
	})
	require.NoError(t, err)
	v2 := aws.ToString(updateValOut.VersionId)
	require.NotEmpty(t, v2)
	require.NotEqual(t, v1, v2)

	getOut, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretName)})
	require.NoError(t, err)
	assert.Equal(
		t,
		"new-value",
		aws.ToString(getOut.SecretString),
		"AWSCURRENT should be the version UpdateSecret just wrote",
	)
	assert.Equal(t, v2, aws.ToString(getOut.VersionId))

	listOut, err := client.ListSecretVersionIds(ctx, &secretsmanager.ListSecretVersionIdsInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)

	stagesByVersion := make(map[string][]string, len(listOut.Versions))
	for _, v := range listOut.Versions {
		stagesByVersion[aws.ToString(v.VersionId)] = v.VersionStages
	}

	assert.Contains(t, stagesByVersion[v2], "AWSCURRENT", "the new version should carry AWSCURRENT")
	assert.Contains(t, stagesByVersion[v1], "AWSPREVIOUS", "the old version should have been demoted to AWSPREVIOUS")
	assert.NotContains(t, stagesByVersion[v1], "AWSCURRENT", "the old version should no longer carry AWSCURRENT")
}
