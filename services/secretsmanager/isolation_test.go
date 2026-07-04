package secretsmanager //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func smCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func TestSecretsManagerRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	ctxEast := smCtxRegion("us-east-1")
	ctxWest := smCtxRegion("us-west-2")

	// 1. Create a secret with the same name in us-east-1.
	eastOut, err := backend.CreateSecret(ctxEast, &CreateSecretInput{Name: "shared", SecretString: "east-value"})
	require.NoError(t, err)
	assert.Contains(t, eastOut.ARN, "us-east-1")

	// 2. Create a secret with the SAME NAME in us-west-2.
	westOut, err := backend.CreateSecret(ctxWest, &CreateSecretInput{Name: "shared", SecretString: "west-value"})
	require.NoError(t, err)
	assert.Contains(t, westOut.ARN, "us-west-2")

	// 3. us-east-1 reads its own value.
	eastVal, err := backend.GetSecretValue(ctxEast, &GetSecretValueInput{SecretID: "shared"})
	require.NoError(t, err)
	assert.Equal(t, "east-value", eastVal.SecretString)
	assert.Contains(t, eastVal.ARN, "us-east-1")

	// 4. us-west-2 reads its own value.
	westVal, err := backend.GetSecretValue(ctxWest, &GetSecretValueInput{SecretID: "shared"})
	require.NoError(t, err)
	assert.Equal(t, "west-value", westVal.SecretString)
	assert.Contains(t, westVal.ARN, "us-west-2")

	// Each region lists exactly one secret.
	eastList, err := backend.ListSecrets(ctxEast, &ListSecretsInput{})
	require.NoError(t, err)
	require.Len(t, eastList.SecretList, 1)

	westList, err := backend.ListSecrets(ctxWest, &ListSecretsInput{})
	require.NoError(t, err)
	require.Len(t, westList.SecretList, 1)

	// 5. Force-delete in us-east-1; us-west-2's secret remains.
	_, err = backend.DeleteSecret(ctxEast, &DeleteSecretInput{
		SecretID:                   "shared",
		ForceDeleteWithoutRecovery: true,
	})
	require.NoError(t, err)

	_, err = backend.GetSecretValue(ctxEast, &GetSecretValueInput{SecretID: "shared"})
	require.Error(t, err)

	stillThere, err := backend.GetSecretValue(ctxWest, &GetSecretValueInput{SecretID: "shared"})
	require.NoError(t, err)
	assert.Equal(t, "west-value", stillThere.SecretString)
}

func TestSecretsManagerResourcePolicyRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	ctxEast := smCtxRegion("us-east-1")
	ctxWest := smCtxRegion("us-west-2")

	_, err := backend.CreateSecret(ctxEast, &CreateSecretInput{Name: "p", SecretString: "v"})
	require.NoError(t, err)
	_, err = backend.CreateSecret(ctxWest, &CreateSecretInput{Name: "p", SecretString: "v"})
	require.NoError(t, err)

	_, err = backend.PutResourcePolicy(ctxEast, &PutResourcePolicyInput{
		SecretID:       "p",
		ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)

	// us-east-1 has the policy; us-west-2 does not.
	eastPol, err := backend.GetResourcePolicy(ctxEast, &GetResourcePolicyInput{SecretID: "p"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, eastPol.ResourcePolicy)

	westPol, err := backend.GetResourcePolicy(ctxWest, &GetResourcePolicyInput{SecretID: "p"})
	require.NoError(t, err)
	assert.Empty(t, westPol.ResourcePolicy)
}
