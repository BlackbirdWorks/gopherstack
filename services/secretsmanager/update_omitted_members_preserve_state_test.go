package secretsmanager_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// These tests prove the zeroguard fix (cmd/zeroguard): UpdateSecretInput's
// Description and Type fields are pointers on the real SDK, so an omitted
// PATCH member must preserve the stored value and an explicit empty string
// must clear it. Before the fix both were plain strings guarded by `!= ""`,
// so omitted and explicit-empty were indistinguishable and a client could
// never clear Description.
func TestUpdateSecret_PreservesOmittedDescription(t *testing.T) {
	t.Parallel()

	client := newTestSMClientWithRegion(
		t,
		secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend()),
		wireFixesRegion,
	)

	_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
		Name: aws.String("update-omit-secret"), SecretString: aws.String("v1"),
	})
	require.NoError(t, err)

	_, err = client.UpdateSecret(t.Context(), &secretsmanagersdk.UpdateSecretInput{
		SecretId:    aws.String("update-omit-secret"),
		Description: aws.String("first desc"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeSecret(t.Context(), &secretsmanagersdk.DescribeSecretInput{
		SecretId: aws.String("update-omit-secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first desc", aws.ToString(desc.Description))

	// Omitted update must preserve the prior description.
	_, err = client.UpdateSecret(t.Context(), &secretsmanagersdk.UpdateSecretInput{
		SecretId: aws.String("update-omit-secret"),
	})
	require.NoError(t, err)

	preserved, err := client.DescribeSecret(t.Context(), &secretsmanagersdk.DescribeSecretInput{
		SecretId: aws.String("update-omit-secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first desc", aws.ToString(preserved.Description), "omitted description must survive")

	// Explicit empty string clears description.
	_, err = client.UpdateSecret(t.Context(), &secretsmanagersdk.UpdateSecretInput{
		SecretId:    aws.String("update-omit-secret"),
		Description: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeSecret(t.Context(), &secretsmanagersdk.DescribeSecretInput{
		SecretId: aws.String("update-omit-secret"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
}

// TestUpdateSecret_PreservesOmittedType covers UpdateSecretInput.Type the same
// way, at the backend level: the real SDK's DescribeSecretOutput has no Type
// member to read it back through (only OwningService, a different concept),
// so this asserts against the backend's own DescribeSecretOutput instead of a
// typed client.
func TestUpdateSecret_PreservesOmittedType(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name: "update-omit-type", SecretString: "v1",
	})
	require.NoError(t, err)

	firstType := "FirstPartner"
	_, err = b.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretID: "update-omit-type",
		Type:     &firstType,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "update-omit-type"})
	require.NoError(t, err)
	assert.Equal(t, "FirstPartner", desc.Type)

	// Omitted update must preserve the prior type.
	_, err = b.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{SecretID: "update-omit-type"})
	require.NoError(t, err)

	preserved, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "update-omit-type"})
	require.NoError(t, err)
	assert.Equal(t, "FirstPartner", preserved.Type, "omitted type must survive")

	// Explicit empty string clears type.
	emptyType := ""
	_, err = b.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretID: "update-omit-type",
		Type:     &emptyType,
	})
	require.NoError(t, err)

	cleared, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "update-omit-type"})
	require.NoError(t, err)
	assert.Empty(t, cleared.Type)
}
