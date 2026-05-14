package secretsmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func TestDescribeSecret_OwnerAccountAndPrimaryRegion(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackendWithConfig("000000000001", "eu-west-2")

	_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "owned", SecretString: "v"})
	require.NoError(t, err)

	out, err := b.DescribeSecret(&sm.DescribeSecretInput{SecretID: "owned"})
	require.NoError(t, err)
	assert.Equal(t, "000000000001", out.OwnerAccountID)
	assert.Equal(t, "eu-west-2", out.PrimaryRegion)
}

func TestListSecretVersionIDs_IncludesLastAccessedDate(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackendWithConfig("000000000001", "us-east-1")

	_, err := b.CreateSecret(&sm.CreateSecretInput{Name: "with-access", SecretString: "value"})
	require.NoError(t, err)

	_, err = b.GetSecretValue(&sm.GetSecretValueInput{SecretID: "with-access"})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(&sm.ListSecretVersionIDsInput{SecretID: "with-access"})
	require.NoError(t, err)
	require.Len(t, out.Versions, 1)
	require.NotNil(t, out.Versions[0].LastAccessedDate)
}
