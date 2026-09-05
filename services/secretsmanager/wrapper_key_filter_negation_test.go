package secretsmanager_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// aws-sdk-go-v2/service/secretsmanager@v1.44.4 types/types.go's Filter.Values doc
// comment: "You can prefix your search value with an exclamation mark ( ! ) in order
// to perform negation filters." ListSecrets and BatchGetSecretValue both take
// []types.Filter, so this applies to both operations.
func TestListSecrets_FilterNegationExcludes(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"prod/db", "prod/api", "dev/db"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "name", Values: []string{"!prod/db"}}},
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.SecretList))
	for _, s := range out.SecretList {
		names = append(names, s.Name)
	}
	assert.ElementsMatch(
		t, []string{"prod/api", "dev/db"}, names,
		"a negated value must exclude its match, not silently match nothing",
	)
}

// BatchGetSecretValueInput.Filters is also []types.Filter (api_op_BatchGetSecretValue.go),
// the identical 7-key vocabulary ListSecrets accepts (name/description/tag-key/tag-value/
// primary-region/owning-service/all) -- confirmed via aws-sdk-go-v2/service/secretsmanager
// @v1.44.4/types/types.go's shared Filter type used by both Input structs.
func TestBatchGetSecretValue_FilterAllKeyIsHonoured(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name: "match-me", SecretString: "v",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name: "other", SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		Filters: []secretsmanager.BatchGetSecretValueFilter{{Key: "all", Values: []string{"match"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretValues, 1, "the 'all' filter key must be honoured, not silently ignored")
	assert.Equal(t, "match-me", out.SecretValues[0].Name)
}
