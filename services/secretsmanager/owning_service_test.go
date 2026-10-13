package secretsmanager_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func filterOf(key, value string) secretsmanager.SecretFilter {
	return secretsmanager.SecretFilter{Key: key, Values: []string{value}}
}

func TestManagedSecretOwningServiceAndRegionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter secretsmanager.SecretFilter
		want   []string
	}{
		{name: "owning service", filter: filterOf("owning-service", "rds"), want: []string{"rds!cluster-1"}},
		{name: "other owner", filter: filterOf("owning-service", "docdb")},
		{
			name:   "primary region hit",
			filter: filterOf("primary-region", "us-east-1"),
			want:   []string{"plain", "rds!cluster-1"},
		},
		{name: "primary region miss", filter: filterOf("primary-region", "eu-west-1")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			t.Cleanup(b.StopRotationScheduler)

			_, err := b.CreateSecret(
				context.Background(), &secretsmanager.CreateSecretInput{Name: "plain", SecretString: "v"},
			)
			require.NoError(t, err)
			_, err = b.CreateManagedSecret("us-east-1", "rds!cluster-1", "", "v")
			require.NoError(t, err)

			out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
				Filters: []secretsmanager.SecretFilter{tt.filter},
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.SecretList))
			for _, e := range out.SecretList {
				got = append(got, e.Name)
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}

	b := secretsmanager.NewInMemoryBackend()
	t.Cleanup(b.StopRotationScheduler)

	_, err := b.CreateManagedSecret("us-east-1", "rds!cluster-1", "", "v")
	require.NoError(t, err)

	d, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "rds!cluster-1"})
	require.NoError(t, err)
	assert.Equal(t, "rds", d.OwningService)
}
