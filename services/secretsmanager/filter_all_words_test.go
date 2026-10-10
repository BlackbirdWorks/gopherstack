package secretsmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func TestListSecrets_FilterAllWords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
		want   []string
	}{
		{
			name:   "camel_case_split",
			values: []string{"credsDatabase#892"},
			want:   []string{"creds-db", "db-892", "tagged"},
		},
		{name: "any_word_matches", values: []string{"My_Secret"}, want: []string{"my-secret-thing"}},
		{name: "case_insensitive", values: []string{"DATABASE"}, want: []string{"creds-db", "tagged"}},
		{name: "word_prefix", values: []string{"datab"}, want: []string{"creds-db", "tagged"}},
		{name: "mid_word_no_match", values: []string{"atabase"}, want: nil},
		{name: "description_word", values: []string{"payroll"}, want: []string{"creds-db"}},
		{name: "tag_value_word", values: []string{"Finance"}, want: []string{"tagged"}},
		{name: "negation", values: []string{"database", "!payroll"}, want: []string{"tagged"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			t.Cleanup(b.StopRotationScheduler)

			for _, in := range []secretsmanager.CreateSecretInput{
				{Name: "creds-db", SecretString: "v", Description: "Payroll database login"},
				{Name: "db-892", SecretString: "v"},
				{Name: "my-secret-thing", SecretString: "v"},
				{Name: "tagged", SecretString: "v", Tags: []secretsmanager.Tag{{Key: "Team", Value: "Finance Database"}}},
				{Name: "unrelated", SecretString: "v"},
			} {
				_, err := b.CreateSecret(t.Context(), &in)
				require.NoError(t, err)
			}

			out, err := b.ListSecrets(t.Context(), &secretsmanager.ListSecretsInput{
				Filters: []secretsmanager.SecretFilter{{Key: "all", Values: tt.values}},
			})
			require.NoError(t, err)

			var got []string
			for _, s := range out.SecretList {
				got = append(got, s.Name)
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
