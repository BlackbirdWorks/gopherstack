package secretsmanager_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// Test_ListSecrets_SortBy verifies the ListSecrets SortBy field (created-date,
// last-changed-date, last-accessed-date, and the default name ordering), combined
// with SortOrder asc/desc. Matches the real AWS SortByType enum
// (aws-sdk-go-v2/service/secretsmanager/types.SortByType) which this mock previously
// ignored entirely, always ordering by name regardless of the client's request.
func Test_ListSecrets_SortBy(t *testing.T) {
	t.Parallel()

	t.Run("created-date", func(t *testing.T) {
		t.Parallel()

		b := sm.NewInMemoryBackend()

		// Create secrets out of name order but in a known CreatedDate order:
		// zebra (oldest) -> mango -> apple (newest).
		times := []time.Time{
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		}
		names := []string{"zebra", "mango", "apple"}

		for i, name := range names {
			ts := times[i]
			b.SetNowForTest(func() time.Time { return ts })
			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
			require.NoError(t, err)
		}
		b.SetNowForTest(time.Now)

		out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{SortBy: "created-date"})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 3)
		gotNames := []string{out.SecretList[0].Name, out.SecretList[1].Name, out.SecretList[2].Name}
		assert.Equal(t, []string{"zebra", "mango", "apple"}, gotNames, "ascending created-date order")

		outDesc, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
			SortBy: "created-date", SortOrder: "desc",
		})
		require.NoError(t, err)
		require.Len(t, outDesc.SecretList, 3)
		gotNamesDesc := []string{outDesc.SecretList[0].Name, outDesc.SecretList[1].Name, outDesc.SecretList[2].Name}
		assert.Equal(t, []string{"apple", "mango", "zebra"}, gotNamesDesc, "descending created-date order")
	})

	t.Run("last-changed-date", func(t *testing.T) {
		t.Parallel()

		b := sm.NewInMemoryBackend()

		for _, name := range []string{"c-secret", "a-secret", "b-secret"} {
			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v0"})
			require.NoError(t, err)
		}

		// Change values out of name order but in a known LastChangedDate order:
		// b-secret (oldest change) -> c-secret -> a-secret (most recent change).
		changeOrder := []struct {
			ts   time.Time
			name string
		}{
			{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "b-secret"},
			{time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), "c-secret"},
			{time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC), "a-secret"},
		}
		for _, step := range changeOrder {
			ts := step.ts
			b.SetNowForTest(func() time.Time { return ts })
			_, err := b.UpdateSecret(context.Background(), &sm.UpdateSecretInput{
				SecretID: step.name, SecretString: "v1",
			})
			require.NoError(t, err)
		}
		b.SetNowForTest(time.Now)

		out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{SortBy: "last-changed-date"})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 3)
		gotNames := []string{out.SecretList[0].Name, out.SecretList[1].Name, out.SecretList[2].Name}
		assert.Equal(t, []string{"b-secret", "c-secret", "a-secret"}, gotNames, "ascending last-changed-date order")
	})

	t.Run("last-accessed-date_nil_sorts_first", func(t *testing.T) {
		t.Parallel()

		b := sm.NewInMemoryBackend()

		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "never-read", SecretString: "v"})
		require.NoError(t, err)
		_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "was-read", SecretString: "v"})
		require.NoError(t, err)

		_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "was-read"})
		require.NoError(t, err)

		out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{SortBy: "last-accessed-date"})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 2)
		// A secret that has never been read has no LastAccessedDate, which sorts as the
		// earliest possible value — it must come first in ascending order.
		assert.Equal(t, "never-read", out.SecretList[0].Name)
		assert.Equal(t, "was-read", out.SecretList[1].Name)
	})

	t.Run("default_is_name", func(t *testing.T) {
		t.Parallel()

		b := sm.NewInMemoryBackend()

		for _, name := range []string{"zebra", "apple", "mango"} {
			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
			require.NoError(t, err)
		}

		out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 3)
		gotNames := []string{out.SecretList[0].Name, out.SecretList[1].Name, out.SecretList[2].Name}
		assert.Equal(t, []string{"apple", "mango", "zebra"}, gotNames, "default SortBy is name ascending")
	})
}

// Test_ListSecrets_NextRotationDate verifies that ListSecrets' SecretListEntry carries
// NextRotationDate (matching aws-sdk-go-v2/service/secretsmanager/types.SecretListEntry),
// which this mock previously omitted — only DescribeSecret computed it.
func Test_ListSecrets_NextRotationDate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		configureFn  func(t *testing.T, b *sm.InMemoryBackend, secretName string)
		name         string
		wantHasValue bool
	}{
		{
			name: "rotation_not_configured_omits_field",
			configureFn: func(_ *testing.T, _ *sm.InMemoryBackend, _ string) {
				// no rotation configured
			},
			wantHasValue: false,
		},
		{
			name: "rotation_configured_populates_field",
			configureFn: func(t *testing.T, b *sm.InMemoryBackend, secretName string) {
				t.Helper()

				_, err := b.RotateSecret(context.Background(), &sm.RotateSecretInput{
					SecretID: secretName,
					RotationRules: &sm.RotationRulesType{
						AutomaticallyAfterDays: ptr64(30),
					},
				})
				require.NoError(t, err)
			},
			wantHasValue: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sm.NewInMemoryBackend()
			_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
				Name: "rot-" + tc.name, SecretString: "v",
			})
			require.NoError(t, err)

			tc.configureFn(t, b, "rot-"+tc.name)

			out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
				Filters: []sm.SecretFilter{{Key: "name", Values: []string{"rot-" + tc.name}}},
			})
			require.NoError(t, err)
			require.Len(t, out.SecretList, 1)

			if tc.wantHasValue {
				assert.NotNil(t, out.SecretList[0].NextRotationDate)
			} else {
				assert.Nil(t, out.SecretList[0].NextRotationDate)
			}
		})
	}
}
