package azurekeyvault_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSecretsClient(t *testing.T, s *testServer, vault string) *azsecrets.Client {
	t.Helper()

	c, err := azsecrets.NewClient(s.vaultURL(vault), fakeCredential{}, &azsecrets.ClientOptions{
		ClientOptions:                        s.clientOptions(),
		DisableChallengeResourceVerification: true,
	})
	require.NoError(t, err)

	return c
}

func TestSecrets_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)

	tests := []struct {
		run  func(t *testing.T, c *azsecrets.Client)
		name string
	}{
		{name: "versions", run: func(t *testing.T, c *azsecrets.Client) {
			t.Helper()
			ctx := t.Context()
			first, err := c.SetSecret(ctx, "db", azsecrets.SetSecretParameters{
				Value: new("one"), ContentType: new("text/plain"), Tags: map[string]*string{"env": new("dev")},
			}, nil)
			require.NoError(t, err)
			second, err := c.SetSecret(ctx, "db", azsecrets.SetSecretParameters{Value: new("two")}, nil)
			require.NoError(t, err)
			require.NotEqual(t, first.ID.Version(), second.ID.Version())
			assert.Len(t, first.ID.Version(), 32)

			latest, err := c.GetSecret(ctx, "db", "", nil)
			require.NoError(t, err)
			assert.Equal(t, "two", *latest.Value)

			old, err := c.GetSecret(ctx, "db", first.ID.Version(), nil)
			require.NoError(t, err)
			assert.Equal(t, "one", *old.Value)
			assert.Equal(t, "text/plain", *old.ContentType)
			assert.Equal(t, "dev", *old.Tags["env"])
			assert.True(t, *old.Attributes.Enabled)
			assert.Equal(t, "Recoverable+Purgeable", *old.Attributes.RecoveryLevel)

			pager := c.NewListSecretPropertiesVersionsPager("db", nil)
			count := 0

			for pager.More() {
				pg, pErr := pager.NextPage(ctx)
				require.NoError(t, pErr)

				count += len(pg.Value)
			}

			assert.Equal(t, 2, count)
		}},
		{name: "update", run: func(t *testing.T, c *azsecrets.Client) {
			t.Helper()
			ctx := t.Context()
			set, err := c.SetSecret(ctx, "tok", azsecrets.SetSecretParameters{Value: new("v")}, nil)
			require.NoError(t, err)

			upd, err := c.UpdateSecretProperties(
				ctx,
				"tok",
				set.ID.Version(),
				azsecrets.UpdateSecretPropertiesParameters{
					SecretAttributes: &azsecrets.SecretAttributes{Enabled: new(false)},
					Tags:             map[string]*string{"a": new("b")},
				},
				nil,
			)
			require.NoError(t, err)
			assert.False(t, *upd.Attributes.Enabled)

			_, err = c.GetSecret(ctx, "tok", "", nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "403")
		}},
		{name: "list_paged", run: func(t *testing.T, c *azsecrets.Client) {
			t.Helper()
			ctx := t.Context()

			for i := range 30 {
				_, err := c.SetSecret(ctx, fmt.Sprintf("s%02d", i), azsecrets.SetSecretParameters{Value: new("v")}, nil)
				require.NoError(t, err)
			}

			pager := c.NewListSecretPropertiesPager(nil)
			pages, names := 0, []string{}

			for pager.More() {
				pg, err := pager.NextPage(ctx)
				require.NoError(t, err)

				pages++

				for _, it := range pg.Value {
					names = append(names, it.ID.Name())
				}
			}

			assert.Equal(t, 2, pages, "default page size is 25 and the SDK follows nextLink")
			assert.Len(t, names, 30)
			assert.True(t, sort.StringsAreSorted(names))
		}},
		{name: "soft_delete", run: func(t *testing.T, c *azsecrets.Client) {
			t.Helper()
			ctx := t.Context()
			_, err := c.SetSecret(ctx, "gone", azsecrets.SetSecretParameters{Value: new("x")}, nil)
			require.NoError(t, err)

			del, err := c.DeleteSecret(ctx, "gone", nil)
			require.NoError(t, err)
			assert.NotNil(t, del.DeletedDate)
			assert.NotNil(t, del.ScheduledPurgeDate)
			assert.Contains(t, *del.RecoveryID, "/deletedsecrets/gone")

			_, err = c.GetSecret(ctx, "gone", "", nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "SecretNotFound")

			_, err = c.SetSecret(ctx, "gone", azsecrets.SetSecretParameters{Value: new("y")}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "Conflict")

			got, err := c.GetDeletedSecret(ctx, "gone", nil)
			require.NoError(t, err)
			assert.Equal(t, "x", *got.Value)

			pager := c.NewListDeletedSecretPropertiesPager(nil)
			pg, err := pager.NextPage(ctx)
			require.NoError(t, err)
			require.Len(t, pg.Value, 1)

			_, err = c.RecoverDeletedSecret(ctx, "gone", nil)
			require.NoError(t, err)

			back, err := c.GetSecret(ctx, "gone", "", nil)
			require.NoError(t, err)
			assert.Equal(t, "x", *back.Value)

			_, err = c.DeleteSecret(ctx, "gone", nil)
			require.NoError(t, err)
			_, err = c.PurgeDeletedSecret(ctx, "gone", nil)
			require.NoError(t, err)

			_, err = c.GetDeletedSecret(ctx, "gone", nil)
			require.Error(t, err)
		}},
		{name: "backup_restore", run: func(t *testing.T, c *azsecrets.Client) {
			t.Helper()
			ctx := t.Context()
			set, err := c.SetSecret(ctx, "bk", azsecrets.SetSecretParameters{Value: new("keep")}, nil)
			require.NoError(t, err)

			bk, err := c.BackupSecret(ctx, "bk", nil)
			require.NoError(t, err)

			_, err = c.RestoreSecret(ctx, azsecrets.RestoreSecretParameters{SecretBackup: bk.Value}, nil)
			require.Error(t, err, "restore over an existing secret conflicts")

			_, err = c.DeleteSecret(ctx, "bk", nil)
			require.NoError(t, err)
			_, err = c.PurgeDeletedSecret(ctx, "bk", nil)
			require.NoError(t, err)

			_, err = c.RestoreSecret(ctx, azsecrets.RestoreSecretParameters{SecretBackup: bk.Value}, nil)
			require.NoError(t, err)

			got, err := c.GetSecret(ctx, "bk", set.ID.Version(), nil)
			require.NoError(t, err)
			assert.Equal(t, "keep", *got.Value)
		}},
		{name: "missing", run: func(t *testing.T, c *azsecrets.Client) {
			t.Helper()
			_, err := c.GetSecret(t.Context(), "nope", "", nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "SecretNotFound")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newSecretsClient(t, s, tt.name))
		})
	}
}
